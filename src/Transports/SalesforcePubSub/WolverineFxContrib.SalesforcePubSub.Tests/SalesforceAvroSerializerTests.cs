using Grpc.Core;
using Microsoft.Extensions.Caching.Memory;
using SalesforceGrpc;
using SolTechnology.Avro;
using Wolverine;
using Wolverine.SalesforcePubSub;
using Wolverine.SalesforcePubSub.Events;
using Wolverine.SalesforcePubSub.Internals.Authentication;
using Wolverine.SalesforcePubSub.Internals.Schema;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// The Avro serializer's two read paths and listen-only contract. Sync stays cache-only (the listener
/// pre-fetches on the hot path); async adds the durable-recovery fetch-on-miss by the persisted schema-id
/// header, with the listener's auth contract — invalidate the token cache on an auth-rejected fetch and
/// retry exactly once with a fresh token (DECISIONS #17).
/// </summary>
public class SalesforceAvroSerializerTests
{
    public sealed class SampleEvent : PlatformEvent
    {
        public string? Message__c { get; set; }
    }

    private static readonly string SampleSchemaJson = AvroConvert.GenerateSchema(typeof(SampleEvent));

    private sealed class ScriptedSchemaRepository : ISchemaRepository
    {
        private int _calls;
        public int SchemaByIdCalls => _calls;

        /// <summary>Scripted per-call result (arg = 1-based call number); throw to simulate a failure.</summary>
        public Func<int, SchemaInfo> OnGetById { get; init; } = _ => throw new NotImplementedException();

        public Task<SchemaInfo> GetDeserializationInfoByTopicNameAsync(string topicName, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<SchemaInfo> GetDeserializationInfoBySchemaIdAsync(string schemaId, CancellationToken cancellationToken = default)
            => Task.FromResult(OnGetById(Interlocked.Increment(ref _calls)));
    }

    private sealed class CountingTokenHandler : IAuthenticationTokenHandler
    {
        private int _calls;
        public int Calls => _calls;

        public Task<AuthenticationTokenResponse> GetAuthenticationTokenAsync(CancellationToken cancellationToken = default)
        {
            var n = Interlocked.Increment(ref _calls);
            return Task.FromResult(new AuthenticationTokenResponse($"token-{n}", "https://instance", "tenant"));
        }
    }

    private sealed record Fixture(
        SalesforceAvroSerializer Serializer,
        CachingSchemaRepository Schemas,
        ScriptedSchemaRepository Repository,
        CachingAuthenticationTokenProvider Tokens,
        CountingTokenHandler TokenHandler);

    private static Fixture Create(Func<int, SchemaInfo>? onGetById = null)
    {
        var repository = new ScriptedSchemaRepository
        {
            OnGetById = onGetById ?? (_ => new SchemaInfo { SchemaId = "s1", SchemaJson = SampleSchemaJson })
        };
        var schemas = new CachingSchemaRepository(repository, new MemoryCache(new MemoryCacheOptions()));
        var handler = new CountingTokenHandler();
        var tokens = new CachingAuthenticationTokenProvider(handler, new SubscriberComponentsSettings());
        return new Fixture(new SalesforceAvroSerializer(schemas, tokens), schemas, repository, tokens, handler);
    }

    private static Envelope EnvelopeFor(string schemaId, long replayId = 42)
    {
        var payload = AvroConvert.SerializeHeadless(
            new SampleEvent { CreatedById = "005xx", CreatedDate = 1_700_000_000_000, Message__c = "hello" },
            SampleSchemaJson);

        var envelope = new Envelope { Data = payload, Offset = replayId };
        envelope.Headers[SalesforceAvroSerializer.SchemaIdHeader] = schemaId;
        return envelope;
    }

    [Fact]
    public void ContentType_is_the_salesforce_avro_type()
        => Assert.Equal("application/x-salesforce-avro", Create().Serializer.ContentType);

    [Fact]
    public void ReadFromData_throws_when_the_schema_id_header_is_missing()
    {
        var envelope = new Envelope { Data = [1, 2, 3] };
        var ex = Assert.Throws<InvalidOperationException>(() => Create().Serializer.ReadFromData(typeof(object), envelope));
        Assert.Contains(SalesforceAvroSerializer.SchemaIdHeader, ex.Message);
    }

    [Fact]
    public void ReadFromData_throws_when_the_schema_is_not_cached()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => Create().Serializer.ReadFromData(typeof(SampleEvent), EnvelopeFor("schema-123")));
        Assert.Contains("not cached", ex.Message);
    }

    [Fact]
    public async Task ReadFromData_decodes_from_the_cached_schema_and_stamps_replay_metadata()
    {
        var ct = TestContext.Current.CancellationToken;
        var fixture = Create();
        await fixture.Schemas.GetDeserializationInfoBySchemaIdAsync("s1", ct); // the listener's pre-fetch

        var envelope = EnvelopeFor("s1", replayId: 77);
        var message = Assert.IsType<SampleEvent>(fixture.Serializer.ReadFromData(typeof(SampleEvent), envelope));

        Assert.Equal("hello", message.Message__c);
        Assert.Equal(77, message.ReplayId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000), envelope.SentAt);
    }

    [Fact]
    public async Task ReadFromDataAsync_uses_the_cached_schema_without_touching_the_repository()
    {
        var ct = TestContext.Current.CancellationToken;
        var fixture = Create();
        await fixture.Schemas.GetDeserializationInfoBySchemaIdAsync("s1", ct);
        Assert.Equal(1, fixture.Repository.SchemaByIdCalls);

        var message = await fixture.Serializer.ReadFromDataAsync(typeof(SampleEvent), EnvelopeFor("s1"));

        Assert.IsType<SampleEvent>(message);
        Assert.Equal(1, fixture.Repository.SchemaByIdCalls); // no second fetch
    }

    [Fact]
    public async Task Recovered_envelope_stamps_the_replay_id_from_the_persisted_header()
    {
        // The durable inbox round-trips headers but NOT Envelope.Offset — a recovered envelope arrives
        // with Offset 0, so the replay id must come from the persisted header (found live: ReplayId 0
        // on the restart-recovery test before this fix).
        var fixture = Create();
        var envelope = EnvelopeFor("s1", replayId: 3503869);
        envelope.Offset = 0; // what recovery actually hands the serializer

        envelope.Headers[SalesforceAvroSerializer.ReplayIdHeader] = "3503869";
        var message = await fixture.Serializer.ReadFromDataAsync(typeof(SampleEvent), envelope);

        Assert.Equal(3503869, Assert.IsType<SampleEvent>(message).ReplayId);
    }

    [Fact]
    public async Task ReadFromDataAsync_fetches_on_a_cache_miss_and_decodes()
    {
        // The durable restart-recovery case: fresh process, empty cache, schema id persisted in the header.
        var fixture = Create();

        var message = await fixture.Serializer.ReadFromDataAsync(typeof(SampleEvent), EnvelopeFor("s1"));

        Assert.Equal("hello", Assert.IsType<SampleEvent>(message).Message__c);
        Assert.Equal(1, fixture.Repository.SchemaByIdCalls);
    }

    [Fact]
    public async Task Auth_failure_on_a_cache_miss_invalidates_the_token_and_retries_once()
    {
        var ct = TestContext.Current.CancellationToken;
        var fixture = Create(onGetById: call => call == 1
            ? throw new RpcException(new Status(StatusCode.Unauthenticated, "revoked"))
            : new SchemaInfo { SchemaId = "s1", SchemaJson = SampleSchemaJson });

        await fixture.Tokens.GetTokenAsync(ct); // a (stale) token is cached
        Assert.Equal(1, fixture.TokenHandler.Calls);

        var message = await fixture.Serializer.ReadFromDataAsync(typeof(SampleEvent), EnvelopeFor("s1"));

        Assert.IsType<SampleEvent>(message);
        Assert.Equal(2, fixture.Repository.SchemaByIdCalls); // exactly one retry

        // The cached token was invalidated: the next request re-fetches instead of reusing the stale one.
        await fixture.Tokens.GetTokenAsync(ct);
        Assert.Equal(2, fixture.TokenHandler.Calls);
    }

    [Fact]
    public async Task Auth_failure_that_persists_after_the_retry_propagates()
    {
        var fixture = Create(onGetById: _ => throw new RpcException(new Status(StatusCode.Unauthenticated, "still revoked")));

        await Assert.ThrowsAsync<RpcException>(
            async () => await fixture.Serializer.ReadFromDataAsync(typeof(SampleEvent), EnvelopeFor("s1")));

        Assert.Equal(2, fixture.Repository.SchemaByIdCalls); // one attempt + one retry, no loop
    }

    [Fact]
    public async Task Non_auth_failure_does_not_invalidate_or_retry()
    {
        var ct = TestContext.Current.CancellationToken;
        var fixture = Create(onGetById: _ => throw new RpcException(new Status(StatusCode.Internal, "boom")));
        await fixture.Tokens.GetTokenAsync(ct);

        await Assert.ThrowsAsync<RpcException>(
            async () => await fixture.Serializer.ReadFromDataAsync(typeof(SampleEvent), EnvelopeFor("s1")));

        Assert.Equal(1, fixture.Repository.SchemaByIdCalls); // no retry
        await fixture.Tokens.GetTokenAsync(ct);
        Assert.Equal(1, fixture.TokenHandler.Calls);          // token not invalidated
    }

    [Fact]
    public async Task Outbound_members_are_unsupported_listen_only()
    {
        var serializer = Create().Serializer;
        Assert.Throws<NotSupportedException>(() => serializer.Write(new Envelope()));
        Assert.Throws<NotSupportedException>(() => serializer.WriteMessage(new object()));
        Assert.Throws<NotSupportedException>(() => serializer.ReadFromData([1, 2, 3]));
        await Assert.ThrowsAsync<NotSupportedException>(async () => await serializer.WriteAsync(new Envelope()));
    }
}
