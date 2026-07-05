using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using Wolverine;
using Wolverine.ErrorHandling;
using Wolverine.SalesforcePubSub;
using Wolverine.SqlServer;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// The Durable half of the README delivery matrix, against a real SQL Server message store
/// (Kafka lineage: DeadLetterQueueTests + duplicate_message_handling_with_postgres_inbox). The tests
/// use their own schemas — <c>witint</c> (IdOnly) and <c>witint_iad</c> (IdAndDestination) — because
/// switching MessageIdentity migrates the inbox primary key, and flip-flopping a shared schema every
/// run is churn the TestHost's dbo tables shouldn't inherit.
/// </summary>
public class DurableTests(SalesforceTestContext ctx)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(60);

    /// <summary>How long a fanned-out duplicate gets to (wrongly) arrive before we call it deduped.</summary>
    private static readonly TimeSpan DuplicateSettle = TimeSpan.FromSeconds(5);

    private string RequireDurabilityConnectionString()
    {
        if (ctx.DurabilityConnectionString is { } cs)
            return cs;

        Assert.Skip("No 'durabilitySettings:connectionString' user secret — Durable-mode facts need the Wolverine SQL Server message store.");
        return null!; // unreachable
    }

    [Fact]
    public async Task A_durable_poison_message_lands_in_the_dead_letter_table()
    {
        var connectionString = RequireDurabilityConnectionString();

        var sink = new EventSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Durability.Mode = DurabilityMode.Solo;
            opts.PersistMessagesWithSqlServer(connectionString, "witint");
            opts.Discovery.IncludeType<DurablePoisonHandler>();
            opts.Policies.OnException<DurablePoisonException>().MoveToErrorQueue();
            opts.ListenToSalesforceTopic("/event/WIT_Event_A__e").MapEvent<WitEventA>().UseDurableInbox();
        });

        try
        {
            var poison = $"dpoison-{Guid.NewGuid():N}";
            await ctx.PublishAsync("WIT_Event_A__e", poison, TestContext.Current.CancellationToken);

            // The handler saw it (and captured the envelope id) even though it never succeeds.
            var envelopeId = await WaitForAsync(
                () => DurablePoisonHandler.EnvelopeIdFor(poison), ReceiveTimeout,
                "the poison event was never attempted");

            // Unlike the Inline no-op store, Durable preserves the poison message: a real DLQ row.
            var found = await WaitForAsync(
                () => DeadLetterRowExistsAsync(connectionString, "witint", envelopeId).Result ? true : (bool?)null,
                ReceiveTimeout, "no dead-letter row appeared for the poison envelope");
            Assert.True(found);
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }

    [Fact]
    public async Task A_fanned_out_event_is_processed_once_under_the_default_identity()
    {
        var connectionString = RequireDurabilityConnectionString();

        var sink = new EventSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Durability.Mode = DurabilityMode.Solo;
            opts.PersistMessagesWithSqlServer(connectionString, "witint");
            opts.Discovery.IncludeType<WitEventAHandler>();

            // The same event arrives via its own topic AND the channel — one deterministic envelope id,
            // and the inbox's IdOnly primary key admits it once (DECISIONS #18).
            opts.ListenToSalesforceTopic("/event/WIT_Event_A__e").MapEvent<WitEventA>().UseDurableInbox();
            opts.ListenToSalesforceTopic("/event/WIT_Channel__chn").MapEvent<WitEventA>().UseDurableInbox();
        });

        try
        {
            var correlation = $"fanout-once-{Guid.NewGuid():N}";
            await ctx.PublishAsync("WIT_Event_A__e", correlation, TestContext.Current.CancellationToken);

            await sink.WaitForAsync(e => e.Message == correlation, count: 1, ReceiveTimeout);
            await Task.Delay(DuplicateSettle, TestContext.Current.CancellationToken);

            Assert.Single(sink.Snapshot(), e => e.Message == correlation);
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }

    [Fact]
    public async Task A_fanned_out_event_is_processed_per_endpoint_under_IdAndDestination()
    {
        var connectionString = RequireDurabilityConnectionString();

        var sink = new EventSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Durability.Mode = DurabilityMode.Solo;
            opts.Durability.MessageIdentity = MessageIdentity.IdAndDestination;
            opts.PersistMessagesWithSqlServer(connectionString, "witint_iad");
            opts.Discovery.IncludeType<WitEventAHandler>();

            opts.ListenToSalesforceTopic("/event/WIT_Event_A__e").MapEvent<WitEventA>().UseDurableInbox();
            opts.ListenToSalesforceTopic("/event/WIT_Channel__chn").MapEvent<WitEventA>().UseDurableInbox();
        });

        try
        {
            var correlation = $"fanout-each-{Guid.NewGuid():N}";
            await ctx.PublishAsync("WIT_Event_A__e", correlation, TestContext.Current.CancellationToken);

            // Wolverine's native knob for deliberate fan-out: id + destination, so each endpoint
            // processes its own copy.
            var received = await sink.WaitForAsync(e => e.Message == correlation, count: 2, ReceiveTimeout);

            Assert.Equal(2, received.Count);
            Assert.Equal(
                new[] { "/event/WIT_Channel__chn", "/event/WIT_Event_A__e" },
                received.Select(e => e.TopicName).OrderBy(t => t).ToArray());
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }

    private static async Task<T> WaitForAsync<T>(Func<T?> probe, TimeSpan timeout, string failure)
        where T : struct
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (probe() is { } value)
                return value;

            await Task.Delay(250);
        }

        throw new TimeoutException($"{failure} (within {timeout})");
    }

    private static async Task<bool> DeadLetterRowExistsAsync(string connectionString, string schema, Guid envelopeId)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = new SqlCommand($"SELECT COUNT(*) FROM [{schema}].wolverine_dead_letters WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("@id", envelopeId);

        return (int)(await cmd.ExecuteScalarAsync())! > 0;
    }
}

public class DurablePoisonException(string message) : Exception(message);

/// <summary>Always throws for dpoison-prefixed correlations, capturing the envelope id for the DLQ assertion.</summary>
public class DurablePoisonHandler
{
    private static readonly ConcurrentDictionary<string, Guid> EnvelopeIds = new();

    public static Guid? EnvelopeIdFor(string correlation)
        => EnvelopeIds.TryGetValue(correlation, out var id) ? id : null;

    public static void Handle(WitEventA evt, Envelope envelope, EventSink sink)
    {
        if (evt.Message__c is { } correlation && correlation.StartsWith("dpoison-"))
        {
            EnvelopeIds[correlation] = envelope.Id;
            throw new DurablePoisonException($"Intentional durable poison failure for {correlation}");
        }

        sink.Record(new ReceivedEvent(
            nameof(WitEventA), evt.Message__c, evt.ReplayId, envelope.Id, envelope.TopicName, envelope.SentAt));
    }
}
