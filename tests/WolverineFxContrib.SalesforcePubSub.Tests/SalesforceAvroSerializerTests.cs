using Microsoft.Extensions.Caching.Memory;
using SalesforceGrpc;
using Wolverine;
using Wolverine.SalesforcePubSub.Internals;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// The sync Avro serializer's guard paths and listen-only contract. The happy-path decode needs a real
/// Avro schema + payload and is covered by integration tests.
/// </summary>
public class SalesforceAvroSerializerTests
{
    private sealed class StubSchemaRepository : ISchemaRepository
    {
        public Task<SchemaInfo> GetDeserializationInfoByTopicNameAsync(string topicName, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<SchemaInfo> GetDeserializationInfoBySchemaIdAsync(string schemaId, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();
    }

    private static SalesforceAvroSerializer Create()
        => new(new CachingSchemaRepository(new StubSchemaRepository(), new MemoryCache(new MemoryCacheOptions())));

    [Fact]
    public void ContentType_is_the_salesforce_avro_type()
        => Assert.Equal("application/x-salesforce-avro", Create().ContentType);

    [Fact]
    public void ReadFromData_throws_when_the_schema_id_header_is_missing()
    {
        var envelope = new Envelope { Data = [1, 2, 3] };
        var ex = Assert.Throws<InvalidOperationException>(() => Create().ReadFromData(typeof(object), envelope));
        Assert.Contains(SalesforceAvroSerializer.SchemaIdHeader, ex.Message);
    }

    [Fact]
    public void ReadFromData_throws_when_the_schema_is_not_cached()
    {
        var envelope = new Envelope { Data = [1, 2, 3] };
        envelope.Headers[SalesforceAvroSerializer.SchemaIdHeader] = "schema-123";

        var ex = Assert.Throws<InvalidOperationException>(() => Create().ReadFromData(typeof(object), envelope));
        Assert.Contains("not cached", ex.Message);
    }

    [Fact]
    public void Outbound_members_are_unsupported_listen_only()
    {
        var serializer = Create();
        Assert.Throws<NotSupportedException>(() => serializer.Write(new Envelope()));
        Assert.Throws<NotSupportedException>(() => serializer.WriteMessage(new object()));
        Assert.Throws<NotSupportedException>(() => serializer.ReadFromData([1, 2, 3]));
    }
}
