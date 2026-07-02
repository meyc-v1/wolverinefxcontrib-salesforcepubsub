using Microsoft.Extensions.Caching.Memory;
using SalesforceGrpc;
using Wolverine.SalesforcePubSub.Events;
using Wolverine.SalesforcePubSub.Internals;
using Wolverine.Util;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// Per-event type resolution (DECISIONS #16): the unconditional single-type endpoint never touches the
/// schema; mapped record names resolve to their type's Wolverine message name; unmapped events resolve to
/// the raw record name (missing-handler path); record names are memoized per schema id.
/// </summary>
public class EventTypeResolverTests
{
    public sealed class EventOne : PlatformEvent;
    public sealed class EventTwo : PlatformEvent;

    private static string SchemaJsonFor(string recordName)
        => $$"""{"name":"{{recordName}}","namespace":"com.sforce.eventbus","type":"record","fields":[]}""";

    private sealed class StubSchemaRepository : ISchemaRepository
    {
        public Task<SchemaInfo> GetDeserializationInfoByTopicNameAsync(string topicName, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<SchemaInfo> GetDeserializationInfoBySchemaIdAsync(string schemaId, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();
    }

    private static (EventTypeResolver Resolver, CachingSchemaRepository Schemas, MemoryCache Cache) Create(
        Type? unconditional = null, Dictionary<string, Type>? map = null)
    {
        var cache = new MemoryCache(new MemoryCacheOptions());
        var schemas = new CachingSchemaRepository(new StubSchemaRepository(), cache);
        return (new EventTypeResolver(unconditional, map ?? [], schemas), schemas, cache);
    }

    private static void SeedSchema(MemoryCache cache, string schemaId, string recordName)
        => cache.Set(schemaId, new SchemaInfo { SchemaId = schemaId, SchemaJson = SchemaJsonFor(recordName) });

    [Fact]
    public void Unconditional_type_short_circuits_without_reading_the_schema()
    {
        var (resolver, _, _) = Create(unconditional: typeof(EventOne));

        // "missing" schema id — the unconditional path must not need it.
        var (typeName, mapped, _) = resolver.Resolve("never-cached");

        Assert.True(mapped);
        Assert.Equal(typeof(EventOne).ToMessageTypeName(), typeName);
        Assert.True(resolver.IsUnconditional);
    }

    [Fact]
    public void Mapped_record_name_resolves_to_the_registered_type()
    {
        var (resolver, _, cache) = Create(map: new()
        {
            ["CM_Test_Event_One__e"] = typeof(EventOne),
            ["CM_Test_Event_Two__e"] = typeof(EventTwo)
        });
        SeedSchema(cache, "s2", "CM_Test_Event_Two__e");

        var (typeName, mapped, recordName) = resolver.Resolve("s2");

        Assert.True(mapped);
        Assert.Equal(typeof(EventTwo).ToMessageTypeName(), typeName);
        Assert.Equal("CM_Test_Event_Two__e", recordName);
    }

    [Fact]
    public void Unmapped_record_name_resolves_to_the_raw_name()
    {
        var (resolver, _, cache) = Create(map: new() { ["CM_Test_Event_One__e"] = typeof(EventOne) });
        SeedSchema(cache, "s9", "Somebody_Elses_Event__e");

        var (typeName, mapped, recordName) = resolver.Resolve("s9");

        Assert.False(mapped);
        Assert.Equal("Somebody_Elses_Event__e", typeName); // Wolverine's missing-handler path takes it from here
        Assert.Equal("Somebody_Elses_Event__e", recordName);
    }

    [Fact]
    public void Record_names_are_memoized_per_schema_id()
    {
        var (resolver, _, cache) = Create(map: new() { ["CM_Test_Event_One__e"] = typeof(EventOne) });
        SeedSchema(cache, "s1", "CM_Test_Event_One__e");

        var first = resolver.Resolve("s1");
        cache.Remove("s1"); // evict the schema — the memoized record name must carry it

        var second = resolver.Resolve("s1");
        Assert.Equal(first, second);
    }

    [Fact]
    public void Throws_when_the_schema_was_never_cached()
    {
        var (resolver, _, _) = Create(map: new() { ["CM_Test_Event_One__e"] = typeof(EventOne) });

        var ex = Assert.Throws<InvalidOperationException>(() => resolver.Resolve("missing"));
        Assert.Contains("not cached", ex.Message);
    }
}
