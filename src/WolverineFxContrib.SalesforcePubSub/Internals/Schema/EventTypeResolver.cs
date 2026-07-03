using System.Collections.Concurrent;
using Wolverine.Util;

namespace Wolverine.SalesforcePubSub.Internals.Schema;

/// <summary>
/// Resolves the Wolverine message type name to stamp on each event from its Avro schema: record name →
/// the endpoint's <c>MapEvent</c> registrations. Every event resolves this way — multi-type is the core
/// model, and a single-type subscription is just the one-entry case (DECISIONS #19). Record names are
/// memoized per schema id; the listener guarantees the schema is cached before resolving. An unmapped
/// event resolves to its raw record name (<c>Mapped = false</c>) so Wolverine's missing-handler machinery
/// deals with it — which also lets a consumer opt in via <c>[MessageIdentity("Api_Name__e")]</c>.
/// </summary>
internal sealed class EventTypeResolver
{
    private readonly Dictionary<string, string> _typeNamesByRecordName;
    private readonly CachingSchemaRepository _schemas;
    private readonly ConcurrentDictionary<string, string> _recordNamesBySchemaId = new(StringComparer.Ordinal);

    public EventTypeResolver(IReadOnlyDictionary<string, Type> eventTypeMap, CachingSchemaRepository schemas)
    {
        _typeNamesByRecordName = eventTypeMap.ToDictionary(
            pair => pair.Key, pair => pair.Value.ToMessageTypeName(), StringComparer.Ordinal);
        _schemas = schemas;
    }

    public (string MessageTypeName, bool Mapped, string RecordName) Resolve(string schemaId)
    {
        var recordName = _recordNamesBySchemaId.GetOrAdd(schemaId, ParseRecordNameFromCache);

        return _typeNamesByRecordName.TryGetValue(recordName, out var typeName)
            ? (typeName, true, recordName)
            : (recordName, false, recordName);
    }

    private string ParseRecordNameFromCache(string schemaId)
        => _schemas.TryGetCachedSchema(schemaId, out var schema)
            ? AvroRecordName.Parse(schema.SchemaJson)
            : throw new InvalidOperationException(
                $"Schema '{schemaId}' is not cached; the listener must ensure the schema is fetched before resolving the event type.");
}
