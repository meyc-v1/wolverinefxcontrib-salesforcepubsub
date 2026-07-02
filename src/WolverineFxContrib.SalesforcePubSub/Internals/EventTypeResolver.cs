using System.Collections.Concurrent;
using Wolverine.Util;

namespace Wolverine.SalesforcePubSub.Internals;

/// <summary>
/// Resolves the Wolverine message type name to stamp on each event from its Avro schema: record name →
/// the endpoint's <c>MapEvent</c> registrations (DECISIONS #16). An unconditional single-type endpoint
/// short-circuits without touching the schema — exact parity with the pre-channel behavior. Record names
/// are memoized per schema id; the listener guarantees the schema is cached before resolving. An unmapped
/// event resolves to its raw record name (<c>Mapped = false</c>) so Wolverine's missing-handler machinery
/// deals with it — which also lets a consumer opt in via <c>[MessageIdentity("Api_Name__e")]</c>.
/// </summary>
internal sealed class EventTypeResolver
{
    private readonly string? _unconditionalTypeName;
    private readonly Dictionary<string, string> _typeNamesByRecordName;
    private readonly CachingSchemaRepository _schemas;
    private readonly ConcurrentDictionary<string, string> _recordNamesBySchemaId = new(StringComparer.Ordinal);

    public EventTypeResolver(Type? unconditionalType, IReadOnlyDictionary<string, Type> eventTypeMap, CachingSchemaRepository schemas)
    {
        _unconditionalTypeName = unconditionalType?.ToMessageTypeName();
        _typeNamesByRecordName = eventTypeMap.ToDictionary(
            pair => pair.Key, pair => pair.Value.ToMessageTypeName(), StringComparer.Ordinal);
        _schemas = schemas;
    }

    public bool IsUnconditional => _unconditionalTypeName is not null;

    public (string MessageTypeName, bool Mapped, string RecordName) Resolve(string schemaId)
    {
        if (_unconditionalTypeName is not null)
            return (_unconditionalTypeName, true, string.Empty);

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
