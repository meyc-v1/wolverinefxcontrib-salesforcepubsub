using Microsoft.Extensions.Caching.Memory;
using SalesforceGrpc;

namespace Wolverine.SalesforcePubSub.Internals;

internal sealed class CachingSchemaRepository
{
    private readonly ISchemaRepository _repository;
    private readonly IMemoryCache _memoryCache;

    public CachingSchemaRepository(ISchemaRepository repository, IMemoryCache memoryCache)
    {
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _memoryCache = memoryCache ?? throw new ArgumentNullException(nameof(memoryCache));
    }

    public async Task<SchemaInfo> GetDeserializationInfoBySchemaIdAsync(string schemaId, CancellationToken cancellationToken)
    {
        // GetOrCreateAsync handles concurrent callers for the same key internally. In the worst case
        // two concurrent callers for a new schema id both hit the underlying repository — one wins the
        // cache write, the other's result is discarded.
        var returnable = await _memoryCache.GetOrCreateAsync(schemaId, async entry =>
        {
            entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(24);
            return await _repository.GetDeserializationInfoBySchemaIdAsync(schemaId, cancellationToken);
        }) ?? throw new InvalidOperationException($"Unable to fetch schema with id: {schemaId}");

        return returnable;
    }

    /// <summary>
    /// Synchronously returns a schema only if it is already cached. Used by the serializer on the dispatch
    /// path; the listener ensures the schema is fetched/cached (async) before handing the envelope off.
    /// </summary>
    public bool TryGetCachedSchema(string schemaId, out SchemaInfo schema)
    {
        if (_memoryCache.TryGetValue(schemaId, out SchemaInfo? cached) && cached is not null)
        {
            schema = cached;
            return true;
        }

        schema = null!;
        return false;
    }
}
