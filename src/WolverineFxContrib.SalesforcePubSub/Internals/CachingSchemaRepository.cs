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
}
