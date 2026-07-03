using SalesforceGrpc;

namespace Wolverine.SalesforcePubSub.Internals.Schema;

internal interface ISchemaRepository
{
    Task<SchemaInfo> GetDeserializationInfoByTopicNameAsync(string topicName, CancellationToken cancellationToken = default);
    Task<SchemaInfo> GetDeserializationInfoBySchemaIdAsync(string schemaId, CancellationToken cancellationToken = default);
}
