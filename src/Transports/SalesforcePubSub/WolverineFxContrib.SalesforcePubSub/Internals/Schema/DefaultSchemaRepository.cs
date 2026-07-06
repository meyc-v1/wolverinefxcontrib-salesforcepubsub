using SalesforceGrpc;

namespace Wolverine.SalesforcePubSub.Internals.Schema;

internal sealed class DefaultSchemaRepository : ISchemaRepository
{
    private readonly PubSub.PubSubClient _client;

    public DefaultSchemaRepository(PubSub.PubSubClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }

    public async Task<SchemaInfo> GetDeserializationInfoByTopicNameAsync(string topicName, CancellationToken cancellationToken = default)
    {
        var topicInfo = await GetTopicInfoByTopicNameAsync(topicName, cancellationToken).ConfigureAwait(false);
        return await GetDeserializationInfoBySchemaIdAsync(topicInfo.SchemaId, cancellationToken).ConfigureAwait(false);
    }

    private async Task<TopicInfo> GetTopicInfoByTopicNameAsync(string topicName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentException(nameof(topicName));

        return await _client.GetTopicAsync(new TopicRequest { TopicName = topicName }, cancellationToken: cancellationToken, deadline: GetDeadline()).ConfigureAwait(false);
    }

    public async Task<SchemaInfo> GetDeserializationInfoBySchemaIdAsync(string schemaId, CancellationToken cancellationToken = default)
    {
        return await GetSchemaInfoBySchemaIdAsync(schemaId, cancellationToken).ConfigureAwait(false);
    }

    private async Task<SchemaInfo> GetSchemaInfoBySchemaIdAsync(string schemaId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(schemaId))
            throw new ArgumentException(nameof(schemaId));

        return await _client.GetSchemaAsync(new SchemaRequest { SchemaId = schemaId }, cancellationToken: cancellationToken, deadline: GetDeadline()).ConfigureAwait(false);
    }

    private static DateTime GetDeadline() => DateTime.UtcNow.AddSeconds(60);
}
