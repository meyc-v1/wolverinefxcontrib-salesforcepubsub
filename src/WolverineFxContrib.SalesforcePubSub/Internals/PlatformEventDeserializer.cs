using SolTechnology.Avro;
using Wolverine.SalesforcePubSub.Events;

namespace Wolverine.SalesforcePubSub.Internals;

internal sealed class PlatformEventDeserializer : IEventDeserializer
{
    private readonly CachingSchemaRepository _schemaRepository;

    public PlatformEventDeserializer(CachingSchemaRepository schemaRepository)
    {
        _schemaRepository = schemaRepository ?? throw new ArgumentNullException(nameof(schemaRepository));
    }

    public async Task<PubSubEvent> DeserializeAsync(EventMessage eventMessage, Type targetType, CancellationToken token)
    {
        var schemaId = eventMessage.ConsumerEvent.Event.SchemaId;
        var schema = await _schemaRepository.GetDeserializationInfoBySchemaIdAsync(schemaId, token);

        return AvroConvert.DeserializeHeadless(eventMessage.ConsumerEvent.Event.Payload.ToByteArray(),
                   schema.SchemaJson, targetType) as PubSubEvent
               ?? throw new InvalidOperationException($"Deserialization failed for type {targetType}");
    }
}
