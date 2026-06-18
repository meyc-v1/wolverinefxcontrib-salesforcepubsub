namespace Wolverine.SalesforcePubSub.Internals;

internal interface IEventDeserializer
{
    Task<PubSubEvent> DeserializeAsync(EventMessage eventMessage, Type targetType, CancellationToken token);
}
