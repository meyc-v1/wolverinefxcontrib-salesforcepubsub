using SalesforceGrpc;

namespace Wolverine.SalesforcePubSub.Internals;

internal sealed class EventMessage
{
    public string Resource { get; }
    public long EventReplayId { get; }
    public ConsumerEvent ConsumerEvent { get; }

    public EventMessage(string resource, long eventReplayId, ConsumerEvent consumerEvent)
    {
        if (string.IsNullOrWhiteSpace(resource))
            throw new ArgumentException(ErrorMessages.StringCannotBeNullOrWhitespace, nameof(resource));

        Resource = resource;
        EventReplayId = eventReplayId;
        ConsumerEvent = consumerEvent ?? throw new ArgumentNullException(nameof(consumerEvent));
    }
}
