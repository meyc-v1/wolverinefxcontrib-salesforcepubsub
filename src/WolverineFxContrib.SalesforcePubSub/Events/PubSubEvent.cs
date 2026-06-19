namespace Wolverine.SalesforcePubSub.Events;

public abstract class PubSubEvent
{
    public long ReplayId { get; set; }
}
