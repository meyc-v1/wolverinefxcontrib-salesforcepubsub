namespace Wolverine.SalesforcePubSub.Events;

public abstract class PlatformEvent : PubSubEvent
{
    public string CreatedById { get; set; } = null!;
    public long CreatedDate { get; set; }
}
