// ReSharper disable InconsistentNaming

using Wolverine.SalesforcePubSub.Events;

namespace TestHost.Events;

/// <summary>
/// Maps to the "WIT Event B" platform event. Add custom (__c) fields here with names matching the
/// platform event schema.
/// </summary>
[SalesforcePlatformEvent("WIT_Event_B__e")]
public sealed class WitEventB : PlatformEvent
{
    public string Message__c { get; set; } = null!;
}
