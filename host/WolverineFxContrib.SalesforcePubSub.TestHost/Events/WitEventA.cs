// ReSharper disable InconsistentNaming

using Wolverine.SalesforcePubSub.Events;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Events;

/// <summary>
/// Maps to the "WIT Event A" platform event. Inherits the standard CreatedById/CreatedDate. Add
/// custom (__c) fields here with names matching the platform event schema.
/// </summary>
[SalesforcePlatformEvent("WIT_Event_A__e")]
public sealed class WitEventA : PlatformEvent
{
    public string Message__c { get; set; } = null!;
}
