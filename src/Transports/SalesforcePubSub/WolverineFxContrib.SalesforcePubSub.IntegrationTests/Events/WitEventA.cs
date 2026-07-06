// ReSharper disable InconsistentNaming

using Wolverine.SalesforcePubSub.Events;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;

/// <summary>Maps to the WIT_Event_A__e integration-test platform event (see docs/org-setup/README.md).</summary>
[SalesforcePlatformEvent("WIT_Event_A__e")]
public sealed class WitEventA : PlatformEvent
{
    public string? Message__c { get; set; }
}
