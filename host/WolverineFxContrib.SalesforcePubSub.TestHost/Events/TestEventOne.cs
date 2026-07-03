// ReSharper disable InconsistentNaming

using Wolverine.SalesforcePubSub.Events;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Events;

/// <summary>
/// Maps to the "CM Test Event One" platform event. Inherits the standard CreatedById/CreatedDate. Add
/// custom (__c) fields here with names matching the platform event schema.
/// </summary>
[SalesforcePlatformEvent("CM_Test_Event_One__e")]
public sealed class TestEventOne : PlatformEvent
{
    public string Message__c { get; set; } = null!;
}
