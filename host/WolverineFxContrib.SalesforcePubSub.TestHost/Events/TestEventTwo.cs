// ReSharper disable InconsistentNaming

using Wolverine.SalesforcePubSub.Events;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Events;

/// <summary>
/// Maps to the "CM Test Event Two" platform event. Add custom (__c) fields here with names matching the
/// platform event schema.
/// </summary>
[SalesforcePlatformEvent("CM_Test_Event_Two__e")]
public sealed class TestEventTwo : PlatformEvent
{
    public string Message__c { get; set; } = null!;
}
