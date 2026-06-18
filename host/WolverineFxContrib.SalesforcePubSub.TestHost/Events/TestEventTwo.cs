using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// Maps to the "Test Event Two" platform event (/event/Test_Event_Two__e), consumed via MES.
/// Add custom (__c) fields here with names matching the platform event schema.
/// </summary>
public sealed class TestEventTwo : PlatformEvent
{
    // e.g. public string? Message__c { get; set; }
}
