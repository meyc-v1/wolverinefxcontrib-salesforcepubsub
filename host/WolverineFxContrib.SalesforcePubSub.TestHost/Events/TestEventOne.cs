using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// Maps to the "Test Event One" platform event (/event/Test_Event_One__e). Inherits the standard
/// CreatedById/CreatedDate. Add custom (__c) fields here with names matching the platform event schema.
/// </summary>
public sealed class TestEventOne : PlatformEvent
{
    // e.g. public string? Message__c { get; set; }
}
