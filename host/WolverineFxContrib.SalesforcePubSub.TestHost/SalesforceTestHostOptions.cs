namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>Test-host configuration, bound from the "Salesforce" section of appsettings/user-secrets.</summary>
public sealed class SalesforceTestHostOptions
{
    // Auth (populate via user-secrets for a real run; tokens expire)
    public string InstanceUrl { get; set; } = "";
    public string AccessToken { get; set; } = "";
    public string TenantId { get; set; } = "";

    public string ApiVersion { get; set; } = "v60.0";
    public string PubSubUri { get; set; } = "https://api.pubsub.salesforce.com:7443";

    // Test Event One — consumed via a managed event subscription (server-side replay).
    // DeveloperName of the ManagedEventSubscription on the sandbox org ("CM_Test_Event_One").
    public string TestEventOneManagedSubscription { get; set; } = "CM_Test_Event_One";
    public string TestEventOneSObject { get; set; } = "CM_Test_Event_One__e";

    // Test Event Two — consumed via a topic subscription (client-side replay)
    public string TestEventTwoChannel { get; set; } = "/event/CM_Test_Event_Two__e";
    public string TestEventTwoSObject { get; set; } = "CM_Test_Event_Two__e";
}
