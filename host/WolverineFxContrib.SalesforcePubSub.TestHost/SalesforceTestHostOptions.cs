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

    // Test Event One — consumed via a topic subscription (client-side replay)
    public string TestEventOneChannel { get; set; } = "/event/CM_Test_Event_One__e";
    public string TestEventOneSObject { get; set; } = "CM_Test_Event_One__e";

    // Test Event Two — consumed via a managed event subscription (server-side replay).
    // PLACEHOLDER: set this to the DeveloperName of the ManagedEventSubscription created in Salesforce
    // (the old runner consumed Test Event Two as a topic, so there was no MES name to carry over).
    public string TestEventTwoManagedSubscription { get; set; } = "CM_Test_Event_Two_MES";
    public string TestEventTwoSObject { get; set; } = "CM_Test_Event_Two__e";
}
