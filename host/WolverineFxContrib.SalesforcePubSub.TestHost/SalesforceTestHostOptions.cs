using Wolverine.SalesforcePubSub;

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

    /// <summary>The Salesforce subscriptions to wire as Wolverine listening endpoints.</summary>
    public List<SalesforceSubscriptionOptions> Subscriptions { get; set; } = [];
}

/// <summary>
/// One Salesforce subscription: its kind, the channel/MES name, the .NET event type it maps to,
/// and (optionally) the platform-event sObject used by the publisher.
/// </summary>
public sealed class SalesforceSubscriptionOptions
{
    /// <summary>Topic or ManagedSubscription.</summary>
    public SalesforceResourceKind Type { get; set; }

    /// <summary>
    /// For a topic, the channel path (e.g. <c>/event/CM_Test_Event_Two__e</c>).
    /// For MES, the ManagedEventSubscription DeveloperName (e.g. <c>CM_Test_Event_One</c>).
    /// </summary>
    public string Channel { get; set; } = "";

    /// <summary>Simple or full name of the <c>PubSubEvent</c>-derived type to deserialize events into.</summary>
    public string MessageType { get; set; } = "";

    /// <summary>
    /// Platform-event sObject API name for the publisher. Optional for topics (derived from
    /// <see cref="Channel"/>); set explicitly to publish an MES-backed event.
    /// </summary>
    public string? SObject { get; set; }

    /// <summary>The sObject to POST to when publishing, or null if it can't be determined.</summary>
    public string? PublishSObject =>
        SObject ?? (Type == SalesforceResourceKind.Topic && Channel.StartsWith("/event/", StringComparison.Ordinal)
            ? Channel["/event/".Length..]
            : null);
}
