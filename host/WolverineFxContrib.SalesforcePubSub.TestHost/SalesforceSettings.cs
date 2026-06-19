using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// General Salesforce + pub/sub configuration, bound from the "salesforceSettings" section.
/// (Auth — ClientId/ClientSecret/LoginUri — lives separately under "salesforceAuthenticationSettings".)
/// </summary>
public sealed class SalesforceSettings
{
    /// <summary>REST data API base, e.g. https://your-org.my.salesforce.com/services/data/v64.0/ (trailing slash required).</summary>
    public Uri BaseUri { get; set; } = null!;

    /// <summary>Salesforce Pub/Sub gRPC endpoint.</summary>
    public Uri PubSubUri { get; set; } = new("https://api.pubsub.salesforce.com:7443");

    /// <summary>The subscriptions to wire as Wolverine listening endpoints.</summary>
    public List<SalesforceSubscriptionOptions> Subscriptions { get; set; } = [];
}

/// <summary>One Salesforce subscription: its kind, the channel/MES name, and the .NET event type it maps to.</summary>
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
}
