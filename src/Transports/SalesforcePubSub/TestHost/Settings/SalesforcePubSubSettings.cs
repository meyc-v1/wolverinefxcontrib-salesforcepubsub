using FluentValidation;
using Microsoft.Extensions.Options;
using Wolverine.SalesforcePubSub;

namespace TestHost.Settings;

/// <summary>
/// Pub/sub subscription configuration, bound from the "salesforceSettings" section. (The REST
/// publisher's BaseUri binds from the same section into the Salesforce lib's settings;
/// auth — ClientId/ClientSecret/LoginUri — lives separately under "salesforceAuthenticationSettings".)
/// </summary>
public sealed class SalesforcePubSubSettings
{
    /// <summary>Salesforce Pub/Sub gRPC endpoint. Defaults to the public Salesforce endpoint (see <see cref="SalesforcePubSubSettingsConfigurer"/>).</summary>
    public Uri PubSubUri { get; set; } = null!;

    /// <summary>The subscriptions to wire as Wolverine listening endpoints.</summary>
    public List<SalesforceSubscriptionOptions> Subscriptions { get; set; } = [];
}

/// <summary>Applies defaults (PostConfigure) and validates <see cref="SalesforcePubSubSettings"/>.</summary>
internal sealed class SalesforcePubSubSettingsConfigurer
    : IPostConfigureOptions<SalesforcePubSubSettings>, IValidateOptions<SalesforcePubSubSettings>
{
    /// <summary>The public Salesforce Pub/Sub gRPC endpoint, used when none is configured.</summary>
    public static readonly Uri DefaultPubSubUri = new("https://api.pubsub.salesforce.com:7443");

    public void PostConfigure(string? name, SalesforcePubSubSettings options)
        => options.PubSubUri ??= DefaultPubSubUri;

    public ValidateOptionsResult Validate(string? name, SalesforcePubSubSettings options)
    {
        var res = new SalesforcePubSubSettingsValidator().Validate(options);

        return res.IsValid
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(res.Errors.Select(x => x.ErrorMessage).ToList());
    }
}

internal sealed class SalesforcePubSubSettingsValidator : AbstractValidator<SalesforcePubSubSettings>
{
    public SalesforcePubSubSettingsValidator()
    {
        RuleFor(x => x.PubSubUri).NotNull();

        RuleForEach(x => x.Subscriptions).ChildRules(sub =>
        {
            sub.RuleFor(x => x.Resource).NotEmpty();
            sub.RuleFor(x => x.Type).IsInEnum();
            sub.RuleFor(x => x.Events)
                .Must(events => events.Count > 0)
                .WithMessage("Every subscription requires an events list (one entry per event type it carries).");

            sub.RuleForEach(x => x.Events).ChildRules(evt =>
            {
                // EventApiName may be omitted when the .NET type carries [SalesforcePlatformEvent].
                evt.RuleFor(x => x.MessageType).NotEmpty();
            });
        });
    }
}

/// <summary>The kind of Salesforce subscription to wire (Channel = custom channel, multi-type).</summary>
public enum SalesforceSubscriptionType
{
    Topic,
    ManagedSubscription,
    Channel
}

/// <summary>One event-type mapping on a multi-type subscription (MapEvent entry).</summary>
public sealed class SalesforceSubscriptionEventOptions
{
    /// <summary>Simple or full name of the <c>PubSubEvent</c>-derived type to deserialize into.</summary>
    public string MessageType { get; set; } = "";

    /// <summary>
    /// The event API name (Avro record name), e.g. <c>CM_Test_Event_One__e</c>. Optional when the .NET
    /// type carries a <c>[SalesforcePlatformEvent]</c> attribute.
    /// </summary>
    public string? EventApiName { get; set; }
}

/// <summary>One Salesforce subscription: its kind, the channel/MES name, and the .NET event type(s) it maps to.</summary>
public sealed class SalesforceSubscriptionOptions
{
    /// <summary>Topic, ManagedSubscription, or Channel (custom channel, multi-type).</summary>
    public SalesforceSubscriptionType Type { get; set; }

    /// <summary>
    /// The subscribed resource — matches <c>SalesforceEndpoint.Resource</c>. For a topic, the path
    /// (<c>/event/CM_Test_Event_Two__e</c> or <c>/event/CM_Test_Channel__chn</c>); for MES, the
    /// ManagedEventSubscription DeveloperName (e.g. <c>CM_Test_Event_One</c>).
    /// </summary>
    public string Resource { get; set; } = "";

    /// <summary>The events this subscription carries: one entry per event type (DECISIONS #19 — always map-style).</summary>
    public List<SalesforceSubscriptionEventOptions> Events { get; set; } = [];

    /// <summary>Wire this subscription as a listener. Set false to run isolated topic-only / MES-only passes.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Endpoint delivery mode: Inline (default, at-least-once), BufferedInMemory (at-most-once), or
    /// Durable (inbox-backed; requires <c>durabilitySettings:connectionString</c>).
    /// </summary>
    public Wolverine.Configuration.EndpointMode? Mode { get; set; }

    /// <summary>Override the idle/fetch timeout that triggers a reconnect (default 270s). Set ~30s to force the timeout-loop test.</summary>
    public TimeSpan? FetchTimeout { get; set; }

    /// <summary>Topic only: on a cold start (no stored replay id) begin from the earliest retained event.</summary>
    public bool? StartFromEarliest { get; set; }

    /// <summary>Override the listener heartbeat cadence (default 15m; 00:00:00 disables). Set ~2m to observe it live.</summary>
    public TimeSpan? HeartbeatInterval { get; set; }

    /// <summary>Override the watchdog cold-stream threshold (default 15m; 00:00:00 disables). Set short + kill the network to observe a trip.</summary>
    public TimeSpan? WatchdogThreshold { get; set; }
}
