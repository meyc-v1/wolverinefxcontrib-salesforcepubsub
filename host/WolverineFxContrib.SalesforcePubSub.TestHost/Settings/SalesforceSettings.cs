using FluentValidation;
using Microsoft.Extensions.Options;
using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Settings;

/// <summary>
/// General Salesforce + pub/sub configuration, bound from the "salesforceSettings" section.
/// (Auth — ClientId/ClientSecret/LoginUri — lives separately under "salesforceAuthenticationSettings".)
/// </summary>
public sealed class SalesforceSettings
{
    /// <summary>REST data API base, e.g. https://your-org.my.salesforce.com/services/data/v64.0/ (trailing slash required).</summary>
    public Uri BaseUri { get; set; } = null!;

    /// <summary>Salesforce Pub/Sub gRPC endpoint. Defaults to the public Salesforce endpoint (see <see cref="SalesforceSettingsConfigurer"/>).</summary>
    public Uri PubSubUri { get; set; } = null!;

    /// <summary>The subscriptions to wire as Wolverine listening endpoints.</summary>
    public List<SalesforceSubscriptionOptions> Subscriptions { get; set; } = [];
}

/// <summary>Applies defaults (PostConfigure) and validates <see cref="SalesforceSettings"/>.</summary>
internal sealed class SalesforceSettingsConfigurer
    : IPostConfigureOptions<SalesforceSettings>, IValidateOptions<SalesforceSettings>
{
    /// <summary>The public Salesforce Pub/Sub gRPC endpoint, used when none is configured.</summary>
    public static readonly Uri DefaultPubSubUri = new("https://api.pubsub.salesforce.com:7443");

    public void PostConfigure(string? name, SalesforceSettings options)
        => options.PubSubUri ??= DefaultPubSubUri;

    public ValidateOptionsResult Validate(string? name, SalesforceSettings options)
    {
        var res = new SalesforceSettingsValidator().Validate(options);

        return res.IsValid
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(res.Errors.Select(x => x.ErrorMessage).ToList());
    }
}

internal sealed class SalesforceSettingsValidator : AbstractValidator<SalesforceSettings>
{
    public SalesforceSettingsValidator()
    {
        RuleFor(x => x.BaseUri).NotNull();
        RuleFor(x => x.PubSubUri).NotNull();

        RuleForEach(x => x.Subscriptions).ChildRules(sub =>
        {
            sub.RuleFor(x => x.Channel).NotEmpty();
            sub.RuleFor(x => x.MessageType).NotEmpty();
            sub.RuleFor(x => x.Type).IsInEnum();
        });
    }
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
