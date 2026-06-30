namespace Wolverine.SalesforcePubSub;

/// <summary>
/// Internal, effective per-listener settings. Transport-level values (PubSubUri, TokenCacheDuration) and
/// defaults are configured via <c>UseSalesforcePubSub</c> / <see cref="SalesforcePubSubConfiguration"/>;
/// per-endpoint overrides (fetch count/timeout, start-from-earliest) layer on top via
/// <see cref="SalesforceListenerConfiguration"/> and are merged into an effective instance in
/// <see cref="SalesforceEndpoint.BuildListenerAsync"/>. Not part of the public surface.
/// </summary>
internal sealed class SubscriberComponentsSettings
{
    internal static readonly int DefaultFetchCount = 10;
    internal static readonly TimeSpan DefaultFetchTimeout = TimeSpan.FromSeconds(270);
    internal static readonly TimeSpan DefaultTokenCacheDuration = TimeSpan.FromMinutes(60);
    internal static readonly Uri DefaultPubSubUri = new("https://api.pubsub.salesforce.com:7443");

    public static readonly string DefaultReplayIdValidationFailedErrorCode =
        "sfdc.platform.eventbus.grpc.subscription.fetch.replayid.corrupted";

    public Uri PubSubUri { get; set; } = DefaultPubSubUri;
    public TimeSpan FetchTimeout { get; set; } = DefaultFetchTimeout;
    public int FetchCount { get; set; } = DefaultFetchCount;

    /// <summary>
    /// How long the transport caches a Salesforce access token before re-fetching it from the
    /// registered <see cref="IAuthenticationTokenHandler"/>. The cache is also invalidated reactively
    /// on an authentication failure, so this is the proactive-refresh backstop. Defaults to 60 minutes.
    /// </summary>
    public TimeSpan TokenCacheDuration { get; set; } = DefaultTokenCacheDuration;

    /// <summary>
    /// When true, a cold-start topic subscription (no stored replay id) begins from
    /// <c>ReplayPreset.Earliest</c> instead of <c>ReplayPreset.Latest</c>. Only affects the initial
    /// fetch; once a replay id is known the subscription pages forward via Custom.
    /// </summary>
    public bool StartFromEarliest { get; set; }

    public bool ProcessNewEventsIfReplayIdValidationFails { get; set; } = true;
    public string ReplayIdValidationFailedErrorCode { get; set; } = DefaultReplayIdValidationFailedErrorCode;
}
