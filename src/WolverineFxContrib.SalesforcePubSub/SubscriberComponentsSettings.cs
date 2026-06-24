namespace Wolverine.SalesforcePubSub;

/// <summary>
/// Transport-level tuning for a Salesforce Pub/Sub subscription. Per-endpoint configuration on the
/// Wolverine endpoint will layer on top of these defaults.
/// </summary>
public sealed class SubscriberComponentsSettings
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
