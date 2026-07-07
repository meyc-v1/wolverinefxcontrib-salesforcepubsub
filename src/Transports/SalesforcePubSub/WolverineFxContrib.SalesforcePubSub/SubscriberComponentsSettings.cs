using Microsoft.Extensions.Logging;

namespace Wolverine.SalesforcePubSub;

/// <summary>
/// Internal, effective per-listener settings. Transport-level values (PubSubUri, TokenCacheDuration) and
/// defaults are configured via <c>UseSalesforcePubSub</c> / <see cref="SalesforcePubSubTransportExpression"/>;
/// per-endpoint overrides (fetch count/timeout, start-from-earliest) layer on top via
/// <see cref="SalesforceListenerConfiguration"/> and are merged into an effective instance in
/// <see cref="SalesforceEndpoint.BuildListenerAsync"/>. Not part of the public surface.
/// </summary>
internal sealed class SubscriberComponentsSettings
{
    internal static readonly int DefaultFetchCount = 10;

    /// <summary>
    /// The Pub/Sub API's documented cap on events per fetch request; the server silently clamps larger
    /// requests to this, so the config surface rejects them instead.
    /// </summary>
    internal static readonly int MaxFetchCount = 100;
    internal static readonly TimeSpan DefaultFetchTimeout = TimeSpan.FromSeconds(270);
    internal static readonly TimeSpan DefaultTokenCacheDuration = TimeSpan.FromMinutes(60);
    internal static readonly Uri DefaultPubSubUri = new("https://api.pubsub.salesforce.com:7443");
    internal static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromMinutes(15);
    internal static readonly TimeSpan DefaultWatchdogThreshold = TimeSpan.FromMinutes(15);
    internal static readonly TimeSpan DefaultWatchdogPollingPeriod = TimeSpan.FromMinutes(1);

    public static readonly string DefaultReplayIdValidationFailedErrorCode =
        "sfdc.platform.eventbus.grpc.subscription.fetch.replayid.corrupted";

    internal static readonly TimeSpan DefaultRepositoryCallTimeout = TimeSpan.FromSeconds(30);

    public Uri PubSubUri { get; set; } = DefaultPubSubUri;
    public TimeSpan FetchTimeout { get; set; } = DefaultFetchTimeout;
    public int FetchCount { get; set; } = DefaultFetchCount;

    /// <summary>
    /// How many completions must accumulate before the throttled replay commit writes the advanced
    /// position (keep-alives and shutdown always flush regardless). Null — the default — tracks
    /// <see cref="FetchCount"/>, preserving the historical coupling; set explicitly to tune commit
    /// cadence (durability granularity vs. repository write volume) independently of fetch batching.
    /// </summary>
    public int? ReplayCommitThreshold { get; set; }

    /// <summary>
    /// Upper bound on any consumer <see cref="IReplayIdRepository"/> call (and the MES stream commit).
    /// The transport must not trust a consumer implementation to be prompt — a black-holed connection
    /// that hangs instead of failing wedged the read loop deaf in the 13.6h soak (DECISIONS #23). A
    /// timed-out call becomes an ordinary commit failure feeding the absorb-and-retry path.
    /// </summary>
    public TimeSpan RepositoryCallTimeout { get; set; } = DefaultRepositoryCallTimeout;

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

    /// <summary>
    /// Cadence of the listener's periodic heartbeat log line (uptime, response/event/error/reconnect
    /// counters, last success/error). Defaults to 15 minutes; <see cref="TimeSpan.Zero"/> disables it.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = DefaultHeartbeatInterval;

    /// <summary>Log level of the heartbeat line. Defaults to <see cref="LogLevel.Information"/>.</summary>
    public LogLevel HeartbeatLogLevel { get; set; } = LogLevel.Information;

    /// <summary>
    /// How long a listener may go without any successful response (event batch or keep-alive) before the
    /// stream is considered silently cold: the watchdog starts logging at <see cref="WatchdogLogLevel"/>
    /// each poll, and reconnect-failure logs escalate from Warning to that same level. Keep-alives arrive
    /// roughly every 2 minutes on a healthy idle stream, so this only trips on a genuinely cold one.
    /// Defaults to 15 minutes; <see cref="TimeSpan.Zero"/> disables both the watchdog and the escalation.
    /// </summary>
    public TimeSpan WatchdogThreshold { get; set; } = DefaultWatchdogThreshold;

    /// <summary>
    /// Log level used once <see cref="WatchdogThreshold"/> is exceeded — by the watchdog's
    /// "has not received a response" line and by escalated reconnect-failure logs. Defaults to
    /// <see cref="LogLevel.Error"/> (the alertable severity).
    /// </summary>
    public LogLevel WatchdogLogLevel { get; set; } = LogLevel.Error;

    /// <summary>How often the watchdog polls for a cold stream. Defaults to 1 minute.</summary>
    public TimeSpan WatchdogPollingPeriod { get; set; } = DefaultWatchdogPollingPeriod;
}
