using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Wolverine.SalesforcePubSub;

/// <summary>
/// Transport-level fluent configuration returned by <c>UseSalesforcePubSub</c>. Named per Wolverine's
/// <c>KafkaTransportExpression</c> convention; it cannot derive from Wolverine's
/// <c>BrokerExpression&lt;…&gt;</c> because that base is paired with <c>BrokerTransport</c> and a sender
/// side this listen-only transport deliberately does not have (see DECISIONS).
/// </summary>
public sealed class SalesforcePubSubTransportExpression
{
    private readonly WolverineOptions _options;
    private readonly SubscriberComponentsSettings _settings;

    internal SalesforcePubSubTransportExpression(SalesforcePubSubTransport transport, WolverineOptions options, SubscriberComponentsSettings settings)
    {
        _options = options;
        _settings = settings;
    }

    /// <summary>Registers the consumer's token handler. It must fetch fresh tokens and not cache — the transport owns caching and invalidation.</summary>
    public SalesforcePubSubTransportExpression UseAuthenticationHandler<T>(ServiceLifetime lifetime = ServiceLifetime.Singleton)
        where T : class, IAuthenticationTokenHandler
    {
        _options.Services.Add(new ServiceDescriptor(typeof(IAuthenticationTokenHandler), typeof(T), lifetime));
        return this;
    }

    /// <summary>
    /// Registers the durable replay-id store for client-managed replay subscriptions, replacing the
    /// in-memory default (which does not survive a restart — use a durable implementation in production).
    /// </summary>
    public SalesforcePubSubTransportExpression UseReplayIdRepository<T>(ServiceLifetime lifetime = ServiceLifetime.Singleton)
        where T : class, IReplayIdRepository
    {
        _options.Services.Replace(new ServiceDescriptor(typeof(IReplayIdRepository), typeof(T), lifetime));
        return this;
    }

    /// <summary>Registers a custom reconnect backoff strategy, replacing the default (linear +15s per consecutive error, capped at 2 min).</summary>
    public SalesforcePubSubTransportExpression UseBackoffStrategy<T>(ServiceLifetime lifetime = ServiceLifetime.Singleton)
        where T : class, IBackoffStrategy
    {
        _options.Services.Replace(new ServiceDescriptor(typeof(IBackoffStrategy), typeof(T), lifetime));
        return this;
    }

    /// <summary>How long the transport caches a Salesforce access token before re-fetching it (default 60 min).</summary>
    public SalesforcePubSubTransportExpression TokenCacheDuration(TimeSpan duration)
    {
        _settings.TokenCacheDuration = duration;
        return this;
    }

    /// <summary>The periodic per-listener heartbeat log line (default: every 15 min at Information).</summary>
    public HeartbeatExpression Heartbeat => new(this, _settings);

    /// <summary>The silent-cold watchdog: alertable logging once a listener has received nothing for the threshold (default: 15 min threshold, 1 min polls, Error).</summary>
    public WatchdogExpression Watchdog => new(this, _settings);

    /// <summary>Transport-level defaults for the heartbeat; endpoints may override per listener.</summary>
    public readonly struct HeartbeatExpression
    {
        private readonly SalesforcePubSubTransportExpression _parent;
        private readonly SubscriberComponentsSettings _settings;

        internal HeartbeatExpression(SalesforcePubSubTransportExpression parent, SubscriberComponentsSettings settings)
        {
            _parent = parent;
            _settings = settings;
        }

        /// <summary>Heartbeat cadence (default 15 min).</summary>
        public SalesforcePubSubTransportExpression Interval(TimeSpan interval)
        {
            _settings.HeartbeatInterval = interval;
            return _parent;
        }

        /// <summary>Log level of the heartbeat line (default Information).</summary>
        public SalesforcePubSubTransportExpression Level(LogLevel level)
        {
            _settings.HeartbeatLogLevel = level;
            return _parent;
        }

        /// <summary>Turns the heartbeat off for all listeners.</summary>
        public SalesforcePubSubTransportExpression Disable()
        {
            _settings.HeartbeatInterval = TimeSpan.Zero;
            return _parent;
        }
    }

    /// <summary>Transport-level defaults for the watchdog; endpoints may override per listener.</summary>
    public readonly struct WatchdogExpression
    {
        private readonly SalesforcePubSubTransportExpression _parent;
        private readonly SubscriberComponentsSettings _settings;

        internal WatchdogExpression(SalesforcePubSubTransportExpression parent, SubscriberComponentsSettings settings)
        {
            _parent = parent;
            _settings = settings;
        }

        /// <summary>
        /// How long a listener may receive nothing (not even a keep-alive) before the watchdog logs each
        /// poll and reconnect-failure logs escalate (default 15 min). Healthy idle streams keep-alive
        /// roughly every 2 minutes, so this only trips on a genuinely cold stream.
        /// </summary>
        public SalesforcePubSubTransportExpression Threshold(TimeSpan threshold)
        {
            _settings.WatchdogThreshold = threshold;
            return _parent;
        }

        /// <summary>How often the watchdog checks for a cold stream (default 1 min).</summary>
        public SalesforcePubSubTransportExpression PollingInterval(TimeSpan interval)
        {
            _settings.WatchdogPollingPeriod = interval;
            return _parent;
        }

        /// <summary>Log level for the watchdog line and escalated reconnect failures (default Error).</summary>
        public SalesforcePubSubTransportExpression Level(LogLevel level)
        {
            _settings.WatchdogLogLevel = level;
            return _parent;
        }

        /// <summary>Turns the watchdog — and the reconnect-failure log escalation — off for all listeners.</summary>
        public SalesforcePubSubTransportExpression Disable()
        {
            _settings.WatchdogThreshold = TimeSpan.Zero;
            return _parent;
        }
    }
}
