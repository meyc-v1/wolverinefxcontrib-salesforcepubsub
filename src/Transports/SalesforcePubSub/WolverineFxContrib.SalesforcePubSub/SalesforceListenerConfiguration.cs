using Microsoft.Extensions.Logging;
using Wolverine.Configuration;
using Wolverine.SalesforcePubSub.Events;

namespace Wolverine.SalesforcePubSub;

/// <summary>
/// Per-endpoint fluent configuration for a Salesforce listener. Derives from Wolverine's
/// <see cref="ListenerConfiguration{TSelf,TEndpoint}"/> so consumers get the standard listener surface
/// (<c>ProcessInline</c>, <c>BufferedInMemory</c>, <c>UseDurableInbox</c>, <c>Named</c>, …). Unsupported
/// modes are rejected by <see cref="SalesforceEndpoint"/>'s <c>supportsMode</c>, and <c>ListenerCount</c>
/// is constrained to 1 in <see cref="SalesforceEndpoint.BuildListenerAsync"/> (multiple listeners would
/// duplicate the stream).
/// </summary>
public class SalesforceListenerConfiguration
    : ListenerConfiguration<SalesforceListenerConfiguration, SalesforceEndpoint>
{
    internal SalesforceListenerConfiguration(SalesforceEndpoint endpoint) : base(endpoint)
    {
    }

    internal SalesforceListenerConfiguration(Func<SalesforceEndpoint> source) : base(source)
    {
    }

    /// <summary>
    /// Declares an event this subscription carries: the event API name (the Avro record name, e.g.
    /// <c>"My_Event__e"</c>) and the .NET type it deserializes into. Every event type is declared this
    /// way — a single-event topic has one entry, a custom channel or MES one per member (DECISIONS #19).
    /// An event arriving with no mapping is dead-lettered by Wolverine's missing-handler policy.
    /// </summary>
    public SalesforceListenerConfiguration MapEvent<T>(string eventApiName) where T : PubSubEvent
        => MapEvent(typeof(T), eventApiName);

    /// <summary>Runtime-typed overload of <see cref="MapEvent{T}(string)"/> (e.g. for configuration-driven wiring).</summary>
    public SalesforceListenerConfiguration MapEvent(Type messageType, string eventApiName)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentException.ThrowIfNullOrWhiteSpace(eventApiName);
        add(e => e.AddEventMapping(messageType, eventApiName));
        return this;
    }

    /// <summary>
    /// Declares an event whose API name is carried on the type via
    /// <see cref="SalesforcePlatformEventAttribute"/> — the declaration lives with the type instead of
    /// being repeated at each registration. An explicit name overload always wins over the attribute.
    /// </summary>
    public SalesforceListenerConfiguration MapEvent<T>() where T : PubSubEvent
        => MapEvent(typeof(T));

    /// <summary>Runtime-typed overload of the attribute-based <see cref="MapEvent{T}()"/>.</summary>
    public SalesforceListenerConfiguration MapEvent(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);

        var attribute = (SalesforcePlatformEventAttribute?)Attribute.GetCustomAttribute(messageType, typeof(SalesforcePlatformEventAttribute))
            ?? throw new InvalidOperationException(
                $"'{messageType.Name}' has no [{nameof(SalesforcePlatformEventAttribute)}] declaring its event API name. Decorate the type ([SalesforcePlatformEvent(\"My_Event__e\")]) or pass the name explicitly: MapEvent<{messageType.Name}>(\"My_Event__e\").");

        return MapEvent(messageType, attribute.EventApiName);
    }

    /// <summary>Override the fetch batch size for this listener (default 10).</summary>
    public SalesforceListenerConfiguration FetchCount(int count)
    {
        add(e => e.FetchCount = count);
        return this;
    }

    /// <summary>Override the idle/fetch timeout that triggers a reconnect for this listener (default 270s).</summary>
    public SalesforceListenerConfiguration FetchTimeout(TimeSpan timeout)
    {
        add(e => e.FetchTimeout = timeout);
        return this;
    }

    /// <summary>
    /// On a cold start (no stored replay id) begin from the earliest retained event instead of the latest.
    /// Client-managed replay subscriptions only; ignored once a replay id is known.
    /// </summary>
    public SalesforceListenerConfiguration StartFromEarliest(bool fromEarliest = true)
    {
        add(e => e.StartFromEarliest = fromEarliest);
        return this;
    }

    /// <summary>Per-listener heartbeat overrides (unset values inherit the transport-level defaults).</summary>
    public HeartbeatExpression Heartbeat => new(this);

    /// <summary>Per-listener watchdog overrides (unset values inherit the transport-level defaults).</summary>
    public WatchdogExpression Watchdog => new(this);

    // The grouped expressions write endpoint overrides through the protected CRTP add() mechanism.
    private SalesforceListenerConfiguration Set(Action<SalesforceEndpoint> configure)
    {
        add(configure);
        return this;
    }

    public readonly struct HeartbeatExpression
    {
        private readonly SalesforceListenerConfiguration _parent;
        internal HeartbeatExpression(SalesforceListenerConfiguration parent) => _parent = parent;

        /// <summary>Heartbeat cadence for this listener (<see cref="TimeSpan.Zero"/> disables).</summary>
        public SalesforceListenerConfiguration Interval(TimeSpan interval)
            => _parent.Set(e => e.HeartbeatInterval = interval);

        /// <summary>Log level of this listener's heartbeat line.</summary>
        public SalesforceListenerConfiguration Level(LogLevel level)
            => _parent.Set(e => e.HeartbeatLogLevel = level);

        /// <summary>Turns the heartbeat off for this listener.</summary>
        public SalesforceListenerConfiguration Disable()
            => _parent.Set(e => e.HeartbeatInterval = TimeSpan.Zero);
    }

    public readonly struct WatchdogExpression
    {
        private readonly SalesforceListenerConfiguration _parent;
        internal WatchdogExpression(SalesforceListenerConfiguration parent) => _parent = parent;

        /// <summary>Cold-stream threshold for this listener (<see cref="TimeSpan.Zero"/> disables the watchdog and the log escalation).</summary>
        public SalesforceListenerConfiguration Threshold(TimeSpan threshold)
            => _parent.Set(e => e.WatchdogThreshold = threshold);

        /// <summary>Log level for this listener's watchdog line and escalated reconnect failures.</summary>
        public SalesforceListenerConfiguration Level(LogLevel level)
            => _parent.Set(e => e.WatchdogLogLevel = level);

        /// <summary>Turns the watchdog — and the reconnect-failure log escalation — off for this listener.</summary>
        public SalesforceListenerConfiguration Disable()
            => _parent.Set(e => e.WatchdogThreshold = TimeSpan.Zero);
    }
}
