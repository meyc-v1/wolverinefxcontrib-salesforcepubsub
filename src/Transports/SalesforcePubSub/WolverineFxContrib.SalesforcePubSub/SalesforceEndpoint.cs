using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Wolverine.Configuration;
using Wolverine.Runtime;
using Wolverine.SalesforcePubSub.Events;
using Wolverine.SalesforcePubSub.Internals;
using Wolverine.SalesforcePubSub.Internals.Authentication;
using Wolverine.SalesforcePubSub.Internals.Schema;
using Wolverine.SalesforcePubSub.Internals.Transports;
using Wolverine.Transports;
using Wolverine.Transports.Sending;
using SalesforceGrpc;

namespace Wolverine.SalesforcePubSub;

public enum SalesforceResourceKind
{
    Topic,
    ManagedSubscription
}

/// <summary>
/// A single Salesforce Pub/Sub listening endpoint — either a topic subscription (client-side replay)
/// or a managed event subscription (server-side replay). Listen-only.
/// </summary>
public sealed class SalesforceEndpoint : Endpoint
{
    internal SalesforcePubSubTransport Parent { get; }
    internal SalesforceResourceKind Kind { get; }
    internal string Resource { get; }

    // Multi-type-first binding (DECISIONS #19): every entry keys on the event API name (the Avro record
    // name, e.g. "CM_Test_Event_One__e"). A single-type subscription is just the one-entry case.
    internal Dictionary<string, Type> EventTypeMap { get; } = new(StringComparer.Ordinal);

    internal bool IsSingleEventTopic =>
        Kind == SalesforceResourceKind.Topic && Resource.EndsWith("__e", StringComparison.Ordinal);

    // Per-endpoint overrides (null = inherit the transport-level default). Set via the fluent
    // SalesforceListenerConfiguration and merged into the effective settings in BuildListenerAsync.
    internal int? FetchCount { get; set; }
    internal TimeSpan? FetchTimeout { get; set; }
    internal bool? StartFromEarliest { get; set; }
    internal TimeSpan? HeartbeatInterval { get; set; }
    internal LogLevel? HeartbeatLogLevel { get; set; }
    internal TimeSpan? WatchdogThreshold { get; set; }
    internal LogLevel? WatchdogLogLevel { get; set; }

    internal SalesforceEndpoint(SalesforcePubSubTransport parent, SalesforceResourceKind kind, string resource, EndpointRole role)
        : base(BuildUri(kind, resource), role)
    {
        Parent = parent;
        Kind = kind;
        Resource = resource;
        EndpointName = resource;
        Mode = EndpointMode.Inline;
    }

    internal static Uri BuildUri(SalesforceResourceKind kind, string resource)
    {
        var segment = kind == SalesforceResourceKind.Topic ? "topic" : "mes";
        return new Uri($"{SalesforcePubSubTransport.ProtocolName}://{segment}/{Uri.EscapeDataString(resource)}");
    }

    /// <summary>
    /// Adds an event-type mapping, keyed by the event API name (the Avro record name each event's schema
    /// carries). Re-registering an identical mapping is a no-op — Wolverine's own transports treat
    /// repeated endpoint configuration as idempotent; a conflicting type for the same name throws.
    /// </summary>
    internal void AddEventMapping(Type messageType, string eventApiName)
    {
        if (!typeof(PubSubEvent).IsAssignableFrom(messageType))
            throw new ArgumentException($"Message type '{messageType}' must derive from {nameof(PubSubEvent)}.", nameof(messageType));

        if (EventTypeMap.TryGetValue(eventApiName, out var existing))
        {
            if (existing == messageType)
                return;
            throw new InvalidOperationException(
                $"Salesforce endpoint '{Resource}' already maps event '{eventApiName}' to {existing.Name}; it cannot also map to {messageType.Name}.");
        }

        EventTypeMap.Add(eventApiName, messageType);

        // Deliberately NOT setting the base Endpoint.MessageType, even for a single-entry map: Wolverine
        // turns a non-null MessageType into an incoming MessageTypeRule that overwrites the envelope's
        // MessageType on EVERY received envelope — clobbering the listener's per-event resolution and
        // force-decoding unmapped events into the one mapped type instead of letting them ride the
        // missing-handler path (found live by the integration suite on a single-MapEvent channel endpoint).
    }

    /// <summary>Fail-fast shape validation for the event map (DECISIONS #19).</summary>
    internal void ValidateEventMap()
    {
        if (EventTypeMap.Count == 0)
            throw new InvalidOperationException(
                $"No event types configured for Salesforce endpoint '{Resource}'. Declare each event with MapEvent<T>(\"Api_Name__e\").");

        if (IsSingleEventTopic)
        {
            // A plain platform-event topic delivers exactly one event type, and its API name is the
            // path's last segment — a mismatch would dead-letter every event at runtime, so fail fast.
            // (Non-__e paths, e.g. standard platform events, skip the name check until their record-name
            // convention is live-verified; __chn channels and MES are inherently 1..N.)
            if (EventTypeMap.Count > 1)
                throw new InvalidOperationException(
                    $"Salesforce topic '{Resource}' maps {EventTypeMap.Count} event types, but a plain platform-event topic delivers exactly one. Subscribe to a custom channel (…__chn) for multi-type streams.");

            var expected = Resource[(Resource.LastIndexOf('/') + 1)..];
            var actual = EventTypeMap.Keys.First();
            if (!string.Equals(expected, actual, StringComparison.Ordinal))
                throw new InvalidOperationException(
                    $"Salesforce topic '{Resource}' delivers '{expected}', but the endpoint maps '{actual}' — every event would dead-letter at runtime. Fix the MapEvent name (or the topic path).");
        }
    }

    protected override bool supportsMode(EndpointMode mode)
        => mode is EndpointMode.Inline or EndpointMode.BufferedInMemory or EndpointMode.Durable;

    public override ValueTask<IListener> BuildListenerAsync(IWolverineRuntime runtime, IReceiver receiver)
    {
        // Wolverine builds ListenerCount parallel listeners (ListeningAgent). For this transport each
        // listener opens its own gRPC subscription to the same channel, so >1 means duplicate delivery.
        if (ListenerCount > 1)
            throw new InvalidOperationException(
                $"The Salesforce Pub/Sub transport supports only a single listener per endpoint, but ListenerCount={ListenerCount} was configured for '{Resource}'. Multiple listeners would open duplicate gRPC subscriptions and cause duplicate delivery; leave ListenerCount at 1.");

        ValidateEventMap();

        var services = runtime.Services;
        var client = services.GetRequiredService<PubSub.PubSubClient>();
        var effective = ResolveEffectiveSettings(services.GetRequiredService<SubscriberComponentsSettings>());
        var logger = runtime.LoggerFactory.CreateLogger<SalesforceListener>();

        // The resume anchor is the listener's in-memory handled watermark on reconnect, null on cold start.
        // Topic uses it to resume without re-reading the repository; MES ignores it (server-side replay).
        Func<long?, ISubscriptionTransport> factory;
        if (Kind == SalesforceResourceKind.Topic)
        {
            var replayRepository = services.GetRequiredService<IReplayIdRepository>();
            factory = resumeFrom => new ClientManagedReplayTransport(client, replayRepository, effective, logger, Resource, resumeFrom);
        }
        else
        {
            factory = _ => new ManagedEventSubscriptionTransport(client, effective, logger, Resource);
        }

        // The serializer decodes Data-bearing envelopes by content-type in Wolverine's pipeline; the
        // listener pre-fetches each schema (async, in its loop) before handing the envelope off.
        var schemaRepository = services.GetRequiredService<CachingSchemaRepository>();
        RegisterSerializer(new SalesforceAvroSerializer(
            schemaRepository,
            services.GetRequiredService<CachingAuthenticationTokenProvider>(),
            runtime.LoggerFactory.CreateLogger<SalesforceAvroSerializer>()));

        // Topic honors the in-memory handled watermark on reconnect (#8); MES resumes from the server-side
        // checkpoint. The flag only governs reconnect-log accuracy — the MES factory already discards resumeFrom.
        var resumesFromWatermark = Kind == SalesforceResourceKind.Topic;

        var eventTypes = new EventTypeResolver(EventTypeMap, schemaRepository);

        // DI fills the listener's service params (schema repository, backoff, token provider, logger); we
        // supply the runtime-contextual ones, including the per-endpoint effective settings. Construction
        // wires everything; Start() below is the explicit "begin consuming" — the SPI has no start method
        // (built == listening), so the lifecycle line lives here where the wiring reads top to bottom.
        var listener = ActivatorUtilities.CreateInstance<SalesforceListener>(
            services, Uri, Resource, resumesFromWatermark, factory, receiver, eventTypes, BuildPrewarmTopics(), effective, runtime.Cancellation);
        listener.Start();

        return ValueTask.FromResult((IListener)listener);
    }

    /// <summary>
    /// The topics whose schemas the listener eagerly pre-warms at startup: every MapEvent entry names an
    /// event, and every event has its own /event/&lt;ApiName&gt; topic — so all endpoints pre-warm,
    /// including MES (whose own resource is a developer name we could not query).
    /// </summary>
    internal IReadOnlyList<string> BuildPrewarmTopics()
        => EventTypeMap.Keys.Select(name => $"/event/{name}").ToArray();

    /// <summary>Merge this endpoint's per-endpoint overrides over the transport-level defaults.</summary>
    internal SubscriberComponentsSettings ResolveEffectiveSettings(SubscriberComponentsSettings defaults) => new()
    {
        PubSubUri = defaults.PubSubUri,
        TokenCacheDuration = defaults.TokenCacheDuration,
        FetchCount = FetchCount ?? defaults.FetchCount,
        FetchTimeout = FetchTimeout ?? defaults.FetchTimeout,
        StartFromEarliest = StartFromEarliest ?? defaults.StartFromEarliest,
        HeartbeatInterval = HeartbeatInterval ?? defaults.HeartbeatInterval,
        HeartbeatLogLevel = HeartbeatLogLevel ?? defaults.HeartbeatLogLevel,
        WatchdogThreshold = WatchdogThreshold ?? defaults.WatchdogThreshold,
        WatchdogLogLevel = WatchdogLogLevel ?? defaults.WatchdogLogLevel,
        WatchdogPollingPeriod = defaults.WatchdogPollingPeriod,
        ProcessNewEventsIfReplayIdValidationFails = defaults.ProcessNewEventsIfReplayIdValidationFails,
        ReplayIdValidationFailedErrorCode = defaults.ReplayIdValidationFailedErrorCode
    };

    protected override ISender CreateSender(IWolverineRuntime runtime)
        => throw new NotSupportedException("The Salesforce Pub/Sub transport is listen-only; publishing is not supported.");
}
