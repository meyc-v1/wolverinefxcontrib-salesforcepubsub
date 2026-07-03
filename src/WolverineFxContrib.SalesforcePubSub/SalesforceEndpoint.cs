using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Wolverine.Configuration;
using Wolverine.Runtime;
using Wolverine.SalesforcePubSub.Events;
using Wolverine.SalesforcePubSub.Internals;
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

    // Map-only type binding: named entries key on the event API name (the Avro record name, e.g.
    // "CM_Test_Event_One__e"); the unconditional entry stamps every event (single-type topic/MES — the
    // degenerate case). The <T> sugar seals the map so it can't be mixed with MapEvent.
    internal Dictionary<string, Type> EventTypeMap { get; } = new(StringComparer.Ordinal);
    internal Type? UnconditionalEventType { get; private set; }
    internal bool EventMapSealed { get; private set; }

    internal bool IsChannel => Resource.EndsWith("__chn", StringComparison.Ordinal);

    // Per-endpoint overrides (null = inherit the transport-level default). Set via the fluent
    // SalesforceListenerConfiguration and merged into the effective settings in BuildListenerAsync.
    internal int? FetchCount { get; set; }
    internal TimeSpan? FetchTimeout { get; set; }
    internal bool? StartFromEarliest { get; set; }
    internal TimeSpan? HeartbeatInterval { get; set; }
    internal LogLevel? HeartbeatLogLevel { get; set; }
    internal TimeSpan? StaleStreamThreshold { get; set; }
    internal LogLevel? StaleStreamLogLevel { get; set; }

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
    /// Adds an event-type mapping. A null <paramref name="eventApiName"/> is the unconditional entry
    /// (every event on the stream decodes as this type); named entries resolve per event by the Avro
    /// record name. <paramref name="seal"/> marks the map complete (the single-type sugar) so a later
    /// MapEvent fails fast instead of silently changing semantics.
    /// </summary>
    internal void AddEventMapping(Type messageType, string? eventApiName, bool seal = false)
    {
        if (!typeof(PubSubEvent).IsAssignableFrom(messageType))
            throw new ArgumentException($"Message type '{messageType}' must derive from {nameof(PubSubEvent)}.", nameof(messageType));

        // Re-registering an identical mapping is a no-op — Wolverine's own transports treat repeated
        // endpoint configuration as idempotent, and config code touching the same topic twice is common.
        if (eventApiName is null && UnconditionalEventType == messageType)
        {
            EventMapSealed |= seal;
            return;
        }

        if (eventApiName is not null && EventTypeMap.TryGetValue(eventApiName, out var existing) && existing == messageType)
            return;

        if (EventMapSealed)
            throw new InvalidOperationException(eventApiName is null && seal
                ? $"Salesforce endpoint '{Resource}' is already registered with the single event type {UnconditionalEventType!.Name}; it cannot be re-registered with the conflicting type {messageType.Name}."
                : $"The event map for Salesforce endpoint '{Resource}' was configured with a single-type ListenTo…<T> registration and cannot be extended with MapEvent. Use the non-generic ListenToSalesforceTopic/ListenToSalesforceChannel/ListenToManagedSubscription overload with MapEvent<T>(…) to declare the event types explicitly.");

        if (eventApiName is null)
        {
            if (UnconditionalEventType is not null)
                throw new InvalidOperationException(
                    $"Salesforce endpoint '{Resource}' already has an unconditional event type ({UnconditionalEventType.Name}). Only one unnamed MapEvent entry is allowed; give each entry an event API name to map multiple types.");
            UnconditionalEventType = messageType;
            MessageType = messageType; // diagnostics parity with the single-type model
        }
        else
        {
            if (!EventTypeMap.TryAdd(eventApiName, messageType))
                throw new InvalidOperationException(
                    $"Salesforce endpoint '{Resource}' already maps event '{eventApiName}' to {EventTypeMap[eventApiName].Name}; it cannot also map to {messageType.Name}.");
        }

        if (seal)
            EventMapSealed = true;
    }

    /// <summary>Fail-fast cardinality/shape validation for the event map (see DECISIONS #16).</summary>
    internal void ValidateEventMap()
    {
        var total = EventTypeMap.Count + (UnconditionalEventType is null ? 0 : 1);

        if (total == 0)
            throw new InvalidOperationException(
                $"No event types configured for Salesforce endpoint '{Resource}'. Use ListenToSalesforceTopic<T> / ListenToManagedSubscription<T>, or the non-generic overloads with MapEvent<T>(…).");

        if (UnconditionalEventType is not null && EventTypeMap.Count > 0)
            throw new InvalidOperationException(
                $"Salesforce endpoint '{Resource}' mixes an unnamed MapEvent entry with named entries. An unnamed (unconditional) entry must be the only one; name every entry to map multiple types.");

        if (Kind == SalesforceResourceKind.Topic && !IsChannel && total > 1)
            throw new InvalidOperationException(
                $"Salesforce topic '{Resource}' maps {total} event types, but a plain platform-event topic delivers exactly one. Map a single type, or subscribe to a custom channel (…__chn) via ListenToSalesforceChannel for multi-type streams.");

        if (IsChannel && UnconditionalEventType is not null)
            throw new InvalidOperationException(
                $"Salesforce channel '{Resource}' has an unnamed MapEvent entry, but a custom channel delivers multiple event types — every entry must specify its event API name (e.g. MapEvent<MyEvent>(\"My_Event__e\")).");
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
            factory = resumeFrom => new TopicTransport(client, replayRepository, effective, logger, Resource, resumeFrom);
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

        var eventTypes = new EventTypeResolver(UnconditionalEventType, EventTypeMap, schemaRepository);

        // DI fills the listener's service params (schema repository, backoff, token provider, logger); we
        // supply the runtime-contextual ones, including the per-endpoint effective settings.
        var listener = ActivatorUtilities.CreateInstance<SalesforceListener>(
            services, Uri, Resource, resumesFromWatermark, factory, receiver, eventTypes, BuildPrewarmTopics(), effective, runtime.Cancellation);

        return ValueTask.FromResult((IListener)listener);
    }

    /// <summary>
    /// The topics whose schemas the listener eagerly pre-warms at startup: named MapEvent entries each have
    /// their own /event/&lt;ApiName&gt; topic; an unconditional topic endpoint warms its own resource. An
    /// unconditional MES has no topic to query (its channel is server-side config) and stays lazy.
    /// </summary>
    internal IReadOnlyList<string> BuildPrewarmTopics()
    {
        if (UnconditionalEventType is not null)
            return Kind == SalesforceResourceKind.Topic ? [Resource] : [];

        return EventTypeMap.Keys.Select(name => $"/event/{name}").ToArray();
    }

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
        StaleStreamThreshold = StaleStreamThreshold ?? defaults.StaleStreamThreshold,
        StaleStreamLogLevel = StaleStreamLogLevel ?? defaults.StaleStreamLogLevel,
        WatchdogPollingPeriod = defaults.WatchdogPollingPeriod,
        ProcessNewEventsIfReplayIdValidationFails = defaults.ProcessNewEventsIfReplayIdValidationFails,
        ReplayIdValidationFailedErrorCode = defaults.ReplayIdValidationFailedErrorCode
    };

    protected override ISender CreateSender(IWolverineRuntime runtime)
        => throw new NotSupportedException("The Salesforce Pub/Sub transport is listen-only; publishing is not supported.");
}
