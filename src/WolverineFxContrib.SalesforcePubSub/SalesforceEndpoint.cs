using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Wolverine.Configuration;
using Wolverine.Runtime;
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

    // Per-endpoint overrides (null = inherit the transport-level default). Set via the fluent
    // SalesforceListenerConfiguration and merged into the effective settings in BuildListenerAsync.
    internal int? FetchCount { get; set; }
    internal TimeSpan? FetchTimeout { get; set; }
    internal bool? StartFromEarliest { get; set; }

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

    protected override bool supportsMode(EndpointMode mode)
        => mode is EndpointMode.Inline or EndpointMode.BufferedInMemory;

    public override ValueTask<IListener> BuildListenerAsync(IWolverineRuntime runtime, IReceiver receiver)
    {
        // Wolverine builds ListenerCount parallel listeners (ListeningAgent). For this transport each
        // listener opens its own gRPC subscription to the same channel, so >1 means duplicate delivery.
        if (ListenerCount > 1)
            throw new InvalidOperationException(
                $"The Salesforce Pub/Sub transport supports only a single listener per endpoint, but ListenerCount={ListenerCount} was configured for '{Resource}'. Multiple listeners would open duplicate gRPC subscriptions and cause duplicate delivery; leave ListenerCount at 1.");

        if (MessageType is null)
            throw new InvalidOperationException(
                $"No message type configured for Salesforce endpoint '{Resource}'. Use ListenToSalesforceTopic<T> / ListenToManagedSubscription<T>.");

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
        RegisterSerializer(new SalesforceAvroSerializer(services.GetRequiredService<CachingSchemaRepository>()));

        // DI fills the listener's service params (schema repository, backoff, token provider, logger); we
        // supply the runtime-contextual ones, including the per-endpoint effective settings.
        var listener = ActivatorUtilities.CreateInstance<SalesforceListener>(
            services, Uri, Resource, factory, receiver, MessageType, effective, runtime.Cancellation);

        return ValueTask.FromResult((IListener)listener);
    }

    /// <summary>Merge this endpoint's per-endpoint overrides over the transport-level defaults.</summary>
    internal SubscriberComponentsSettings ResolveEffectiveSettings(SubscriberComponentsSettings defaults) => new()
    {
        PubSubUri = defaults.PubSubUri,
        TokenCacheDuration = defaults.TokenCacheDuration,
        FetchCount = FetchCount ?? defaults.FetchCount,
        FetchTimeout = FetchTimeout ?? defaults.FetchTimeout,
        StartFromEarliest = StartFromEarliest ?? defaults.StartFromEarliest,
        ProcessNewEventsIfReplayIdValidationFails = defaults.ProcessNewEventsIfReplayIdValidationFails,
        ReplayIdValidationFailedErrorCode = defaults.ReplayIdValidationFailedErrorCode
    };

    protected override ISender CreateSender(IWolverineRuntime runtime)
        => throw new NotSupportedException("The Salesforce Pub/Sub transport is listen-only; publishing is not supported.");
}
