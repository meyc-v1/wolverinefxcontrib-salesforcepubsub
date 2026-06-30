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
        var settings = services.GetRequiredService<SubscriberComponentsSettings>();
        var logger = runtime.LoggerFactory.CreateLogger<SalesforceListener>();

        Func<ISubscriptionTransport> factory;
        if (Kind == SalesforceResourceKind.Topic)
        {
            var replayRepository = services.GetRequiredService<IReplayIdRepository>();
            factory = () => new TopicTransport(client, replayRepository, settings, logger, Resource);
        }
        else
        {
            factory = () => new ManagedEventSubscriptionTransport(client, settings, logger, Resource);
        }

        // The serializer decodes Data-bearing envelopes by content-type in Wolverine's pipeline; the
        // listener pre-fetches each schema (async, in its loop) before handing the envelope off.
        RegisterSerializer(new SalesforceAvroSerializer(services.GetRequiredService<CachingSchemaRepository>()));

        // DI fills the listener's service params (schema repository, settings, backoff, token provider,
        // logger); we supply the runtime-contextual ones.
        var listener = ActivatorUtilities.CreateInstance<SalesforceListener>(
            services, Uri, Resource, factory, receiver, MessageType, runtime.Cancellation);

        return ValueTask.FromResult((IListener)listener);
    }

    protected override ISender CreateSender(IWolverineRuntime runtime)
        => throw new NotSupportedException("The Salesforce Pub/Sub transport is listen-only; publishing is not supported.");
}
