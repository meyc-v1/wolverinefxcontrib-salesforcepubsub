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
        if (MessageType is null)
            throw new InvalidOperationException(
                $"No message type configured for Salesforce endpoint '{Resource}'. Use ListenToSalesforceTopic<T> / ListenToManagedSubscription<T>.");

        var services = runtime.Services;
        var client = services.GetRequiredService<PubSub.PubSubClient>();
        var settings = services.GetRequiredService<SubscriberComponentsSettings>();
        var backoff = services.GetRequiredService<IBackoffStrategy>();
        var deserializer = services.GetRequiredService<PlatformEventDeserializer>();
        var tokens = services.GetRequiredService<CachingAuthenticationTokenProvider>();
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

        var listener = new SalesforceListener(
            Uri, Resource, factory, receiver, MessageType, deserializer, settings, backoff, tokens, logger, runtime.Cancellation);

        return ValueTask.FromResult((IListener)listener);
    }

    protected override ISender CreateSender(IWolverineRuntime runtime)
        => throw new NotSupportedException("The Salesforce Pub/Sub transport is listen-only; publishing is not supported.");
}
