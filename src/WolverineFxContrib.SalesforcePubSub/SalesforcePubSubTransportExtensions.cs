using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Wolverine.Configuration;
using Wolverine.SalesforcePubSub.Events;
using Wolverine.SalesforcePubSub.Internals;
using Wolverine.Transports;
using SalesforceGrpc;

namespace Wolverine.SalesforcePubSub;

public static class SalesforcePubSubTransportExtensions
{
    /// <summary>
    /// Enables the Salesforce Pub/Sub transport and registers its services (gRPC client, schema repo,
    /// deserializer, and default replay/backoff strategies). Register an
    /// <see cref="IAuthenticationTokenHandler"/> via <see cref="SalesforcePubSubConfiguration.UseAuthenticationHandler{T}"/>.
    /// </summary>
    public static SalesforcePubSubConfiguration UseSalesforcePubSub(this WolverineOptions options, Action<SubscriberComponentsSettings>? configure = null)
    {
        var transport = options.Transports.GetOrCreate<SalesforcePubSubTransport>();

        var settings = new SubscriberComponentsSettings();
        configure?.Invoke(settings);

        var services = options.Services;
        services.TryAddSingleton(settings);
        services.TryAddSingleton<IReplayIdRepository, InMemoryReplayIdRepository>();
        services.TryAddSingleton<IBackoffStrategy, DefaultBackoffStrategy>();
        services.TryAddSingleton<ISchemaRepository, DefaultSchemaRepository>();
        services.TryAddSingleton<CachingSchemaRepository>();
        services.TryAddSingleton<PlatformEventDeserializer>();
        services.AddMemoryCache();

        services.AddGrpcClient<PubSub.PubSubClient>(o => { o.Address = settings.PubSubUri; })
            .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                KeepAlivePingDelay = TimeSpan.FromSeconds(30),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(15),
                EnableMultipleHttp2Connections = true
            })
            .AddCallCredentials(async (_, metadata, provider) =>
            {
                var handler = provider.GetRequiredService<IAuthenticationTokenHandler>();
                var tokenResponse = await handler.GetAuthenticationTokenAsync().ConfigureAwait(false);
                metadata.Add("accesstoken", tokenResponse.AccessToken);
                metadata.Add("instanceurl", tokenResponse.InstanceUri);
                metadata.Add("tenantid", tokenResponse.TenantId);
            });

        return new SalesforcePubSubConfiguration(transport, options);
    }

    /// <summary>Listen to a Salesforce topic (e.g. a Platform Event channel) with client-side replay tracking.</summary>
    public static SalesforceListenerConfiguration ListenToSalesforceTopic<T>(this WolverineOptions options, string topicName)
        where T : PubSubEvent
        => options.ListenToSalesforceTopic(topicName, typeof(T));

    /// <summary>Listen to a Salesforce topic with the message type supplied at runtime (e.g. from configuration).</summary>
    public static SalesforceListenerConfiguration ListenToSalesforceTopic(this WolverineOptions options, string topicName, Type messageType)
        => ConfigureListener(options, SalesforceResourceKind.Topic, topicName, messageType);

    /// <summary>Listen to a Salesforce managed event subscription (MES) with server-side replay tracking.</summary>
    public static SalesforceListenerConfiguration ListenToManagedSubscription<T>(this WolverineOptions options, string subscriptionName)
        where T : PubSubEvent
        => options.ListenToManagedSubscription(subscriptionName, typeof(T));

    /// <summary>Listen to a managed event subscription with the message type supplied at runtime.</summary>
    public static SalesforceListenerConfiguration ListenToManagedSubscription(this WolverineOptions options, string subscriptionName, Type messageType)
        => ConfigureListener(options, SalesforceResourceKind.ManagedSubscription, subscriptionName, messageType);

    private static SalesforceListenerConfiguration ConfigureListener(WolverineOptions options, SalesforceResourceKind kind, string resource, Type messageType)
    {
        if (!typeof(PubSubEvent).IsAssignableFrom(messageType))
            throw new ArgumentException($"Message type '{messageType}' must derive from {nameof(PubSubEvent)}.", nameof(messageType));

        var transport = options.Transports.GetOrCreate<SalesforcePubSubTransport>();
        var endpoint = transport.EndpointForResource(kind, resource);
        endpoint.MessageType = messageType;
        endpoint.IsListener = true;
        return new SalesforceListenerConfiguration(endpoint);
    }
}

/// <summary>Fluent configuration returned by <c>UseSalesforcePubSub</c>.</summary>
public sealed class SalesforcePubSubConfiguration
{
    private readonly SalesforcePubSubTransport _transport;
    private readonly WolverineOptions _options;

    internal SalesforcePubSubConfiguration(SalesforcePubSubTransport transport, WolverineOptions options)
    {
        _transport = transport;
        _options = options;
    }

    public SalesforcePubSubConfiguration UseAuthenticationHandler<T>(ServiceLifetime lifetime = ServiceLifetime.Singleton)
        where T : class, IAuthenticationTokenHandler
    {
        _options.Services.Add(new ServiceDescriptor(typeof(IAuthenticationTokenHandler), typeof(T), lifetime));
        return this;
    }
}

/// <summary>Per-endpoint fluent configuration for a Salesforce listener.</summary>
public sealed class SalesforceListenerConfiguration
{
    private readonly SalesforceEndpoint _endpoint;

    internal SalesforceListenerConfiguration(SalesforceEndpoint endpoint) => _endpoint = endpoint;

    /// <summary>Process events inline (at-least-once: replay advances only after the handler runs). The default.</summary>
    public SalesforceListenerConfiguration ProcessInline()
    {
        _endpoint.Mode = EndpointMode.Inline;
        return this;
    }

    /// <summary>Buffer events in memory for parallel processing (at-most-once on handler failure).</summary>
    public SalesforceListenerConfiguration BufferedInMemory()
    {
        _endpoint.Mode = EndpointMode.BufferedInMemory;
        return this;
    }
}
