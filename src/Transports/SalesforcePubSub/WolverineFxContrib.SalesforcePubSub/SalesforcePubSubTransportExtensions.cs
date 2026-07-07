using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Wolverine.SalesforcePubSub.Internals.Authentication;
using Wolverine.SalesforcePubSub.Internals.Backoff;
using Wolverine.SalesforcePubSub.Internals.Replay;
using Wolverine.SalesforcePubSub.Internals.Schema;
using SalesforceGrpc;

namespace Wolverine.SalesforcePubSub;

public static class SalesforcePubSubTransportExtensions
{
    /// <summary>
    /// Enables the Salesforce Pub/Sub transport and registers its services (gRPC client, schema repo,
    /// replay/backoff defaults, token cache). Optionally override the gRPC endpoint; transport-level
    /// tuning and consumer implementations are configured on the returned
    /// <see cref="SalesforcePubSubTransportExpression"/>, per-endpoint tuning on each
    /// <see cref="SalesforceListenerConfiguration"/>.
    /// </summary>
    public static SalesforcePubSubTransportExpression UseSalesforcePubSub(this WolverineOptions options, Uri? pubSubUri = null)
    {
        var transport = options.Transports.GetOrCreate<SalesforcePubSubTransport>();

        // The settings live on the transport instance so a repeated UseSalesforcePubSub call returns an
        // expression over the SAME configuration (previously the second call silently mutated a fresh
        // settings object the container never registered). The service wiring runs once — the gRPC
        // client registration is not idempotent (a second AddCallCredentials would stack duplicate
        // metadata) — everything after the guard composes safely.
        var settings = transport.Settings;
        if (pubSubUri is not null)
            settings.PubSubUri = pubSubUri;

        if (transport.ServicesRegistered)
            return new SalesforcePubSubTransportExpression(transport, options, settings);
        transport.ServicesRegistered = true;

        var services = options.Services;
        services.TryAddSingleton(settings);
        services.TryAddSingleton<IReplayIdRepository, InMemoryReplayIdRepository>();
        services.TryAddSingleton<IBackoffStrategy, DefaultBackoffStrategy>();
        services.TryAddSingleton<ISchemaRepository, DefaultSchemaRepository>();
        services.TryAddSingleton<CachingSchemaRepository>();
        services.TryAddSingleton<CachingAuthenticationTokenProvider>();
        services.AddMemoryCache();

        // Explicitly named: the default name is the type's SHORT name ("PubSubClient"), so any other
        // library in the host that registers its own generated Pub/Sub client would share our options
        // bucket — addresses overwrite and call credentials stack across the two (observed live in a
        // host running this transport next to an older Pub/Sub subscriber lib).
        services.AddGrpcClient<PubSub.PubSubClient>("WolverineSalesforcePubSub", o => { o.Address = settings.PubSubUri; })
            .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                KeepAlivePingDelay = TimeSpan.FromSeconds(30),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(15),
                EnableMultipleHttp2Connections = true
            })
            .AddCallCredentials(async (context, metadata, provider) =>
            {
                // Token caching/invalidation is owned by the transport's provider, not the consumer's handler.
                var tokens = provider.GetRequiredService<CachingAuthenticationTokenProvider>();
                var tokenResponse = await tokens.GetTokenAsync(context.CancellationToken).ConfigureAwait(false);
                metadata.Add("accesstoken", tokenResponse.AccessToken);
                metadata.Add("instanceurl", tokenResponse.InstanceUri);
                metadata.Add("tenantid", tokenResponse.TenantId);
            });

        return new SalesforcePubSubTransportExpression(transport, options, settings);
    }

    /// <summary>
    /// Listen to a Salesforce topic with client-managed replay. The path may be a platform-event topic
    /// (<c>/event/My_Event__e</c>, exactly one event type) or a custom channel (<c>/event/My_Channel__chn</c>,
    /// one or more) — both are topics to the Pub/Sub API. Declare each event the stream carries with
    /// <c>MapEvent&lt;T&gt;("Api_Name__e")</c>; an event with no mapping is stamped with its raw record
    /// name and handled by Wolverine's missing-handler policy (dead-letter by default).
    /// </summary>
    public static SalesforceListenerConfiguration ListenToSalesforceTopic(this WolverineOptions options, string topicName)
        => ConfigureListener(options, SalesforceResourceKind.Topic, topicName);

    /// <summary>
    /// Listen to a Salesforce managed event subscription (MES) — Salesforce manages the replay position.
    /// The MES's server-side channel may carry one or more event types; declare each with
    /// <c>MapEvent&lt;T&gt;("Api_Name__e")</c>.
    /// </summary>
    public static SalesforceListenerConfiguration ListenToManagedSubscription(this WolverineOptions options, string subscriptionName)
        => ConfigureListener(options, SalesforceResourceKind.ManagedSubscription, subscriptionName);

    private static SalesforceListenerConfiguration ConfigureListener(WolverineOptions options, SalesforceResourceKind kind, string resource)
    {
        var transport = options.Transports.GetOrCreate<SalesforcePubSubTransport>();
        var endpoint = transport.EndpointForResource(kind, resource);
        endpoint.IsListener = true;
        return new SalesforceListenerConfiguration(endpoint);
    }
}
