using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
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
    /// replay/backoff defaults, token cache). Optionally override the gRPC endpoint; transport-level tuning
    /// and the <see cref="IAuthenticationTokenHandler"/> are configured on the returned
    /// <see cref="SalesforcePubSubConfiguration"/>, and per-endpoint tuning on each
    /// <see cref="SalesforceListenerConfiguration"/>.
    /// </summary>
    public static SalesforcePubSubConfiguration UseSalesforcePubSub(this WolverineOptions options, Uri? pubSubUri = null)
    {
        var transport = options.Transports.GetOrCreate<SalesforcePubSubTransport>();

        var settings = new SubscriberComponentsSettings();
        if (pubSubUri is not null)
            settings.PubSubUri = pubSubUri;

        var services = options.Services;
        services.TryAddSingleton(settings);
        services.TryAddSingleton<IReplayIdRepository, InMemoryReplayIdRepository>();
        services.TryAddSingleton<IBackoffStrategy, DefaultBackoffStrategy>();
        services.TryAddSingleton<ISchemaRepository, DefaultSchemaRepository>();
        services.TryAddSingleton<CachingSchemaRepository>();
        services.TryAddSingleton<CachingAuthenticationTokenProvider>();
        services.AddMemoryCache();

        services.AddGrpcClient<PubSub.PubSubClient>(o => { o.Address = settings.PubSubUri; })
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

        return new SalesforcePubSubConfiguration(transport, options, settings);
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
    private readonly WolverineOptions _options;
    private readonly SubscriberComponentsSettings _settings;

    internal SalesforcePubSubConfiguration(SalesforcePubSubTransport transport, WolverineOptions options, SubscriberComponentsSettings settings)
    {
        _options = options;
        _settings = settings;
    }

    public SalesforcePubSubConfiguration UseAuthenticationHandler<T>(ServiceLifetime lifetime = ServiceLifetime.Singleton)
        where T : class, IAuthenticationTokenHandler
    {
        _options.Services.Add(new ServiceDescriptor(typeof(IAuthenticationTokenHandler), typeof(T), lifetime));
        return this;
    }

    /// <summary>How long the transport caches a Salesforce access token before re-fetching it (default 60 min).</summary>
    public SalesforcePubSubConfiguration TokenCacheDuration(TimeSpan duration)
    {
        _settings.TokenCacheDuration = duration;
        return this;
    }

    /// <summary>
    /// Cadence of each listener's periodic heartbeat log line (default 15 min; <see cref="TimeSpan.Zero"/>
    /// disables it), optionally with its log level (default Information; null leaves the level unchanged).
    /// </summary>
    public SalesforcePubSubConfiguration HeartbeatInterval(TimeSpan interval, LogLevel? logLevel = null)
    {
        _settings.HeartbeatInterval = interval;
        if (logLevel is { } level)
            _settings.HeartbeatLogLevel = level;
        return this;
    }

    /// <summary>Disables the periodic heartbeat log line for all listeners.</summary>
    public SalesforcePubSubConfiguration DisableHeartbeat()
    {
        _settings.HeartbeatInterval = TimeSpan.Zero;
        return this;
    }

    /// <summary>
    /// How long a listener may go without any successful response before the stale-stream watchdog logs
    /// "has not received a response" each poll and reconnect-failure logs escalate from Warning — both at
    /// the given log level (default Error; null leaves the level unchanged). Defaults to 15 min;
    /// <see cref="TimeSpan.Zero"/> disables the watchdog and the escalation.
    /// </summary>
    public SalesforcePubSubConfiguration StaleStreamThreshold(TimeSpan threshold, LogLevel? logLevel = null)
    {
        _settings.StaleStreamThreshold = threshold;
        if (logLevel is { } level)
            _settings.StaleStreamLogLevel = level;
        return this;
    }

    /// <summary>
    /// Disables the stale-stream watchdog for all listeners — and with it the reconnect-failure log-level
    /// escalation (those logs stay at Warning).
    /// </summary>
    public SalesforcePubSubConfiguration DisableStaleStreamWatchdog()
    {
        _settings.StaleStreamThreshold = TimeSpan.Zero;
        return this;
    }
}

/// <summary>
/// Per-endpoint fluent configuration for a Salesforce listener. Derives from Wolverine's
/// <see cref="ListenerConfiguration{TSelf,TEndpoint}"/> so consumers get the standard listener surface
/// (<c>ProcessInline</c>, <c>BufferedInMemory</c>, <c>Sequential</c>, <c>MaximumParallelMessages</c>,
/// <c>Named</c>, …). Unsupported modes are rejected by <see cref="SalesforceEndpoint"/>'s
/// <c>supportsMode</c>, and <c>ListenerCount</c> is constrained to 1 in
/// <see cref="SalesforceEndpoint.BuildListenerAsync"/> (multiple listeners would duplicate the stream).
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
    /// Topic subscriptions only; ignored once a replay id is known.
    /// </summary>
    public SalesforceListenerConfiguration StartFromEarliest(bool fromEarliest = true)
    {
        add(e => e.StartFromEarliest = fromEarliest);
        return this;
    }

    /// <summary>
    /// Override the heartbeat cadence for this listener (transport default 15 min; <see cref="TimeSpan.Zero"/>
    /// disables it), optionally with its log level (null inherits the transport-level log level).
    /// </summary>
    public SalesforceListenerConfiguration HeartbeatInterval(TimeSpan interval, LogLevel? logLevel = null)
    {
        add(e =>
        {
            e.HeartbeatInterval = interval;
            if (logLevel is { } level)
                e.HeartbeatLogLevel = level;
        });
        return this;
    }

    /// <summary>Disables the periodic heartbeat log line for this listener.</summary>
    public SalesforceListenerConfiguration DisableHeartbeat()
    {
        add(e => e.HeartbeatInterval = TimeSpan.Zero);
        return this;
    }

    /// <summary>
    /// Override the stale-stream threshold for this listener (transport default 15 min;
    /// <see cref="TimeSpan.Zero"/> disables the watchdog and the reconnect-failure log escalation),
    /// optionally with the stale log level (null inherits the transport-level log level).
    /// </summary>
    public SalesforceListenerConfiguration StaleStreamThreshold(TimeSpan threshold, LogLevel? logLevel = null)
    {
        add(e =>
        {
            e.StaleStreamThreshold = threshold;
            if (logLevel is { } level)
                e.StaleStreamLogLevel = level;
        });
        return this;
    }

    /// <summary>
    /// Disables the stale-stream watchdog for this listener — and with it the reconnect-failure log-level
    /// escalation (those logs stay at Warning).
    /// </summary>
    public SalesforceListenerConfiguration DisableStaleStreamWatchdog()
    {
        add(e => e.StaleStreamThreshold = TimeSpan.Zero);
        return this;
    }
}
