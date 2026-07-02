using Microsoft.Extensions.Logging;
using Wolverine;
using Wolverine.Configuration;
using Wolverine.SalesforcePubSub;
using Wolverine.SalesforcePubSub.Internals;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// Per-endpoint config (Phase 4): the fluent setters set endpoint overrides, and effective settings take
/// the per-endpoint override when present, otherwise the transport-level default.
/// </summary>
public class PerEndpointConfigTests
{
    [Fact]
    public void Effective_settings_take_endpoint_overrides_then_transport_defaults()
    {
        var transport = new SalesforcePubSubTransport();
        var endpoint = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/A__e");
        endpoint.FetchCount = 50; // override; FetchTimeout / StartFromEarliest left to inherit

        var defaults = new SubscriberComponentsSettings
        {
            FetchCount = 10,
            FetchTimeout = TimeSpan.FromSeconds(99),
            StartFromEarliest = true
        };

        var effective = endpoint.ResolveEffectiveSettings(defaults);

        Assert.Equal(50, effective.FetchCount);                         // override wins
        Assert.Equal(TimeSpan.FromSeconds(99), effective.FetchTimeout); // inherited default
        Assert.True(effective.StartFromEarliest);                       // inherited default
    }

    [Fact]
    public void Fluent_setters_apply_overrides_to_the_endpoint()
    {
        var transport = new SalesforcePubSubTransport();
        var endpoint = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/B__e");
        var config = new SalesforceListenerConfiguration(endpoint);

        config.FetchCount(25).FetchTimeout(TimeSpan.FromSeconds(30)).StartFromEarliest();
        ((IDelayedEndpointConfiguration)config).Apply(); // run the deferred config actions

        Assert.Equal(25, endpoint.FetchCount);
        Assert.Equal(TimeSpan.FromSeconds(30), endpoint.FetchTimeout);
        Assert.True(endpoint.StartFromEarliest);
    }

    [Fact]
    public void Observability_effective_settings_take_endpoint_overrides_then_transport_defaults()
    {
        var transport = new SalesforcePubSubTransport();
        var endpoint = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/C__e");
        endpoint.HeartbeatInterval = TimeSpan.FromMinutes(2);       // override
        endpoint.StaleStreamLogLevel = LogLevel.Critical;           // override; the other two inherit

        var defaults = new SubscriberComponentsSettings
        {
            HeartbeatInterval = TimeSpan.FromMinutes(15),
            HeartbeatLogLevel = LogLevel.Debug,
            StaleStreamThreshold = TimeSpan.FromMinutes(20),
            StaleStreamLogLevel = LogLevel.Error,
            WatchdogPollingPeriod = TimeSpan.FromSeconds(30)
        };

        var effective = endpoint.ResolveEffectiveSettings(defaults);

        Assert.Equal(TimeSpan.FromMinutes(2), effective.HeartbeatInterval);      // override wins
        Assert.Equal(LogLevel.Debug, effective.HeartbeatLogLevel);               // inherited default
        Assert.Equal(TimeSpan.FromMinutes(20), effective.StaleStreamThreshold);  // inherited default
        Assert.Equal(LogLevel.Critical, effective.StaleStreamLogLevel);          // override wins
        Assert.Equal(TimeSpan.FromSeconds(30), effective.WatchdogPollingPeriod); // defaults-only, carried over
    }

    [Fact]
    public void Observability_fluent_setters_apply_overrides_to_the_endpoint()
    {
        var transport = new SalesforcePubSubTransport();
        var endpoint = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/D__e");
        var config = new SalesforceListenerConfiguration(endpoint);

        config.HeartbeatInterval(TimeSpan.FromMinutes(5), LogLevel.Debug)
            .StaleStreamThreshold(TimeSpan.FromMinutes(10), LogLevel.Critical);
        ((IDelayedEndpointConfiguration)config).Apply();

        Assert.Equal(TimeSpan.FromMinutes(5), endpoint.HeartbeatInterval);
        Assert.Equal(LogLevel.Debug, endpoint.HeartbeatLogLevel);
        Assert.Equal(TimeSpan.FromMinutes(10), endpoint.StaleStreamThreshold);
        Assert.Equal(LogLevel.Critical, endpoint.StaleStreamLogLevel);
    }

    [Fact]
    public void Interval_only_fluent_call_leaves_the_log_level_inherited()
    {
        var transport = new SalesforcePubSubTransport();
        var endpoint = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/E__e");
        var config = new SalesforceListenerConfiguration(endpoint);

        config.HeartbeatInterval(TimeSpan.FromMinutes(5)).StaleStreamThreshold(TimeSpan.FromMinutes(10));
        ((IDelayedEndpointConfiguration)config).Apply();

        Assert.Null(endpoint.HeartbeatLogLevel);   // still inherits the transport-level log level
        Assert.Null(endpoint.StaleStreamLogLevel);
    }

    [Fact]
    public void Disable_methods_zero_the_endpoint_settings()
    {
        var transport = new SalesforcePubSubTransport();
        var endpoint = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/F__e");
        var config = new SalesforceListenerConfiguration(endpoint);

        config.DisableHeartbeat().DisableStaleStreamWatchdog();
        ((IDelayedEndpointConfiguration)config).Apply();

        Assert.Equal(TimeSpan.Zero, endpoint.HeartbeatInterval);
        Assert.Equal(TimeSpan.Zero, endpoint.StaleStreamThreshold);
    }

    [Fact]
    public void Transport_level_fluent_setters_apply_to_the_shared_defaults()
    {
        var settings = new SubscriberComponentsSettings();
        var config = new SalesforcePubSubConfiguration(new SalesforcePubSubTransport(), new WolverineOptions(), settings);

        config.HeartbeatInterval(TimeSpan.FromMinutes(3), LogLevel.Debug)
            .StaleStreamThreshold(TimeSpan.FromMinutes(7), LogLevel.Critical);

        Assert.Equal(TimeSpan.FromMinutes(3), settings.HeartbeatInterval);
        Assert.Equal(LogLevel.Debug, settings.HeartbeatLogLevel);
        Assert.Equal(TimeSpan.FromMinutes(7), settings.StaleStreamThreshold);
        Assert.Equal(LogLevel.Critical, settings.StaleStreamLogLevel);

        config.DisableHeartbeat().DisableStaleStreamWatchdog();

        Assert.Equal(TimeSpan.Zero, settings.HeartbeatInterval);
        Assert.Equal(TimeSpan.Zero, settings.StaleStreamThreshold);
        Assert.Equal(LogLevel.Debug, settings.HeartbeatLogLevel);   // disable leaves levels untouched
        Assert.Equal(LogLevel.Critical, settings.StaleStreamLogLevel);
    }
}
