using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Wolverine;
using Wolverine.Configuration;
using Wolverine.SalesforcePubSub;
using Wolverine.SalesforcePubSub.Internals;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// Per-endpoint config (Phase 4): the fluent setters set endpoint overrides, and effective settings take
/// the per-endpoint override when present, otherwise the transport-level default. The observability knobs
/// are grouped under Heartbeat/Watchdog sub-expressions (DECISIONS #19); the transport expression also
/// registers consumer implementations (replay repo, backoff).
/// </summary>
public class PerEndpointConfigTests
{
    private sealed class FakeReplayRepo : IReplayIdRepository
    {
        public Task<long> GetLastReplayIdAsync(string topicName, CancellationToken cancellationToken = default) => Task.FromResult(-1L);
        public Task ReportEventsReceivedResponseAsync(string topicName, long lastReplayId, List<long> replayIds, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task ReportKeepAliveResponseAsync(string topicName, long lastReplayId, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task ResetForNewEventsOnlyAsync(string topicName, CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private sealed class FakeBackoff : IBackoffStrategy
    {
        public Task BackoffAsync(long consecutiveFailuresWithoutResponse, TimeSpan durationSinceLastSuccessfulResponseOrStart, string resource, CancellationToken token) => Task.CompletedTask;
    }

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
        endpoint.WatchdogLogLevel = LogLevel.Critical;              // override; the other two inherit

        var defaults = new SubscriberComponentsSettings
        {
            HeartbeatInterval = TimeSpan.FromMinutes(15),
            HeartbeatLogLevel = LogLevel.Debug,
            WatchdogThreshold = TimeSpan.FromMinutes(20),
            WatchdogLogLevel = LogLevel.Error,
            WatchdogPollingPeriod = TimeSpan.FromSeconds(30)
        };

        var effective = endpoint.ResolveEffectiveSettings(defaults);

        Assert.Equal(TimeSpan.FromMinutes(2), effective.HeartbeatInterval);      // override wins
        Assert.Equal(LogLevel.Debug, effective.HeartbeatLogLevel);               // inherited default
        Assert.Equal(TimeSpan.FromMinutes(20), effective.WatchdogThreshold);     // inherited default
        Assert.Equal(LogLevel.Critical, effective.WatchdogLogLevel);             // override wins
        Assert.Equal(TimeSpan.FromSeconds(30), effective.WatchdogPollingPeriod); // defaults-only, carried over
    }

    [Fact]
    public void Grouped_observability_expressions_apply_endpoint_overrides()
    {
        var transport = new SalesforcePubSubTransport();
        var endpoint = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/D__e");
        var config = new SalesforceListenerConfiguration(endpoint);

        config.Heartbeat.Interval(TimeSpan.FromMinutes(5)).Heartbeat.Level(LogLevel.Debug)
            .Watchdog.Threshold(TimeSpan.FromMinutes(10)).Watchdog.Level(LogLevel.Critical);
        ((IDelayedEndpointConfiguration)config).Apply();

        Assert.Equal(TimeSpan.FromMinutes(5), endpoint.HeartbeatInterval);
        Assert.Equal(LogLevel.Debug, endpoint.HeartbeatLogLevel);
        Assert.Equal(TimeSpan.FromMinutes(10), endpoint.WatchdogThreshold);
        Assert.Equal(LogLevel.Critical, endpoint.WatchdogLogLevel);
    }

    [Fact]
    public void Interval_only_override_leaves_the_log_level_inherited()
    {
        var transport = new SalesforcePubSubTransport();
        var endpoint = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/E__e");
        var config = new SalesforceListenerConfiguration(endpoint);

        config.Heartbeat.Interval(TimeSpan.FromMinutes(5)).Watchdog.Threshold(TimeSpan.FromMinutes(10));
        ((IDelayedEndpointConfiguration)config).Apply();

        Assert.Null(endpoint.HeartbeatLogLevel);   // still inherits the transport-level log level
        Assert.Null(endpoint.WatchdogLogLevel);
    }

    [Fact]
    public void Disable_zeroes_the_endpoint_settings()
    {
        var transport = new SalesforcePubSubTransport();
        var endpoint = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/F__e");
        var config = new SalesforceListenerConfiguration(endpoint);

        config.Heartbeat.Disable().Watchdog.Disable();
        ((IDelayedEndpointConfiguration)config).Apply();

        Assert.Equal(TimeSpan.Zero, endpoint.HeartbeatInterval);
        Assert.Equal(TimeSpan.Zero, endpoint.WatchdogThreshold);
    }

    [Fact]
    public void Transport_level_grouped_expressions_apply_to_the_shared_defaults()
    {
        var settings = new SubscriberComponentsSettings();
        var expression = new SalesforcePubSubTransportExpression(new SalesforcePubSubTransport(), new WolverineOptions(), settings);

        expression.Heartbeat.Interval(TimeSpan.FromMinutes(3)).Heartbeat.Level(LogLevel.Debug)
            .Watchdog.Threshold(TimeSpan.FromMinutes(7)).Watchdog.Level(LogLevel.Critical)
            .Watchdog.PollingInterval(TimeSpan.FromSeconds(20));

        Assert.Equal(TimeSpan.FromMinutes(3), settings.HeartbeatInterval);
        Assert.Equal(LogLevel.Debug, settings.HeartbeatLogLevel);
        Assert.Equal(TimeSpan.FromMinutes(7), settings.WatchdogThreshold);
        Assert.Equal(LogLevel.Critical, settings.WatchdogLogLevel);
        Assert.Equal(TimeSpan.FromSeconds(20), settings.WatchdogPollingPeriod);

        expression.Heartbeat.Disable().Watchdog.Disable();

        Assert.Equal(TimeSpan.Zero, settings.HeartbeatInterval);
        Assert.Equal(TimeSpan.Zero, settings.WatchdogThreshold);
        Assert.Equal(LogLevel.Debug, settings.HeartbeatLogLevel);   // disable leaves levels untouched
        Assert.Equal(LogLevel.Critical, settings.WatchdogLogLevel);
    }

    [Fact]
    public void Transport_expression_registers_consumer_implementations()
    {
        var options = new WolverineOptions();
        var expression = options.UseSalesforcePubSub(); // registers the TryAdd defaults

        expression.UseReplayIdRepository<FakeReplayRepo>().UseBackoffStrategy<FakeBackoff>();

        var replay = options.Services.Single(d => d.ServiceType == typeof(IReplayIdRepository));
        var backoff = options.Services.Single(d => d.ServiceType == typeof(IBackoffStrategy));
        Assert.Equal(typeof(FakeReplayRepo), replay.ImplementationType); // Replace swapped out the default
        Assert.Equal(typeof(FakeBackoff), backoff.ImplementationType);
    }
}
