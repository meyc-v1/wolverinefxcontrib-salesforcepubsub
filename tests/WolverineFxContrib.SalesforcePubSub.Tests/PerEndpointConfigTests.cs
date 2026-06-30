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
}
