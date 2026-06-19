using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// Covers the dependency-free transport wiring: URI keying and the endpoint cache in
/// <see cref="SalesforcePubSubTransport"/> / <see cref="SalesforceEndpoint"/>.
/// </summary>
public class EndpointResolutionTests
{
    [Fact]
    public void BuildUri_uses_the_sfpubsub_scheme()
    {
        var uri = SalesforceEndpoint.BuildUri(SalesforceResourceKind.Topic, "CM_Test");
        Assert.Equal(SalesforcePubSubTransport.ProtocolName, uri.Scheme);
    }

    [Fact]
    public void BuildUri_distinguishes_topic_from_managed_subscription()
    {
        var topic = SalesforceEndpoint.BuildUri(SalesforceResourceKind.Topic, "Same");
        var mes = SalesforceEndpoint.BuildUri(SalesforceResourceKind.ManagedSubscription, "Same");
        Assert.NotEqual(topic, mes);
    }

    [Fact]
    public void BuildUri_is_stable_for_the_same_inputs()
    {
        var a = SalesforceEndpoint.BuildUri(SalesforceResourceKind.Topic, "/event/CM_Test_Event_Two__e");
        var b = SalesforceEndpoint.BuildUri(SalesforceResourceKind.Topic, "/event/CM_Test_Event_Two__e");
        Assert.Equal(a, b);
    }

    [Fact]
    public void EndpointForResource_returns_the_same_instance_for_the_same_resource()
    {
        var transport = new SalesforcePubSubTransport();
        var first = transport.EndpointForResource(SalesforceResourceKind.ManagedSubscription, "CM_Test_Event_One");
        var second = transport.EndpointForResource(SalesforceResourceKind.ManagedSubscription, "CM_Test_Event_One");
        Assert.Same(first, second);
    }

    [Fact]
    public void EndpointForResource_creates_distinct_endpoints_per_resource()
    {
        var transport = new SalesforcePubSubTransport();
        var one = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/A__e");
        var two = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/B__e");
        Assert.NotSame(one, two);
    }

    [Fact]
    public void EndpointForResource_treats_topic_and_mes_with_the_same_name_as_distinct()
    {
        var transport = new SalesforcePubSubTransport();
        var topic = transport.EndpointForResource(SalesforceResourceKind.Topic, "Same");
        var mes = transport.EndpointForResource(SalesforceResourceKind.ManagedSubscription, "Same");
        Assert.NotSame(topic, mes);
    }

    [Fact]
    public void New_endpoint_defaults_to_inline_and_names_itself_after_the_resource()
    {
        var transport = new SalesforcePubSubTransport();
        var endpoint = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/CM_Test_Event_Two__e");

        Assert.Equal(SalesforceResourceKind.Topic, endpoint.Kind);
        Assert.Equal("/event/CM_Test_Event_Two__e", endpoint.Resource);
        Assert.Equal("/event/CM_Test_Event_Two__e", endpoint.EndpointName);
        Assert.Equal("Inline", endpoint.Mode.ToString());
        Assert.Equal(SalesforceEndpoint.BuildUri(SalesforceResourceKind.Topic, "/event/CM_Test_Event_Two__e"), endpoint.Uri);
    }

    [Fact]
    public void BuildListenerAsync_throws_when_no_message_type_is_configured()
    {
        var transport = new SalesforcePubSubTransport();
        var endpoint = transport.EndpointForResource(SalesforceResourceKind.Topic, "/event/A__e");

        // The MessageType guard runs synchronously before the runtime is ever touched.
        Assert.Throws<InvalidOperationException>(() => { _ = endpoint.BuildListenerAsync(null!, null!); });
    }
}
