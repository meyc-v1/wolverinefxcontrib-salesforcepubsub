using Wolverine;
using Wolverine.Configuration;
using Wolverine.SalesforcePubSub;
using Wolverine.SalesforcePubSub.Events;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// The map-only registration model (DECISIONS #16): single-type sugar seals the map, MapEvent declares
/// per-event mappings, cardinality is enforced per endpoint kind (topic = one, channel = named 1..N,
/// MES = 1..N), and the __chn/__e entry points guard each other.
/// </summary>
public class EventMapRegistrationTests
{
    public sealed class EventA : PlatformEvent;
    public sealed class EventB : PlatformEvent;

    private static SalesforceEndpoint EndpointOf(WolverineOptions options, SalesforceResourceKind kind, string resource)
        => options.Transports.GetOrCreate<SalesforcePubSubTransport>().EndpointForResource(kind, resource);

    private static void Apply(SalesforceListenerConfiguration config)
        => ((IDelayedEndpointConfiguration)config).Apply();

    [Fact]
    public void Single_type_sugar_creates_a_sealed_unconditional_entry()
    {
        var options = new WolverineOptions();
        options.ListenToSalesforceTopic<EventA>("/event/A__e");

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/A__e");
        Assert.Equal(typeof(EventA), endpoint.UnconditionalEventType);
        Assert.Equal(typeof(EventA), endpoint.MessageType); // diagnostics parity
        Assert.True(endpoint.EventMapSealed);
        endpoint.ValidateEventMap(); // exactly-one topic entry: valid
    }

    [Fact]
    public void Re_registering_the_same_single_type_is_idempotent()
    {
        // Wolverine's own transports treat repeated endpoint configuration as idempotent; two composition
        // modules touching the same topic with the same type must not crash startup.
        var options = new WolverineOptions();
        options.ListenToSalesforceTopic<EventA>("/event/A__e");
        options.ListenToSalesforceTopic<EventA>("/event/A__e");

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/A__e");
        Assert.Equal(typeof(EventA), endpoint.UnconditionalEventType);
        Assert.True(endpoint.EventMapSealed);
        endpoint.ValidateEventMap();
    }

    [Fact]
    public void Re_registering_a_conflicting_single_type_throws()
    {
        var options = new WolverineOptions();
        options.ListenToSalesforceTopic<EventA>("/event/A__e");

        var ex = Assert.Throws<InvalidOperationException>(() => options.ListenToSalesforceTopic<EventB>("/event/A__e"));
        Assert.Contains("conflicting", ex.Message);
    }

    [Fact]
    public void Duplicate_map_entry_with_the_same_type_is_idempotent()
    {
        var options = new WolverineOptions();
        var config = options.ListenToSalesforceChannel("/event/C__chn").MapEvent<EventA>("A__e").MapEvent<EventA>("A__e");
        Apply(config);

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/C__chn");
        Assert.Single(endpoint.EventTypeMap);
        Assert.Equal(typeof(EventA), endpoint.EventTypeMap["A__e"]);
    }

    [Fact]
    public void Null_message_type_is_rejected_at_registration()
    {
        var options = new WolverineOptions();
        Assert.Throws<ArgumentNullException>(() => options.ListenToSalesforceTopic("/event/A__e", null!));
        Assert.Throws<ArgumentNullException>(() => options.ListenToManagedSubscription("My_Sub", null!));
        Assert.Throws<ArgumentNullException>(() => options.ListenToSalesforceChannel("/event/C__chn").MapEvent(null!, "A__e"));
    }

    [Fact]
    public void MapEvent_after_the_sugar_throws_at_apply_time()
    {
        var options = new WolverineOptions();
        var config = options.ListenToSalesforceTopic<EventA>("/event/A__e").MapEvent<EventB>("B__e");

        var ex = Assert.Throws<InvalidOperationException>(() => Apply(config));
        Assert.Contains("MapEvent", ex.Message);
    }

    [Fact]
    public void Topic_with_two_mapped_events_fails_validation()
    {
        var options = new WolverineOptions();
        var config = options.ListenToSalesforceTopic("/event/A__e").MapEvent<EventA>("A__e").MapEvent<EventB>("B__e");
        Apply(config);

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/A__e");
        var ex = Assert.Throws<InvalidOperationException>(endpoint.ValidateEventMap);
        Assert.Contains("exactly one", ex.Message);
    }

    [Fact]
    public void Channel_maps_multiple_named_events()
    {
        var options = new WolverineOptions();
        var config = options.ListenToSalesforceChannel("/event/C__chn")
            .MapEvent<EventA>("A__e")
            .MapEvent<EventB>("B__e");
        Apply(config);

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/C__chn");
        endpoint.ValidateEventMap();
        Assert.Equal(typeof(EventA), endpoint.EventTypeMap["A__e"]);
        Assert.Equal(typeof(EventB), endpoint.EventTypeMap["B__e"]);
        Assert.Null(endpoint.UnconditionalEventType);
    }

    [Fact]
    public void Channel_rejects_an_unnamed_entry()
    {
        var options = new WolverineOptions();
        var config = options.ListenToSalesforceChannel("/event/C__chn").MapEvent<EventA>();
        Apply(config);

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/C__chn");
        var ex = Assert.Throws<InvalidOperationException>(endpoint.ValidateEventMap);
        Assert.Contains("event API name", ex.Message);
    }

    [Fact]
    public void Mes_allows_multiple_named_events()
    {
        // A MES may target a custom channel server-side, so the map-style MES form allows 1..N.
        var options = new WolverineOptions();
        var config = options.ListenToManagedSubscription("My_Sub").MapEvent<EventA>("A__e").MapEvent<EventB>("B__e");
        Apply(config);

        var endpoint = EndpointOf(options, SalesforceResourceKind.ManagedSubscription, "My_Sub");
        endpoint.ValidateEventMap();
        Assert.Equal(2, endpoint.EventTypeMap.Count);
    }

    [Fact]
    public void Unnamed_entry_mixed_with_named_entries_fails_validation()
    {
        var options = new WolverineOptions();
        var config = options.ListenToManagedSubscription("My_Sub").MapEvent<EventA>().MapEvent<EventB>("B__e");
        Apply(config);

        var endpoint = EndpointOf(options, SalesforceResourceKind.ManagedSubscription, "My_Sub");
        var ex = Assert.Throws<InvalidOperationException>(endpoint.ValidateEventMap);
        Assert.Contains("unnamed", ex.Message);
    }

    [Fact]
    public void Duplicate_event_api_name_throws()
    {
        var options = new WolverineOptions();
        var config = options.ListenToSalesforceChannel("/event/C__chn").MapEvent<EventA>("X__e").MapEvent<EventB>("X__e");

        var ex = Assert.Throws<InvalidOperationException>(() => Apply(config));
        Assert.Contains("already maps", ex.Message);
    }

    [Fact]
    public void Second_unnamed_entry_throws()
    {
        var options = new WolverineOptions();
        var config = options.ListenToManagedSubscription("My_Sub").MapEvent<EventA>().MapEvent<EventB>();

        var ex = Assert.Throws<InvalidOperationException>(() => Apply(config));
        Assert.Contains("unconditional", ex.Message);
    }

    [Fact]
    public void Empty_map_fails_validation()
    {
        var options = new WolverineOptions();
        options.ListenToSalesforceChannel("/event/C__chn"); // no MapEvent

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/C__chn");
        var ex = Assert.Throws<InvalidOperationException>(endpoint.ValidateEventMap);
        Assert.Contains("No event types", ex.Message);
    }

    [Fact]
    public void ListenToSalesforceTopic_rejects_a_channel_resource()
    {
        var options = new WolverineOptions();
        var ex = Assert.Throws<ArgumentException>(() => options.ListenToSalesforceTopic("/event/C__chn"));
        Assert.Contains("ListenToSalesforceChannel", ex.Message);

        ex = Assert.Throws<ArgumentException>(() => options.ListenToSalesforceTopic<EventA>("/event/C__chn"));
        Assert.Contains("ListenToSalesforceChannel", ex.Message);
    }

    [Fact]
    public void ListenToSalesforceChannel_rejects_a_topic_resource()
    {
        var options = new WolverineOptions();
        var ex = Assert.Throws<ArgumentException>(() => options.ListenToSalesforceChannel("/event/A__e"));
        Assert.Contains("ListenToSalesforceTopic", ex.Message);
    }

    [Fact]
    public void Non_pubsub_event_type_is_rejected()
    {
        var options = new WolverineOptions();
        var config = options.ListenToSalesforceChannel("/event/C__chn").MapEvent(typeof(string), "X__e");

        Assert.Throws<ArgumentException>(() => Apply(config));
    }

    [Fact]
    public void Durable_mode_is_supported()
    {
        var options = new WolverineOptions();
        options.ListenToSalesforceChannel("/event/C__chn").MapEvent<EventA>("A__e");

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/C__chn");
        endpoint.Mode = EndpointMode.Durable; // previously rejected by supportsMode
        Assert.Equal(EndpointMode.Durable, endpoint.Mode);
    }

    [Fact]
    public void Prewarm_topics_come_from_the_map_or_the_topic_itself()
    {
        var options = new WolverineOptions();

        var channelConfig = options.ListenToSalesforceChannel("/event/C__chn").MapEvent<EventA>("A__e").MapEvent<EventB>("B__e");
        Apply(channelConfig);
        var channel = EndpointOf(options, SalesforceResourceKind.Topic, "/event/C__chn");
        Assert.Equal(["/event/A__e", "/event/B__e"], channel.BuildPrewarmTopics().Order());

        options.ListenToSalesforceTopic<EventA>("/event/A__e");
        var topic = EndpointOf(options, SalesforceResourceKind.Topic, "/event/A__e");
        Assert.Equal(["/event/A__e"], topic.BuildPrewarmTopics());

        options.ListenToManagedSubscription<EventA>("My_Sub");
        var mes = EndpointOf(options, SalesforceResourceKind.ManagedSubscription, "My_Sub");
        Assert.Empty(mes.BuildPrewarmTopics()); // an unconditional MES has no topic to query
    }
}
