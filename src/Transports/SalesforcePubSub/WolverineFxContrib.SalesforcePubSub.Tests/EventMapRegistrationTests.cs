using Wolverine;
using Wolverine.Configuration;
using Wolverine.SalesforcePubSub;
using Wolverine.SalesforcePubSub.Events;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// The multi-type-first registration model (DECISIONS #19): two entry points split on the replay axis,
/// every event declared by API name via MapEvent, a single-event topic's name validated against its
/// path, and idempotent duplicate registration.
/// </summary>
public class EventMapRegistrationTests
{
    public sealed class EventA : PlatformEvent;
    public sealed class EventB : PlatformEvent;

    [SalesforcePlatformEvent("Attributed__e")]
    public sealed class AttributedEvent : PlatformEvent;

    private static SalesforceEndpoint EndpointOf(WolverineOptions options, SalesforceResourceKind kind, string resource)
        => options.Transports.GetOrCreate<SalesforcePubSubTransport>().EndpointForResource(kind, resource);

    private static void Apply(SalesforceListenerConfiguration config)
        => ((IDelayedEndpointConfiguration)config).Apply();

    [Fact]
    public void Entry_points_create_the_right_kinds()
    {
        var options = new WolverineOptions();
        options.ListenToSalesforceTopic("/event/A__e").MapEvent<EventA>("A__e");
        options.ListenToManagedSubscription("My_Sub").MapEvent<EventA>("A__e");

        Assert.Equal(SalesforceResourceKind.Topic, EndpointOf(options, SalesforceResourceKind.Topic, "/event/A__e").Kind);
        Assert.Equal(SalesforceResourceKind.ManagedSubscription, EndpointOf(options, SalesforceResourceKind.ManagedSubscription, "My_Sub").Kind);
    }

    [Fact]
    public void MapEvent_requires_the_event_api_name()
    {
        var options = new WolverineOptions();
        var config = options.ListenToSalesforceTopic("/event/A__e");

        Assert.ThrowsAny<ArgumentException>(() => config.MapEvent<EventA>(null!));
        Assert.ThrowsAny<ArgumentException>(() => config.MapEvent<EventA>(""));
        Assert.ThrowsAny<ArgumentException>(() => config.MapEvent<EventA>("   "));
        Assert.Throws<ArgumentNullException>(() => config.MapEvent(null!, "A__e"));
    }

    [Fact]
    public void Single_event_topic_with_the_matching_name_is_valid()
    {
        var options = new WolverineOptions();
        Apply(options.ListenToSalesforceTopic("/event/A__e").MapEvent<EventA>("A__e"));

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/A__e");
        endpoint.ValidateEventMap();
        Assert.Equal(typeof(EventA), endpoint.EventTypeMap["A__e"]);

        // MessageType must stay null even for a single-entry map: Wolverine turns a non-null value into
        // an incoming MessageTypeRule that overwrites every envelope's MessageType, force-decoding
        // unmapped events into the mapped type (found live by the integration suite).
        Assert.Null(endpoint.MessageType);
    }

    [Fact]
    public void Single_event_topic_name_must_match_the_path()
    {
        // "/event/A__e" delivers A__e; mapping B__e would dead-letter every event at runtime — fail fast.
        var options = new WolverineOptions();
        Apply(options.ListenToSalesforceTopic("/event/A__e").MapEvent<EventB>("B__e"));

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/A__e");
        var ex = Assert.Throws<InvalidOperationException>(endpoint.ValidateEventMap);
        Assert.Contains("dead-letter", ex.Message);
    }

    [Fact]
    public void Single_event_topic_rejects_multiple_mappings()
    {
        var options = new WolverineOptions();
        Apply(options.ListenToSalesforceTopic("/event/A__e").MapEvent<EventA>("A__e").MapEvent<EventB>("B__e"));

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/A__e");
        var ex = Assert.Throws<InvalidOperationException>(endpoint.ValidateEventMap);
        Assert.Contains("exactly one", ex.Message);
    }

    [Fact]
    public void Channel_maps_multiple_named_events()
    {
        var options = new WolverineOptions();
        Apply(options.ListenToSalesforceTopic("/event/C__chn").MapEvent<EventA>("A__e").MapEvent<EventB>("B__e"));

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/C__chn");
        endpoint.ValidateEventMap();
        Assert.Equal(typeof(EventA), endpoint.EventTypeMap["A__e"]);
        Assert.Equal(typeof(EventB), endpoint.EventTypeMap["B__e"]);
        Assert.Null(endpoint.MessageType); // never set — see the single-entry test
    }

    [Fact]
    public void Mes_allows_one_or_many_events()
    {
        var options = new WolverineOptions();
        Apply(options.ListenToManagedSubscription("My_Sub").MapEvent<EventA>("A__e").MapEvent<EventB>("B__e"));

        var endpoint = EndpointOf(options, SalesforceResourceKind.ManagedSubscription, "My_Sub");
        endpoint.ValidateEventMap();
        Assert.Equal(2, endpoint.EventTypeMap.Count);
    }

    [Fact]
    public void Duplicate_identical_mapping_is_idempotent()
    {
        // Two composition modules touching the same subscription with the same mapping must not crash.
        var options = new WolverineOptions();
        Apply(options.ListenToSalesforceTopic("/event/A__e").MapEvent<EventA>("A__e"));
        Apply(options.ListenToSalesforceTopic("/event/A__e").MapEvent<EventA>("A__e"));

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/A__e");
        Assert.Single(endpoint.EventTypeMap);
        endpoint.ValidateEventMap();
    }

    [Fact]
    public void Conflicting_type_for_the_same_name_throws()
    {
        var options = new WolverineOptions();
        var config = options.ListenToSalesforceTopic("/event/C__chn").MapEvent<EventA>("X__e").MapEvent<EventB>("X__e");

        var ex = Assert.Throws<InvalidOperationException>(() => Apply(config));
        Assert.Contains("already maps", ex.Message);
    }

    [Fact]
    public void Empty_map_fails_validation()
    {
        var options = new WolverineOptions();
        options.ListenToSalesforceTopic("/event/C__chn"); // no MapEvent

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/C__chn");
        var ex = Assert.Throws<InvalidOperationException>(endpoint.ValidateEventMap);
        Assert.Contains("No event types", ex.Message);
    }

    [Fact]
    public void Attribute_supplies_the_event_api_name()
    {
        var options = new WolverineOptions();
        Apply(options.ListenToSalesforceTopic("/event/Attributed__e").MapEvent<AttributedEvent>());

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/Attributed__e");
        endpoint.ValidateEventMap();
        Assert.Equal(typeof(AttributedEvent), endpoint.EventTypeMap["Attributed__e"]);
    }

    [Fact]
    public void Attribute_less_type_with_no_explicit_name_throws()
    {
        var options = new WolverineOptions();
        var config = options.ListenToSalesforceTopic("/event/A__e");

        var ex = Assert.Throws<InvalidOperationException>(() => config.MapEvent<EventA>());
        Assert.Contains(nameof(SalesforcePlatformEventAttribute), ex.Message);
    }

    [Fact]
    public void Explicit_name_wins_over_the_attribute()
    {
        var options = new WolverineOptions();
        Apply(options.ListenToSalesforceTopic("/event/C__chn").MapEvent<AttributedEvent>("Override__e"));

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/C__chn");
        Assert.Equal(typeof(AttributedEvent), endpoint.EventTypeMap["Override__e"]);
        Assert.False(endpoint.EventTypeMap.ContainsKey("Attributed__e"));
    }

    [Fact]
    public void Non_pubsub_event_type_is_rejected()
    {
        var options = new WolverineOptions();
        var config = options.ListenToSalesforceTopic("/event/C__chn").MapEvent(typeof(string), "X__e");

        Assert.Throws<ArgumentException>(() => Apply(config));
    }

    [Fact]
    public void Durable_mode_is_supported()
    {
        var options = new WolverineOptions();
        options.ListenToSalesforceTopic("/event/C__chn").MapEvent<EventA>("A__e");

        var endpoint = EndpointOf(options, SalesforceResourceKind.Topic, "/event/C__chn");
        endpoint.Mode = EndpointMode.Durable;
        Assert.Equal(EndpointMode.Durable, endpoint.Mode);
    }

    [Fact]
    public void Every_endpoint_prewarms_from_its_map_including_mes()
    {
        var options = new WolverineOptions();

        Apply(options.ListenToSalesforceTopic("/event/C__chn").MapEvent<EventA>("A__e").MapEvent<EventB>("B__e"));
        Assert.Equal(["/event/A__e", "/event/B__e"],
            EndpointOf(options, SalesforceResourceKind.Topic, "/event/C__chn").BuildPrewarmTopics().Order());

        Apply(options.ListenToManagedSubscription("My_Sub").MapEvent<EventA>("A__e"));
        Assert.Equal(["/event/A__e"],
            EndpointOf(options, SalesforceResourceKind.ManagedSubscription, "My_Sub").BuildPrewarmTopics());
    }
}
