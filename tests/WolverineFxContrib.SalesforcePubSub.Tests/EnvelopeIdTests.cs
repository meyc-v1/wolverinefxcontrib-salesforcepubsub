using Wolverine.SalesforcePubSub.Internals;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// The deterministic envelope Id mapping (<see cref="SalesforceListener.ResolveEnvelopeId"/>) — a
/// redelivered Salesforce event must always yield the same Id so the durable inbox can dedup it.
/// </summary>
public class EnvelopeIdTests
{
    [Fact]
    public void Uses_the_salesforce_event_guid_when_it_parses()
    {
        var eventId = Guid.NewGuid();
        var resolved = SalesforceListener.ResolveEnvelopeId(eventId.ToString(), "/event/A__e", 5);
        Assert.Equal(eventId, resolved);
    }

    [Fact]
    public void Is_deterministic_and_keyed_on_the_event_id_for_a_non_guid_id()
    {
        var a = SalesforceListener.ResolveEnvelopeId("evt-abc", "/event/A__e", 5);
        var b = SalesforceListener.ResolveEnvelopeId("evt-abc", "/event/A__e", 99); // different replay id

        Assert.Equal(a, b);                 // keyed on the event id, not the replay id
        Assert.NotEqual(Guid.Empty, a);
    }

    [Fact]
    public void Distinct_event_ids_yield_distinct_envelope_ids()
    {
        var a = SalesforceListener.ResolveEnvelopeId("evt-abc", "/event/A__e", 5);
        var b = SalesforceListener.ResolveEnvelopeId("evt-xyz", "/event/A__e", 5);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Falls_back_to_resource_and_replay_when_no_event_id()
    {
        var nullId = SalesforceListener.ResolveEnvelopeId(null, "/event/A__e", 5);
        var emptyId = SalesforceListener.ResolveEnvelopeId("", "/event/A__e", 5);
        var differentReplay = SalesforceListener.ResolveEnvelopeId(null, "/event/A__e", 6);

        Assert.Equal(nullId, emptyId);           // null and empty collapse to the same fallback key
        Assert.NotEqual(nullId, differentReplay); // replay id participates in the fallback key
    }
}
