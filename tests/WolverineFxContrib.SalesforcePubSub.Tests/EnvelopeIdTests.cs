using Wolverine.SalesforcePubSub.Internals;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// The deterministic envelope Id mapping (<see cref="SalesforceListener.ResolveEnvelopeId"/>) — a
/// redelivered Salesforce event must always yield the same Id so the durable inbox can dedup it, and all
/// three derivation paths implement the same one-event-one-identity semantic with no per-endpoint salt
/// (DECISIONS #18): the same event delivered via a topic, a channel, and a MES yields the same Id on all
/// of them, regardless of which path produced it.
/// </summary>
public class EnvelopeIdTests
{
    [Fact]
    public void Uses_the_salesforce_event_guid_when_it_parses()
    {
        var eventId = Guid.NewGuid();
        var resolved = SalesforceListener.ResolveEnvelopeId(eventId.ToString(), 5);
        Assert.Equal(eventId, resolved);
    }

    [Fact]
    public void Is_deterministic_and_keyed_on_the_event_id_for_a_non_guid_id()
    {
        var a = SalesforceListener.ResolveEnvelopeId("evt-abc", 5);
        var b = SalesforceListener.ResolveEnvelopeId("evt-abc", 99); // different replay id

        Assert.Equal(a, b);                 // keyed on the event id, not the replay id
        Assert.NotEqual(Guid.Empty, a);
    }

    [Fact]
    public void Distinct_event_ids_yield_distinct_envelope_ids()
    {
        var a = SalesforceListener.ResolveEnvelopeId("evt-abc", 5);
        var b = SalesforceListener.ResolveEnvelopeId("evt-xyz", 5);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Falls_back_to_the_replay_id_when_no_event_id()
    {
        // Replay ids are positions in the org's single shared event bus, so the bare replay id is a valid
        // event identity — the same event carries the same replay id on every endpoint that delivers it.
        var nullId = SalesforceListener.ResolveEnvelopeId(null, 5);
        var emptyId = SalesforceListener.ResolveEnvelopeId("", 5);
        var differentReplay = SalesforceListener.ResolveEnvelopeId(null, 6);

        Assert.Equal(nullId, emptyId);            // null and empty collapse to the same fallback key
        Assert.NotEqual(nullId, differentReplay); // the replay id is the fallback key
    }

    [Fact]
    public void All_paths_yield_one_identity_per_event_across_endpoints()
    {
        // The finding-#1 semantic, pinned: no derivation path salts with the endpoint, so the same event
        // arriving on "/event/A__e" and "/event/C__chn" resolves to the same Id on every path. Consumers
        // wanting per-endpoint identities use Wolverine's native MessageIdentity.IdAndDestination.
        var guid = Guid.NewGuid().ToString();

        Assert.Equal(
            SalesforceListener.ResolveEnvelopeId(guid, 5),
            SalesforceListener.ResolveEnvelopeId(guid, 5));
        Assert.Equal(
            SalesforceListener.ResolveEnvelopeId("evt-abc", 5),
            SalesforceListener.ResolveEnvelopeId("evt-abc", 5));
        Assert.Equal(
            SalesforceListener.ResolveEnvelopeId(null, 5),
            SalesforceListener.ResolveEnvelopeId(null, 5));
    }
}
