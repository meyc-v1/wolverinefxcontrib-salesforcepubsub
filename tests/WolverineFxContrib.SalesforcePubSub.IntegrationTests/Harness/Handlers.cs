using Wolverine;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

// Recording handlers, included per test via opts.Discovery.IncludeType<>() so each host wires exactly
// the handlers its scenario needs (conventional discovery is disabled in the host factory).

public class WitEventAHandler
{
    public static void Handle(WitEventA evt, Envelope envelope, EventSink sink)
        => sink.Record(new ReceivedEvent(
            nameof(WitEventA), evt.Message__c, evt.ReplayId, envelope.Id, envelope.TopicName, envelope.SentAt));
}

public class WitEventBHandler
{
    public static void Handle(WitEventB evt, Envelope envelope, EventSink sink)
        => sink.Record(new ReceivedEvent(
            nameof(WitEventB), evt.Message__c, evt.ReplayId, envelope.Id, envelope.TopicName, envelope.SentAt));
}
