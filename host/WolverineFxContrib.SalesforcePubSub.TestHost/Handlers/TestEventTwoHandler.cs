using Microsoft.Extensions.Logging;
using Wolverine;
using WolverineFxContrib.SalesforcePubSub.TestHost.Events;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Handlers;

public class TestEventTwoHandler
{
    public void Handle(TestEventTwo message, Envelope envelope, RunMetrics metrics, ILogger<TestEventTwoHandler> logger)
    {
        metrics.RecordHandled(nameof(TestEventTwo), message.ReplayId);
        logger.LogInformation(
            "Handled TestEventTwo from {Source} — ReplayId {ReplayId}, CreatedById {CreatedById}, CreatedDate {CreatedDate}",
            envelope.TopicName, message.ReplayId, message.CreatedById, message.CreatedDate);
    }
}
