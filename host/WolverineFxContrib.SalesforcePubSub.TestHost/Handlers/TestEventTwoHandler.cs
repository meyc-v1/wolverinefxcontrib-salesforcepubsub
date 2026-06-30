using Microsoft.Extensions.Logging;
using WolverineFxContrib.SalesforcePubSub.TestHost.Events;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Handlers;

public class TestEventTwoHandler
{
    public void Handle(TestEventTwo message, RunMetrics metrics, ILogger<TestEventTwoHandler> logger)
    {
        metrics.RecordHandled(nameof(TestEventTwo), message.ReplayId);
        logger.LogInformation(
            "Handled TestEventTwo (topic) — ReplayId {ReplayId}, CreatedById {CreatedById}, CreatedDate {CreatedDate}",
            message.ReplayId, message.CreatedById, message.CreatedDate);
    }
}
