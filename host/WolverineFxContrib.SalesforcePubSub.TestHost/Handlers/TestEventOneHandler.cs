using Microsoft.Extensions.Logging;
using WolverineFxContrib.SalesforcePubSub.TestHost.Events;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Handlers;

public class TestEventOneHandler
{
    public void Handle(TestEventOne message, RunMetrics metrics, ILogger<TestEventOneHandler> logger)
    {
        metrics.RecordHandled(nameof(TestEventOne), message.ReplayId);
        logger.LogInformation(
            "Handled TestEventOne (MES) — ReplayId {ReplayId}, CreatedById {CreatedById}, CreatedDate {CreatedDate}",
            message.ReplayId, message.CreatedById, message.CreatedDate);
    }
}
