using Microsoft.Extensions.Logging;
using WolverineFxContrib.SalesforcePubSub.TestHost.Events;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Handlers;

public class TestEventTwoHandler
{
    public void Handle(TestEventTwo message, ILogger<TestEventTwoHandler> logger)
        => logger.LogInformation(
            "Handled TestEventTwo (MES) — ReplayId {ReplayId}, CreatedById {CreatedById}, CreatedDate {CreatedDate}",
            message.ReplayId, message.CreatedById, message.CreatedDate);
}
