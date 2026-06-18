using Microsoft.Extensions.Logging;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

public class TestEventOneHandler
{
    public void Handle(TestEventOne message, ILogger<TestEventOneHandler> logger)
        => logger.LogInformation(
            "Handled TestEventOne (topic) — ReplayId {ReplayId}, CreatedById {CreatedById}, CreatedDate {CreatedDate}",
            message.ReplayId, message.CreatedById, message.CreatedDate);
}
