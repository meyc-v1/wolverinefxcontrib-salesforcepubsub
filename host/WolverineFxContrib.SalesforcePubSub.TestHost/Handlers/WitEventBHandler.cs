using Microsoft.Extensions.Logging;
using Wolverine;
using WolverineFxContrib.SalesforcePubSub.TestHost.Events;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Handlers;

public class WitEventBHandler
{
    public void Handle(WitEventB message, Envelope envelope, RunMetrics metrics, ILogger<WitEventBHandler> logger)
    {
        metrics.RecordHandled(nameof(WitEventB), message.ReplayId);
        logger.LogInformation(
            "Handled WitEventB from {Source} — ReplayId {ReplayId}, CreatedById {CreatedById}, CreatedDate {CreatedDate}",
            envelope.TopicName, message.ReplayId, message.CreatedById, message.CreatedDate);
    }
}
