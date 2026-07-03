using Microsoft.Extensions.Logging;
using Wolverine;
using WolverineFxContrib.SalesforcePubSub.TestHost.Events;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Handlers;

public class TestEventOneHandler
{
    /// <summary>
    /// Failure-testing seams, keyed on the published Message__c so a test drives them from the sf CLI:
    /// "poison" throws on every attempt (exercises retry → dead-letter; a real DLQ row under Durable);
    /// "slow" delays ~30s (a kill mid-handle leaves the envelope in the durable inbox for
    /// restart-recovery).
    /// </summary>
    public async Task Handle(TestEventOne message, Envelope envelope, RunMetrics metrics, ILogger<TestEventOneHandler> logger)
    {
        if (string.Equals(message.Message__c, "poison", StringComparison.OrdinalIgnoreCase))
        {
            logger.LogWarning("Poison TestEventOne received (ReplayId {ReplayId}, attempt {Attempt}) — throwing.",
                message.ReplayId, envelope.Attempts);
            throw new InvalidOperationException("Poison message test: handler failure requested via Message__c.");
        }

        if (string.Equals(message.Message__c, "slow", StringComparison.OrdinalIgnoreCase))
        {
            logger.LogInformation("Slow TestEventOne received (ReplayId {ReplayId}) — delaying 30s.", message.ReplayId);
            await Task.Delay(TimeSpan.FromSeconds(30));
        }

        // Recorded only on a successful pass so poison retries don't inflate the run's handled ledger.
        metrics.RecordHandled(nameof(TestEventOne), message.ReplayId);
        logger.LogInformation(
            "Handled TestEventOne from {Source} — ReplayId {ReplayId}, CreatedById {CreatedById}, CreatedDate {CreatedDate}",
            envelope.TopicName, message.ReplayId, message.CreatedById, message.CreatedDate);
    }
}
