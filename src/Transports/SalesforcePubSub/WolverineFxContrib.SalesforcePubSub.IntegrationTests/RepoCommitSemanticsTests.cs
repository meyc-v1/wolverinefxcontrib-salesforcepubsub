using Microsoft.Extensions.DependencyInjection;
using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// Kafka-suite equivalent: kafka_offset_committer, taken live. At low volume the completion throttle is
/// never reached, so the pending handled position is flushed by the next idle keep-alive — and that
/// commit must be events-handled (it covers a handled event), not keep-alive. This is the
/// regression pin, in its natural low-volume shape, for the bug where a 19h overnight never populated
/// the repository's last-event diagnostics because every commit was flagged by its trigger.
/// </summary>
public class RepoCommitSemanticsTests(SalesforceTestContext ctx)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(60);

    /// <summary>Keep-alives arrive roughly every two minutes on an idle stream.</summary>
    private static readonly TimeSpan KeepAliveFlushTimeout = TimeSpan.FromMinutes(3.5);

    [Fact]
    public async Task A_keepalive_flush_of_a_handled_event_reports_events_received()
    {
        var repo = new RecordingReplayIdRepository();
        var sink = new EventSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Discovery.IncludeType<WitEventAHandler>();
            opts.ListenToSalesforceTopic("/event/WIT_Event_A__e").MapEvent<WitEventA>();
        }, s => s.AddSingleton<IReplayIdRepository>(repo));

        try
        {
            var correlation = $"repo-semantics-{Guid.NewGuid():N}";
            await ctx.PublishAsync("WIT_Event_A__e", correlation, TestContext.Current.CancellationToken);

            var received = await sink.WaitForAsync(e => e.Message == correlation, count: 1, ReceiveTimeout);
            var handledReplayId = Assert.Single(received).ReplayId;

            // One handled event is far below the commit throttle, so nothing commits until the next
            // idle keep-alive flushes the pending position — which must cover our event and commit
            // as events-handled.
            var deadline = DateTime.UtcNow + KeepAliveFlushTimeout;
            while (DateTime.UtcNow < deadline)
            {
                if (repo.Commits.Any(c => c.Kind == ReplayCommitKind.EventsHandled && c.ReplayId >= handledReplayId))
                    return;

                await Task.Delay(500, TestContext.Current.CancellationToken);
            }

            var seen = string.Join("; ", repo.Commits.Select(c => $"{c.Kind}@{c.ReplayId}"));
            Assert.Fail($"No events-handled commit covering replay {handledReplayId} within {KeepAliveFlushTimeout}. Commits seen: {seen}");
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }
}
