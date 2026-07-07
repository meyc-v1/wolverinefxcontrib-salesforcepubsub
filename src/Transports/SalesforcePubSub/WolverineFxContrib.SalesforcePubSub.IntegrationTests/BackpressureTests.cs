using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Wolverine;
using Wolverine.SalesforcePubSub;
using Wolverine.Transports;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// Compliance-suite equivalent: can_stop_receiving_when_too_busy_and_restart_listeners — the one test
/// where Wolverine manages our listener rather than the listener managing itself. A Buffered endpoint
/// with tiny buffering limits and a slow handler overflows under a burst; the ListeningAgent stops AND
/// DISPOSES the listener (no drain), the queue works off, and the agent rebuilds a fresh listener via
/// BuildListenerAsync. A second wave published after the trip can only arrive through the rebuilt
/// listener.
///
/// The replay-commit monotonicity across the rebuild is a HARD assertion: the once-open DECISIONS gap
/// ("stale-commit regression") is closed — a disposed listener loses commit authority, and the tracker's
/// write gate drops stale positions — with the deterministic reproduction pinned in the unit suite
/// (SalesforceListenerTests). This live fact confirms the guarantee holds under a real stop→rebuild.
/// </summary>
public class BackpressureTests(SalesforceTestContext ctx, ITestOutputHelper output)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(150);

    [Fact]
    public async Task Backpressure_stops_the_listener_and_a_rebuilt_one_resumes_delivery()
    {
        var repo = new RecordingReplayIdRepository();
        var sink = new EventSink();
        var logs = new LogSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
            {
                opts.Discovery.IncludeType<SlowEventBHandler>();
                opts.ListenToSalesforceTopic("/event/WIT_Event_B__e")
                    .MapEvent<WitEventB>()
                    .BufferedInMemory(new BufferingLimits(4, 2));
            },
            s => s.AddSingleton<IReplayIdRepository>(repo),
            logs,
            readyEventName: "WIT_Event_B__e"); // this host listens only to B — an A sentinel would never arrive

        try
        {
            // Wave 1: overflow the 4-envelope buffer through a 2s-per-event handler.
            var wave1 = Enumerable.Range(0, 12).Select(i => $"bp-w1-{i}-{Guid.NewGuid():N}").ToList();
            foreach (var correlation in wave1)
                await ctx.PublishAsync("WIT_Event_B__e", correlation, TestContext.Current.CancellationToken);

            // Backpressure must actually trip — otherwise this test is exercising nothing.
            var tripped = await logs.WaitForAsync(l => l.Contains("too busy", StringComparison.OrdinalIgnoreCase), TimeSpan.FromSeconds(90));
            Assert.True(tripped, "the listener was never marked too busy — buffering limits not exceeded?");

            // Wave 2: published after the stop+dispose; only a REBUILT listener can deliver these.
            var wave2 = Enumerable.Range(0, 3).Select(i => $"bp-w2-{i}-{Guid.NewGuid():N}").ToList();
            foreach (var correlation in wave2)
                await ctx.PublishAsync("WIT_Event_B__e", correlation, TestContext.Current.CancellationToken);

            var all = wave1.Concat(wave2).ToHashSet();
            await sink.WaitForAsync(e => e.Message is { } m && all.Contains(m), count: all.Count, ReceiveTimeout);

            // At-least-once across the stop/rebuild: every published event handled (dups tolerated —
            // Buffered redelivers the received-but-uncommitted tail after a rebuild by design).
            var handled = sink.Snapshot().Select(e => e.Message).Where(m => m is not null).ToHashSet();
            Assert.True(all.IsSubsetOf(handled!), "not every published event was handled across the rebuild");

            // The rebuild happened: the agent started listening on the endpoint at least twice.
            Assert.True(logs.Count(l => l.Contains("Started message listening at sfpubsub", StringComparison.OrdinalIgnoreCase)) >= 2,
                "no listener restart observed after the too-busy stop");

            AssertCommitMonotonicity(repo);
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }

    /// <summary>
    /// Hard assertion (see class docs): no committed replay position may regress below an earlier one
    /// across the stop→rebuild — the live confirmation of the guard pinned deterministically in
    /// SalesforceListenerTests.
    /// </summary>
    private void AssertCommitMonotonicity(RecordingReplayIdRepository repo)
    {
        var commits = repo.Commits;
        var violations = new List<string>();
        long highWater = long.MinValue;

        foreach (var commit in commits)
        {
            if (commit.ReplayId < highWater)
                violations.Add($"{commit.Kind}@{commit.ReplayId} after high-water {highWater}");
            highWater = Math.Max(highWater, commit.ReplayId);
        }

        foreach (var commit in commits)
            output.WriteLine($"  {commit.At:HH:mm:ss.fff} {commit.Kind} @ {commit.ReplayId}");

        Assert.True(violations.Count == 0,
            $"replay commits regressed across the stop→rebuild: {string.Join("; ", violations)}");
    }
}

/// <summary>Two seconds per bp-prefixed event — slow enough for a small buffer to overflow under a burst.</summary>
public class SlowEventBHandler
{
    public static async Task Handle(WitEventB evt, Envelope envelope, EventSink sink)
    {
        if (evt.Message__c is { } m && m.StartsWith("bp-"))
            await Task.Delay(TimeSpan.FromSeconds(2));

        sink.Record(new ReceivedEvent(
            nameof(WitEventB), evt.Message__c, evt.ReplayId, envelope.Id, envelope.TopicName, envelope.SentAt));
    }
}
