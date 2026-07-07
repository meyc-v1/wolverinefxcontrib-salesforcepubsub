using Wolverine.SalesforcePubSub.Internals.Replay;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// The replay watermark: the committed position must never advance past an in-flight (or deferred) event,
/// commits are throttled, keep-alives advance during idle, and the position is monotonic.
/// </summary>
public class ReplayCommitTrackerTests
{
    private sealed class Recorder
    {
        public List<(long ReplayId, bool KeepAlive)> Commits { get; } = [];
        public Func<long, bool, Task> Commit => (id, ka) => { Commits.Add((id, ka)); return Task.CompletedTask; };
        public long? Last => Commits.Count == 0 ? null : Commits[^1].ReplayId;
    }

    [Fact]
    public async Task In_order_completes_advance_the_position_monotonically()
    {
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 1);

        t.Track(1); t.Track(2); t.Track(3);
        await t.CompleteAsync(1);
        await t.WaitForWriterAsync();
        await t.CompleteAsync(2);
        await t.WaitForWriterAsync();
        await t.CompleteAsync(3);
        await t.WaitForWriterAsync();

        Assert.Equal(3, rec.Last);
        var ids = rec.Commits.Select(c => c.ReplayId).ToList();
        Assert.Equal(ids.OrderBy(x => x), ids); // never goes backwards
    }

    [Fact]
    public async Task Does_not_commit_past_an_in_flight_event()
    {
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 1);

        t.Track(1); t.Track(2);
        await t.CompleteAsync(2);   // 1 still in flight
        await t.WaitForWriterAsync();

        Assert.DoesNotContain(rec.Commits, c => c.ReplayId >= 1);

        await t.CompleteAsync(1);   // now everything through 2 is done
        await t.WaitForWriterAsync();
        Assert.Equal(2, rec.Last);
    }

    [Fact]
    public async Task A_deferred_event_holds_the_floor()
    {
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 1);

        t.Track(10); t.Track(11); t.Track(12);
        // 10 is "deferred" — never completed; 11 and 12 do
        await t.CompleteAsync(11);
        await t.WaitForWriterAsync();
        await t.CompleteAsync(12);
        await t.WaitForWriterAsync();

        Assert.DoesNotContain(rec.Commits, c => c.ReplayId >= 10);
    }

    [Fact]
    public async Task Throttle_commits_once_per_threshold()
    {
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 3);

        t.Track(1); t.Track(2); t.Track(3);
        await t.CompleteAsync(1);
        await t.WaitForWriterAsync();
        await t.CompleteAsync(2);
        await t.WaitForWriterAsync();
        await t.CompleteAsync(3);
        await t.WaitForWriterAsync();

        Assert.Single(rec.Commits);
        Assert.Equal(3, rec.Last);
    }

    [Fact]
    public async Task Flush_writes_the_pending_throttled_position()
    {
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 100);

        t.Track(1);
        await t.CompleteAsync(1);
        await t.WaitForWriterAsync();
        Assert.Empty(rec.Commits);   // below threshold

        await t.FlushAsync();
        Assert.Equal(1, rec.Last);
    }

    [Fact]
    public async Task Keepalive_advances_when_nothing_is_in_flight()
    {
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 100);

        await t.ObserveKeepAliveAsync(50);
        await t.WaitForWriterAsync();

        Assert.Equal(50, rec.Last);
        Assert.True(rec.Commits[^1].KeepAlive);
    }

    [Fact]
    public async Task Keepalive_does_not_jump_past_in_flight()
    {
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 1);

        t.Track(5);
        await t.ObserveKeepAliveAsync(50); // 5 in flight → can't commit past 4
        await t.WaitForWriterAsync();

        Assert.DoesNotContain(rec.Commits, c => c.ReplayId >= 5);
    }

    [Fact]
    public async Task Never_commits_backwards()
    {
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 1);

        t.Track(10);
        await t.CompleteAsync(10);
        await t.WaitForWriterAsync();
        await t.ObserveKeepAliveAsync(5); // stale/lower position
        await t.WaitForWriterAsync();

        Assert.Equal(10, rec.Last);
        Assert.DoesNotContain(rec.Commits, c => c.ReplayId < 10);
    }

    [Fact]
    public async Task Mes_reaffirms_committed_position_on_each_idle_keepalive()
    {
        // MES must send a CommitReplayRequest within Salesforce's 1800s deadline or the subscription is
        // torn down. During idle the position doesn't advance, so re-affirm it on every keep-alive.
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 1, commitKeepAliveWhenIdle: true);

        await t.ObserveKeepAliveAsync(50); // establishes + commits 50
        await t.WaitForWriterAsync();
        await t.ObserveKeepAliveAsync(50); // idle, unchanged — re-affirm to refresh the server deadline
        await t.WaitForWriterAsync();
        await t.ObserveKeepAliveAsync(50);
        await t.WaitForWriterAsync();

        Assert.Equal(3, rec.Commits.Count);
        Assert.All(rec.Commits, c => Assert.Equal(50, c.ReplayId));
        Assert.All(rec.Commits, c => Assert.True(c.KeepAlive));
    }

    [Fact]
    public async Task Topic_does_not_recommit_an_unchanged_idle_keepalive()
    {
        // Topic commits client-side and has no server deadline, so an unchanged idle keep-alive is a no-op.
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 1); // commitKeepAliveWhenIdle defaults false

        await t.ObserveKeepAliveAsync(50);
        await t.WaitForWriterAsync();
        await t.ObserveKeepAliveAsync(50);
        await t.WaitForWriterAsync();
        await t.ObserveKeepAliveAsync(50);
        await t.WaitForWriterAsync();

        Assert.Single(rec.Commits);
        Assert.Equal(50, rec.Last);
    }

    [Fact]
    public async Task Keepalive_triggered_commit_carrying_completed_events_reports_events_received()
    {
        // At low volume the completion path rarely reaches the throttle, so the commit is TRIGGERED by a
        // keep-alive — but it covers handled events and must report events-received (isKeepAlive false),
        // or a low-volume subscription's last-event diagnostics never update (observed live: 19h overnight
        // with every commit flagged keepAlive:true).
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 100);

        t.Track(1);
        await t.CompleteAsync(1);          // below throttle — no commit yet
        await t.WaitForWriterAsync();
        Assert.Empty(rec.Commits);

        await t.ObserveKeepAliveAsync(2);  // keep-alive flushes the pending completion
        await t.WaitForWriterAsync();

        Assert.Equal(2, rec.Last);
        Assert.False(rec.Commits[^1].KeepAlive);
    }

    [Fact]
    public async Task Keepalive_drift_after_events_are_committed_reports_keepalive()
    {
        // Once handled events are committed, a later keep-alive advancing via global-bus drift alone
        // carries no events and must report keep-alive.
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 1);

        t.Track(1);
        await t.CompleteAsync(1);          // commits 1 (events)
        await t.WaitForWriterAsync();
        await t.ObserveKeepAliveAsync(9);  // pure drift — no completions since
        await t.WaitForWriterAsync();

        Assert.Equal(9, rec.Last);
        Assert.False(rec.Commits[0].KeepAlive);
        Assert.True(rec.Commits[^1].KeepAlive);
    }

    [Fact]
    public async Task Flush_with_pending_completions_reports_events_received()
    {
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 100);

        t.Track(1);
        await t.CompleteAsync(1);
        await t.WaitForWriterAsync();
        await t.FlushAsync();

        Assert.Equal(1, rec.Last);
        Assert.False(rec.Commits[^1].KeepAlive);
    }

    [Fact]
    public async Task Mes_reaffirm_never_commits_past_an_in_flight_event()
    {
        // The keep-alive re-affirm re-sends the last committed position — it must never jump to the tip
        // while an earlier event is still in flight.
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 1, commitKeepAliveWhenIdle: true);

        await t.ObserveKeepAliveAsync(10); // commit 10
        await t.WaitForWriterAsync();
        t.Track(11);                       // 11 now in flight
        await t.ObserveKeepAliveAsync(20); // tip is 20, but 11 in flight → re-affirm 10, not 20
        await t.WaitForWriterAsync();

        Assert.DoesNotContain(rec.Commits, c => c.ReplayId >= 11);
        Assert.Equal(10, rec.Last);
    }
}
