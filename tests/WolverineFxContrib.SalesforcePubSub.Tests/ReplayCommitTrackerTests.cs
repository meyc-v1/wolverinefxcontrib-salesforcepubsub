using Wolverine.SalesforcePubSub.Internals;

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
        await t.CompleteAsync(2);
        await t.CompleteAsync(3);

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

        Assert.DoesNotContain(rec.Commits, c => c.ReplayId >= 1);

        await t.CompleteAsync(1);   // now everything through 2 is done
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
        await t.CompleteAsync(12);

        Assert.DoesNotContain(rec.Commits, c => c.ReplayId >= 10);
    }

    [Fact]
    public async Task Throttle_commits_once_per_threshold()
    {
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 3);

        t.Track(1); t.Track(2); t.Track(3);
        await t.CompleteAsync(1);
        await t.CompleteAsync(2);
        await t.CompleteAsync(3);

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

        Assert.DoesNotContain(rec.Commits, c => c.ReplayId >= 5);
    }

    [Fact]
    public async Task Never_commits_backwards()
    {
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 1);

        t.Track(10);
        await t.CompleteAsync(10);
        await t.ObserveKeepAliveAsync(5); // stale/lower position

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
        await t.ObserveKeepAliveAsync(50); // idle, unchanged — re-affirm to refresh the server deadline
        await t.ObserveKeepAliveAsync(50);

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
        await t.ObserveKeepAliveAsync(50);
        await t.ObserveKeepAliveAsync(50);

        Assert.Single(rec.Commits);
        Assert.Equal(50, rec.Last);
    }

    [Fact]
    public async Task Mes_reaffirm_never_commits_past_an_in_flight_event()
    {
        // The keep-alive re-affirm re-sends the last committed position — it must never jump to the tip
        // while an earlier event is still in flight.
        var rec = new Recorder();
        var t = new ReplayCommitTracker(rec.Commit, commitEvery: 1, commitKeepAliveWhenIdle: true);

        await t.ObserveKeepAliveAsync(10); // commit 10
        t.Track(11);                       // 11 now in flight
        await t.ObserveKeepAliveAsync(20); // tip is 20, but 11 in flight → re-affirm 10, not 20

        Assert.DoesNotContain(rec.Commits, c => c.ReplayId >= 11);
        Assert.Equal(10, rec.Last);
    }
}
