using Wolverine.SalesforcePubSub.Internals;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// The listener's health counters and observability trip logic (DECISIONS #15): successes and errors
/// accumulate correctly, a recovery is one reconnect per error streak, and the silent-cold watchdog trips
/// only when the stream is genuinely cold — past the threshold with no responses moving — and keeps
/// tripping every poll until it recovers.
/// </summary>
public class ListenerDiagnosticsTests
{
    private static readonly DateTimeOffset T0 = new(2026, 7, 1, 12, 0, 0, TimeSpan.Zero);

    private static TimeSpan Minutes(int m) => TimeSpan.FromMinutes(m);

    [Fact]
    public void Success_counts_responses_and_events()
    {
        var d = new ListenerDiagnostics(T0);

        d.RecordSuccess(eventCount: 0, T0 + Minutes(1)); // keep-alive
        d.RecordSuccess(eventCount: 3, T0 + Minutes(2));

        Assert.Equal(2, d.ResponseCount);
        Assert.Equal(3, d.TotalEvents);
        Assert.Equal(T0 + Minutes(2), d.LastSuccessUtc);
    }

    [Fact]
    public void Success_without_prior_errors_is_not_a_recovery()
    {
        var d = new ListenerDiagnostics(T0);

        Assert.Null(d.RecordSuccess(1, T0 + Minutes(1)));
        Assert.Equal(0, d.Reconnects);
    }

    [Fact]
    public void First_success_after_errors_is_a_recovery_with_streak_and_downtime()
    {
        var d = new ListenerDiagnostics(T0);
        d.RecordSuccess(1, T0 + Minutes(1));      // healthy baseline
        d.RecordError(T0 + Minutes(2));
        d.RecordError(T0 + Minutes(3));

        var recovery = d.RecordSuccess(1, T0 + Minutes(5));

        Assert.NotNull(recovery);
        Assert.Equal(2, recovery.Value.ConsecutiveErrors);
        Assert.Equal(Minutes(4), recovery.Value.Downtime); // since the last success at +1m
        Assert.Equal(1, d.Reconnects);
        Assert.Equal(0, d.ConsecutiveErrors);
    }

    [Fact]
    public void Consecutive_errors_reset_on_success_but_totals_accumulate()
    {
        var d = new ListenerDiagnostics(T0);

        d.RecordError(T0 + Minutes(1));
        d.RecordError(T0 + Minutes(2));
        d.RecordSuccess(0, T0 + Minutes(3));
        var (consecutive, _) = d.RecordError(T0 + Minutes(4));

        Assert.Equal(1, consecutive);
        Assert.Equal(3, d.TotalErrors);
        Assert.Equal(T0 + Minutes(4), d.LastErrorUtc);
    }

    [Fact]
    public void One_reconnect_per_error_streak()
    {
        var d = new ListenerDiagnostics(T0);

        d.RecordError(T0 + Minutes(1));
        d.RecordError(T0 + Minutes(2));
        d.RecordSuccess(0, T0 + Minutes(3)); // streak 1 ends
        d.RecordError(T0 + Minutes(4));
        d.RecordSuccess(0, T0 + Minutes(5)); // streak 2 ends

        Assert.Equal(2, d.Reconnects);
    }

    [Fact]
    public void Error_reports_time_since_last_success()
    {
        var d = new ListenerDiagnostics(T0);
        d.RecordSuccess(0, T0 + Minutes(10));

        var (_, sinceLastSuccess) = d.RecordError(T0 + Minutes(25));

        Assert.Equal(Minutes(15), sinceLastSuccess);
    }

    [Fact]
    public void Watchdog_does_not_trip_under_threshold()
    {
        var d = new ListenerDiagnostics(T0);

        Assert.False(d.CheckStale(Minutes(15), T0 + Minutes(14), out _));
    }

    [Fact]
    public void Watchdog_trips_when_cold_past_threshold()
    {
        var d = new ListenerDiagnostics(T0);

        var stale = d.CheckStale(Minutes(15), T0 + Minutes(16), out var duration);

        Assert.True(stale);
        Assert.Equal(Minutes(16), duration);
    }

    [Fact]
    public void Watchdog_keeps_tripping_every_poll_while_cold()
    {
        var d = new ListenerDiagnostics(T0);

        Assert.True(d.CheckStale(Minutes(15), T0 + Minutes(16), out _));
        Assert.True(d.CheckStale(Minutes(15), T0 + Minutes(17), out _));
        Assert.True(d.CheckStale(Minutes(15), T0 + Minutes(18), out _));
    }

    [Fact]
    public void Watchdog_resets_once_a_response_arrives()
    {
        var d = new ListenerDiagnostics(T0);
        Assert.True(d.CheckStale(Minutes(15), T0 + Minutes(16), out _));

        d.RecordSuccess(1, T0 + Minutes(17));

        Assert.False(d.CheckStale(Minutes(15), T0 + Minutes(18), out _));
    }

    [Fact]
    public void Watchdog_count_guard_skips_one_poll_after_responses_moved()
    {
        // The "response count unchanged" guard from the old MonitorAsync: even if the clock claims the
        // threshold has passed, a poll that observes new responses does not trip — only the next poll
        // with a frozen count does.
        var d = new ListenerDiagnostics(T0);
        d.CheckStale(Minutes(15), T0 + Minutes(1), out _);   // baseline poll, count 0
        d.RecordSuccess(1, T0 + Minutes(2));                 // a response arrives

        // 20 minutes later: past the threshold, but the count moved since the previous poll.
        Assert.False(d.CheckStale(Minutes(15), T0 + Minutes(22), out _));
        // Next poll: count is frozen now → trips.
        Assert.True(d.CheckStale(Minutes(15), T0 + Minutes(23), out _));
    }

    [Fact]
    public void Snapshot_reflects_all_counters()
    {
        var d = new ListenerDiagnostics(T0);
        d.RecordSuccess(2, T0 + Minutes(1));
        d.RecordError(T0 + Minutes(2));
        d.RecordSuccess(1, T0 + Minutes(3)); // recovery

        var s = d.Snapshot();

        Assert.Equal(T0, s.StartedOn);
        Assert.Equal(2, s.Responses);
        Assert.Equal(3, s.Events);
        Assert.Equal(1, s.Errors);
        Assert.Equal(1, s.Reconnects);
        Assert.Equal(T0 + Minutes(3), s.LastSuccess);
        Assert.Equal(T0 + Minutes(2), s.LastError);
    }
}
