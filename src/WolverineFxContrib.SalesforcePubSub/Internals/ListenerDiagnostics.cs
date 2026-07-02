namespace Wolverine.SalesforcePubSub.Internals;

/// <summary>
/// Health counters for a <see cref="SalesforceListener"/> plus the trip logic for its periodic heartbeat
/// and silent-cold watchdog loops (ported from the original SubscriptionOrchestrator's HeartbeatAsync /
/// MonitorAsync — DECISIONS #15). Written by the consume loop, read by the timer loops, so all access is
/// lock-guarded; timestamps are passed in explicitly to keep the trip logic deterministic under test.
/// </summary>
internal sealed class ListenerDiagnostics
{
    private readonly object _lock = new();

    // The watchdog's response count at its previous poll — the old MonitorAsync "did anything move" guard.
    private long _lastKnownResponseCount;

    public ListenerDiagnostics(DateTimeOffset startedOnUtc)
    {
        StartedOnUtc = startedOnUtc;
        LastSuccessUtc = startedOnUtc;
    }

    public DateTimeOffset StartedOnUtc { get; }
    public DateTimeOffset LastSuccessUtc { get; private set; }
    public DateTimeOffset? LastErrorUtc { get; private set; }
    public long ResponseCount { get; private set; }
    public long TotalEvents { get; private set; }
    public long TotalErrors { get; private set; }
    public long Reconnects { get; private set; }
    public long ConsecutiveErrors { get; private set; }

    /// <summary>
    /// A response (event batch or keep-alive) arrived: the stream is healthy. When this response ends an
    /// error streak — a recovery — returns the streak length and the downtime for the recovery log;
    /// otherwise null. Unlike the original orchestrator, reconnects count actual recoveries, not errors.
    /// </summary>
    public (long ConsecutiveErrors, TimeSpan Downtime)? RecordSuccess(int eventCount, DateTimeOffset nowUtc)
    {
        lock (_lock)
        {
            (long, TimeSpan)? recovery = null;
            if (ConsecutiveErrors > 0)
            {
                recovery = (ConsecutiveErrors, nowUtc - LastSuccessUtc);
                ConsecutiveErrors = 0;
                Reconnects++;
            }

            ResponseCount++;
            TotalEvents += eventCount;
            LastSuccessUtc = nowUtc;
            return recovery;
        }
    }

    /// <summary>A stream/connect attempt failed. Returns the values the error log and backoff need.</summary>
    public (long ConsecutiveErrors, TimeSpan SinceLastSuccess) RecordError(DateTimeOffset nowUtc)
    {
        lock (_lock)
        {
            ConsecutiveErrors++;
            TotalErrors++;
            LastErrorUtc = nowUtc;
            return (ConsecutiveErrors, nowUtc - LastSuccessUtc);
        }
    }

    /// <summary>
    /// The watchdog condition: stale when nothing succeeded for <paramref name="threshold"/> AND the
    /// response count hasn't moved since the previous check (a clock-skew guard — a response always stamps
    /// <see cref="LastSuccessUtc"/>). Deliberately trips on every poll while the stream stays cold.
    /// </summary>
    public bool CheckStale(TimeSpan threshold, DateTimeOffset nowUtc, out TimeSpan sinceLastSuccess)
    {
        lock (_lock)
        {
            sinceLastSuccess = nowUtc - LastSuccessUtc;
            var stale = sinceLastSuccess >= threshold && ResponseCount == _lastKnownResponseCount;
            _lastKnownResponseCount = ResponseCount;
            return stale;
        }
    }

    /// <summary>A consistent snapshot for the heartbeat log line.</summary>
    public (DateTimeOffset StartedOn, long Responses, long Events, long Errors, long Reconnects,
        DateTimeOffset LastSuccess, DateTimeOffset? LastError) Snapshot()
    {
        lock (_lock)
            return (StartedOnUtc, ResponseCount, TotalEvents, TotalErrors, Reconnects, LastSuccessUtc, LastErrorUtc);
    }
}
