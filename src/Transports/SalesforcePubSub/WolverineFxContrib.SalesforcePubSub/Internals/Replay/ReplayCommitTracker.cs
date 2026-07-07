namespace Wolverine.SalesforcePubSub.Internals.Replay;

/// <summary>
/// Tracks the safe replay-commit position for a single subscription so the committed position never
/// advances past an event that is still in flight (received but not yet completed) — even under
/// out-of-order completion. Modeled on Wolverine's Kafka <c>OffsetWatermark</c>, adapted for Salesforce
/// "resume after replayId" semantics and a single ordered stream (no partitions).
///
/// <para>Lifecycle: <see cref="Track"/> on receive (marks in-flight); <see cref="CompleteAsync"/> on a
/// resolved envelope — success, dead-letter, discard, or no-handler — which removes it and advances the
/// position. A <b>deferred</b> (requeued) envelope stays in flight — the listener re-injects it for an
/// in-memory retry (Kafka-style; DECISIONS #10), never re-tracking it — so it holds the floor until it is
/// finally completed (handler success, or the terminal <c>MoveToErrorQueue</c>, which also completes).
/// <see cref="ObserveKeepAliveAsync"/> advances the position during idle periods when nothing is in
/// flight.</para>
///
/// <para>Commits are throttled (written once the position advances and at least <c>commitEvery</c>
/// completions have occurred) and serialized through a gate — required for MES, whose commit shares the
/// gRPC request stream (concurrent writes throw). <see cref="FlushAsync"/> forces a final commit on
/// shutdown.</para>
/// </summary>
internal sealed class ReplayCommitTracker
{
    /// <summary>
    /// Persists a committed position. Args: replayId, isKeepAlive — true only when NO completed events
    /// contributed to the position (idle keep-alive drift or a MES re-affirm); a commit covering handled
    /// events reports false regardless of whether the completion throttle or a keep-alive triggered the
    /// write. Best-effort; never cancelled.
    /// </summary>
    private readonly Func<long, bool, Task> _commit;
    private readonly int _commitEvery;

    // MES closes a managed subscription that receives no CommitReplayRequest within its server-side deadline
    // (1800s). During idle the watermark doesn't advance, so nothing would be committed and the subscription
    // is torn down every ~30 min. When true (MES), re-affirm the last committed position on each idle
    // keep-alive to refresh that deadline. Topic commits client-side and has no such deadline → false.
    private readonly bool _refreshOnKeepAlive;

    private readonly object _lock = new();
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly SortedSet<long> _inflight = new();

    private long _highWater = -1;
    private long _lastCommitted = -1;
    private long _lastWritten = -1;
    private bool _seen;
    private int _sinceCommit;
    private (long ReplayId, bool IsKeepAlive)? _pending;
    private bool _writerRunning;
    private Task _writer = Task.CompletedTask;

    public ReplayCommitTracker(Func<long, bool, Task> commit, int commitEvery, bool commitKeepAliveWhenIdle = false)
    {
        _commit = commit ?? throw new ArgumentNullException(nameof(commit));
        _commitEvery = commitEvery < 1 ? 1 : commitEvery;
        _refreshOnKeepAlive = commitKeepAliveWhenIdle;
    }

    /// <summary>Marks an event in flight (received, not yet completed). Called before dispatch.</summary>
    public void Track(long replayId)
    {
        lock (_lock)
        {
            _inflight.Add(replayId);
            Observe(replayId);
        }
    }

    /// <summary>
    /// An envelope was resolved (handled / dead-lettered / discarded). Advance and maybe commit.
    /// Returns immediately — the write happens on the single-flight writer (see <see cref="ScheduleLocked"/>),
    /// so a slow or hung repository can never stall the completion path (DECISIONS #23).
    /// </summary>
    public Task CompleteAsync(long replayId)
    {
        lock (_lock)
        {
            _inflight.Remove(replayId);
            Observe(replayId);
            _sinceCommit++;
            if (TryTakeCommittable(force: false) is { } position)
                ScheduleLocked(position, isKeepAlive: false);
        }

        return Task.CompletedTask;
    }

    /// <summary>A keep-alive carries a position but no events; advance during idle (respects the in-flight floor).</summary>
    public Task ObserveKeepAliveAsync(long replayId)
    {
        long? position;
        bool fromCompletions;
        lock (_lock)
        {
            Observe(replayId);

            // The commit flag reports whether completed events contributed to the position — not what
            // triggered the write. At low volume the completion path rarely reaches the throttle, so most
            // commits are keep-alive-TRIGGERED yet carry handled events; flagging those "keep-alive" would
            // starve the repository's events-handled path (its last-event diagnostics never update).
            // Captured before TryTakeCommittable, which resets the counter.
            fromCompletions = _sinceCommit > 0;
            position = TryTakeCommittable(force: true); // keep-alives are sparse — commit promptly

            // Nothing new to commit, but MES needs a periodic CommitReplayRequest to keep the managed
            // subscription alive (see _refreshOnKeepAlive). Re-affirm the last committed position — a no-op
            // for the watermark (never regresses, never advances past an in-flight event) that just resets
            // the server's deadline. Only once something has been committed (_lastCommitted >= 0).
            if (position is null)
            {
                fromCompletions = false; // a pure re-affirm carries no events
                if (_refreshOnKeepAlive && _lastCommitted >= 0)
                    position = _lastCommitted;
            }

            if (position is { } p)
                ScheduleLocked(p, isKeepAlive: !fromCompletions);
        }

        // Returns immediately: the read loop awaits this call, and a hung repository on that path is
        // exactly how the #23 soak went deaf. The single-flight writer owns the wait.
        return Task.CompletedTask;
    }

    /// <summary>
    /// The position to resume an <b>in-process reconnect</b> from: the highest replayId with every event
    /// ≤ it completed (lowest in-flight − 1, otherwise the high-water). Salesforce delivers events strictly
    /// after this id, so in-flight (received-but-not-completed) events are re-delivered while handled ones
    /// are not. Returns null until something has been observed — the caller then falls back to the durable
    /// store (a true cold start). Non-mutating; does not touch commit state.
    /// </summary>
    public long? TryGetResumePosition()
    {
        lock (_lock)
        {
            if (!_seen) return null;
            return _inflight.Count == 0 ? _highWater : _inflight.Min - 1;
        }
    }

    /// <summary>
    /// Force a final commit of the current safe position (shutdown) and wait for the writer to drain —
    /// the one caller that genuinely needs the write completed (the listener bounds the wait at 5s).
    /// </summary>
    public Task FlushAsync()
    {
        lock (_lock)
        {
            // Same flag semantics as the keep-alive path: commit as events-handled only when completed
            // events contributed to the flushed position (a drift-only flush carries none).
            var fromCompletions = _sinceCommit > 0;
            if (TryTakeCommittable(force: true) is { } position)
                ScheduleLocked(position, isKeepAlive: !fromCompletions);

            return _writer;
        }
    }

    /// <summary>The current writer task — test hook for awaiting commit quiescence without forcing a flush.</summary>
    internal Task WaitForWriterAsync()
    {
        lock (_lock)
            return _writer;
    }

    private void Observe(long replayId)
    {
        if (!_seen)
        {
            _highWater = replayId;
            _seen = true;
            return;
        }

        if (replayId > _highWater)
        {
            _highWater = replayId;
        }
    }

    /// <summary>
    /// The safe commit position — the highest replayId such that every event with replayId ≤ it is done:
    /// <c>lowest in-flight − 1</c> when something is in flight, otherwise the high-water mark. Returns null
    /// when the position hasn't advanced past the last commit, or (for un-forced calls) the throttle
    /// threshold hasn't been reached.
    /// </summary>
    private long? TryTakeCommittable(bool force)
    {
        if (!_seen) return null;
        if (!force && _sinceCommit < _commitEvery) return null;

        var candidate = _inflight.Count == 0 ? _highWater : _inflight.Min - 1;
        if (candidate <= _lastCommitted) return null;

        _lastCommitted = candidate;
        _sinceCommit = 0;
        return candidate;
    }

    /// <summary>
    /// Latest-wins hand-off to the single-flight writer. Called under <see cref="_lock"/>. Positions
    /// coalesce: if a write is already in flight, only the newest pending position is kept — the same
    /// semantics the commit throttle already gives, now decoupled from the caller (DECISIONS #23: a
    /// hung repository parks the writer, never the completion path or the read loop).
    /// </summary>
    private void ScheduleLocked(long replayId, bool isKeepAlive)
    {
        _pending = (replayId, isKeepAlive);
        if (_writerRunning)
            return;

        _writerRunning = true;
        _writer = Task.Run(WriteLoopAsync);
    }

    private async Task WriteLoopAsync()
    {
        while (true)
        {
            (long ReplayId, bool IsKeepAlive) next;
            lock (_lock)
            {
                if (_pending is not { } pending)
                {
                    _writerRunning = false;
                    return;
                }

                _pending = null;
                next = pending;
            }

            await _writeGate.WaitAsync().ConfigureAwait(false);
            try
            {
                // Writer-side monotonicity: a staler position written last would regress the durable
                // row — bounded duplicates on the next cold start. Drop strictly-older writes; an EQUAL
                // position passes deliberately, because the MES idle re-affirm re-sends the last
                // committed position to reset the server's 1800s no-commit deadline.
                if (next.ReplayId >= _lastWritten)
                {
                    _lastWritten = next.ReplayId;
                    // Best-effort by contract (the listener's delegate absorbs and logs failures), but a
                    // throwing delegate must not kill the writer and silently stop all future commits.
                    try
                    {
                        await _commit(next.ReplayId, next.IsKeepAlive).ConfigureAwait(false);
                    }
                    catch
                    {
                        // The position is re-committed on the next completion/keep-alive.
                    }
                }
            }
            finally
            {
                _writeGate.Release();
            }
        }
    }
}
