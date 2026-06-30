namespace Wolverine.SalesforcePubSub.Internals;

/// <summary>
/// Tracks the safe replay-commit position for a single subscription so the committed position never
/// advances past an event that is still in flight (received but not yet completed) — even under
/// out-of-order completion. Modeled on Wolverine's Kafka <c>OffsetWatermark</c>, adapted for Salesforce
/// "resume after replayId" semantics and a single ordered stream (no partitions).
///
/// <para>Lifecycle: <see cref="Track"/> on receive (marks in-flight); <see cref="CompleteAsync"/> on a
/// resolved envelope — success, dead-letter, discard, or no-handler — which removes it and advances the
/// position. A <b>deferred</b> (requeued) envelope is intentionally left in flight so it holds the floor
/// until it is eventually completed or re-delivered on reconnect: this transport has no native per-message
/// requeue, so the supported failure model is inline-retry + DLQ (see DECISIONS #2).
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
    /// <summary>Persists a committed position. Args: replayId, isKeepAlive. Best-effort; never cancelled.</summary>
    private readonly Func<long, bool, Task> _commit;
    private readonly int _commitEvery;

    private readonly object _lock = new();
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly SortedSet<long> _inflight = new();

    private long _highWater = -1;
    private long _lastCommitted = -1;
    private bool _seen;
    private int _sinceCommit;

    public ReplayCommitTracker(Func<long, bool, Task> commit, int commitEvery)
    {
        _commit = commit ?? throw new ArgumentNullException(nameof(commit));
        _commitEvery = commitEvery < 1 ? 1 : commitEvery;
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

    /// <summary>An envelope was resolved (handled / dead-lettered / discarded). Advance and maybe commit.</summary>
    public Task CompleteAsync(long replayId)
    {
        long? position;
        lock (_lock)
        {
            _inflight.Remove(replayId);
            Observe(replayId);
            _sinceCommit++;
            position = TryTakeCommittable(force: false);
        }

        return position is { } p ? CommitAsync(p, isKeepAlive: false) : Task.CompletedTask;
    }

    /// <summary>A keep-alive carries a position but no events; advance during idle (respects the in-flight floor).</summary>
    public Task ObserveKeepAliveAsync(long replayId)
    {
        long? position;
        lock (_lock)
        {
            Observe(replayId);
            position = TryTakeCommittable(force: true); // keep-alives are sparse — commit promptly
        }

        return position is { } p ? CommitAsync(p, isKeepAlive: true) : Task.CompletedTask;
    }

    /// <summary>Force a final commit of the current safe position (shutdown).</summary>
    public Task FlushAsync()
    {
        long? position;
        lock (_lock)
        {
            position = TryTakeCommittable(force: true);
        }

        return position is { } p ? CommitAsync(p, isKeepAlive: false) : Task.CompletedTask;
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

    private async Task CommitAsync(long replayId, bool isKeepAlive)
    {
        await _writeGate.WaitAsync().ConfigureAwait(false);
        try
        {
            await _commit(replayId, isKeepAlive).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }
}
