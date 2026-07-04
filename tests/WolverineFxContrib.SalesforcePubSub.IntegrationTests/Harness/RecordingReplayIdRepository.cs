using System.Collections.Concurrent;
using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

/// <summary>One replay commit as observed by the repository.</summary>
public sealed record ReplayCommit(string Method, string Topic, long ReplayId, DateTimeOffset At);

/// <summary>
/// In-memory <see cref="IReplayIdRepository"/> that also records every commit. Shared across hosts in a
/// test, it plays the durable store for restart-resume scenarios; its commit ledger backs the
/// repo-semantics assertions (events-received vs keep-alive) and the monotonicity checks in the
/// backpressure tests.
/// </summary>
public sealed class RecordingReplayIdRepository : IReplayIdRepository
{
    private const long NewEventsOnly = -1;

    private readonly ConcurrentDictionary<string, long> _positions = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentQueue<ReplayCommit> _commits = new();

    public IReadOnlyList<ReplayCommit> Commits => _commits.ToList();

    public Task<long> GetLastReplayIdAsync(string topicName, CancellationToken token = default)
        => Task.FromResult(_positions.GetOrAdd(topicName, NewEventsOnly));

    public Task ReportKeepAliveResponseAsync(string topicName, long replayId, CancellationToken token = default)
        => StoreAsync("KeepAlive", topicName, replayId);

    public Task ReportEventsReceivedResponseAsync(string topicName, long replayId, List<long> replayIdsReceived, CancellationToken token = default)
        => StoreAsync("EventsReceived", topicName, replayId);

    public Task ResetForNewEventsOnlyAsync(string topicName, CancellationToken token = default)
    {
        _positions[topicName] = NewEventsOnly;
        return Task.CompletedTask;
    }

    private Task StoreAsync(string method, string topic, long replayId)
    {
        _positions[topic] = replayId;
        _commits.Enqueue(new ReplayCommit(method, topic, replayId, DateTimeOffset.UtcNow));
        return Task.CompletedTask;
    }
}
