using System.Collections.Concurrent;

namespace Wolverine.SalesforcePubSub.Internals.Replay;

/// <summary>
/// Non-persistent <see cref="IReplayIdRepository"/> used as a fallback when a consumer registers none.
/// Positions are kept in memory only and are lost on restart (resumes new-events-only).
/// </summary>
internal sealed class InMemoryReplayIdRepository : IReplayIdRepository
{
    private readonly ConcurrentDictionary<string, long> _replayIds = new();

    public Task<long> GetLastReplayIdAsync(string topicName, CancellationToken token = default)
    {
        return Task.FromResult(_replayIds.GetOrAdd(topicName, ReplayIds.NewEventsOnly));
    }

    public Task StoreReplayIdAsync(string topicName, long replayId, ReplayCommitKind kind, CancellationToken token = default)
    {
        return AddOrUpdateReplayIdForTopicAsync(topicName, replayId);
    }

    public Task ResetForNewEventsOnlyAsync(string topicName, CancellationToken token = default)
    {
        return AddOrUpdateReplayIdForTopicAsync(topicName, ReplayIds.NewEventsOnly);
    }

    private Task AddOrUpdateReplayIdForTopicAsync(string topicName, long replayId)
    {
        _replayIds.AddOrUpdate(topicName, replayId, (_, _) => replayId);
        return Task.CompletedTask;
    }
}
