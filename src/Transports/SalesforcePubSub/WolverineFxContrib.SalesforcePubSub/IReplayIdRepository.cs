namespace Wolverine.SalesforcePubSub;

/// <summary>
/// Persists and retrieves the last-seen replay id per topic so a subscription can resume
/// from where it left off after a reconnect. Only used by topic subscriptions — managed
/// event subscriptions (MES) track replay server-side and need no repository.
///
/// <para>Implement position writes with a <b>monotonic guard</b>: only overwrite a smaller stored id
/// (an equal write must pass). The transport bounds every call with its RepositoryCallTimeout and
/// <i>abandons</i> writes that exceed it — an abandoned write can complete late, after newer positions
/// have landed, and an unguarded store would regress the position (bounded duplicate redelivery on the
/// next cold start, never loss). <see cref="ResetForNewEventsOnlyAsync"/> is the one deliberate
/// regression and must bypass the guard.</para>
/// </summary>
public interface IReplayIdRepository
{
    /// <summary>
    /// Retrieves the last replay id for a given topic. Returns -1 (new events only) when none is stored.
    /// </summary>
    Task<long> GetLastReplayIdAsync(string topicName, CancellationToken token = default);

    /// <summary>
    /// Stores a committed replay position for a topic. <paramref name="kind"/> says whether handled
    /// events contributed to it or the position advanced through idle keep-alive drift alone —
    /// implementations that don't care about the distinction can ignore it.
    /// </summary>
    Task StoreReplayIdAsync(string topicName, long replayId, ReplayCommitKind kind, CancellationToken token = default);

    /// <summary>
    /// Resets the stored position so the subscription restarts from new-events-only.
    /// </summary>
    Task ResetForNewEventsOnlyAsync(string topicName, CancellationToken token = default);
}
