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
    /// Reports a committed position that no handled event contributed to — the replay id advanced through
    /// keep-alive drift alone (replay ids are global across all Salesforce streaming events, so the
    /// position moves during idle). Store the position; do not update any last-event diagnostics.
    /// </summary>
    Task ReportKeepAliveResponseAsync(string topicName, long replayId, CancellationToken token = default);

    /// <summary>
    /// Reports a committed position that covers one or more handled events, regardless of whether the
    /// commit was triggered by the completion throttle or flushed by a keep-alive/shutdown.
    /// </summary>
    Task ReportEventsReceivedResponseAsync(string topicName, long replayId, List<long> replayIdsReceived, CancellationToken token = default);

    /// <summary>
    /// Resets the stored position so the subscription restarts from new-events-only.
    /// </summary>
    Task ResetForNewEventsOnlyAsync(string topicName, CancellationToken token = default);
}
