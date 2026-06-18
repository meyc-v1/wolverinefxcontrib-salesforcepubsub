namespace Wolverine.SalesforcePubSub;

/// <summary>
/// Persists and retrieves the last-seen replay id per topic so a subscription can resume
/// from where it left off after a reconnect. Only used by topic subscriptions — managed
/// event subscriptions (MES) track replay server-side and need no repository.
/// </summary>
public interface IReplayIdRepository
{
    /// <summary>
    /// Retrieves the last replay id for a given topic. Returns -1 (new events only) when none is stored.
    /// </summary>
    Task<long> GetLastReplayIdAsync(string topicName, CancellationToken token = default);

    /// <summary>
    /// Reports a keep-alive response. No events are returned for a keep-alive, but the replay id may
    /// still advance because replay ids are global across all Salesforce streaming events.
    /// </summary>
    Task ReportKeepAliveResponseAsync(string topicName, long replayId, CancellationToken token = default);

    /// <summary>
    /// Reports the receipt of events for a topic by updating its replay id.
    /// </summary>
    Task ReportEventsReceivedResponseAsync(string topicName, long replayId, List<long> replayIdsReceived, CancellationToken token = default);

    /// <summary>
    /// Resets the stored position so the subscription restarts from new-events-only.
    /// </summary>
    Task ResetForNewEventsOnlyAsync(string topicName, CancellationToken token = default);
}
