using System.Collections.Concurrent;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// Shared counters for the durability/resiliency runs. The publisher records what it sent; handlers record
/// what was delivered (with the replay id). The <see cref="HeartbeatService"/> periodically snapshots these
/// so loss/duplication is observable across an outage: published-vs-handled counts, and whether replay ids
/// stay contiguous (gaps = loss, repeats = duplicates).
/// </summary>
public sealed class RunMetrics
{
    private readonly ConcurrentDictionary<string, long> _published = new();
    private readonly ConcurrentDictionary<string, long> _handled = new();
    private readonly ConcurrentDictionary<string, long> _lastReplayId = new();

    public void RecordPublished(string eventName) => _published.AddOrUpdate(eventName, 1, (_, n) => n + 1);

    public void RecordHandled(string messageType, long replayId)
    {
        _handled.AddOrUpdate(messageType, 1, (_, n) => n + 1);
        _lastReplayId[messageType] = replayId;
    }

    public IReadOnlyDictionary<string, long> Published => _published;
    public IReadOnlyDictionary<string, long> Handled => _handled;
    public IReadOnlyDictionary<string, long> LastReplayId => _lastReplayId;
}
