using System.Collections.Concurrent;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

/// <summary>One received event as observed by a handler, with the envelope facts the tests assert on.</summary>
public sealed record ReceivedEvent(
    string EventType,
    string? Message,
    long ReplayId,
    Guid EnvelopeId,
    string? TopicName,
    DateTimeOffset? SentAt);

/// <summary>
/// Per-host recording sink the test handlers write into (the pattern Wolverine's own Kafka suite uses
/// for receive assertions — a shared sink plus poll-until). Tests isolate themselves by correlation:
/// every published Message__c carries a unique id, and assertions match only their own events, so
/// anything else arriving on the shared org topics is ignored rather than failed on.
/// </summary>
public sealed class EventSink
{
    private readonly ConcurrentQueue<ReceivedEvent> _events = new();

    public void Record(ReceivedEvent evt) => _events.Enqueue(evt);

    public IReadOnlyList<ReceivedEvent> Snapshot() => _events.ToList();

    /// <summary>Polls until at least <paramref name="count"/> events match, or throws with the sink contents.</summary>
    public async Task<IReadOnlyList<ReceivedEvent>> WaitForAsync(
        Func<ReceivedEvent, bool> match, int count, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var hits = _events.Where(match).ToList();
            if (hits.Count >= count)
                return hits;

            await Task.Delay(100);
        }

        var seen = _events.IsEmpty
            ? "(none)"
            : string.Join("; ", _events.Select(e => $"{e.EventType} '{e.Message}' replay {e.ReplayId}"));
        throw new TimeoutException(
            $"Expected {count} matching event(s) within {timeout}, but the sink saw: {seen}");
    }
}
