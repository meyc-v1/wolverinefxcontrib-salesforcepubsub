using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Wolverine.SalesforcePubSub;
using TestHost.Settings;

namespace TestHost.Replay;

/// <summary>
/// Persistent topic replay store (raw ADO.NET, AAD auth) for the resume-across-restart durability test.
/// In-memory is authoritative for the process lifetime — reconnects never re-read SQL — so only a fresh
/// process cold-reads the last persisted position. Writes are write-through per report (the transport's
/// <c>ReplayCommitTracker</c> already coalesces upstream, so volume is low). A cold-start read failure is
/// fail-loud: we let it throw so the listener backs off and retries rather than fabricating a position.
/// MES is unaffected (server-side replay); this only serves topic endpoints.
/// </summary>
internal sealed class SqlReplayIdRepository : IReplayIdRepository
{
    private const long NewEventsOnly = -1;

    private sealed class TopicPosition
    {
        public bool Loaded;
        public long ReplayId = NewEventsOnly;
        public long? LastEventReplayId;
        public DateTime? LastEventOn;
    }

    private readonly ConcurrentDictionary<string, TopicPosition> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly SqlReplayStore _store;
    private readonly ILogger<SqlReplayIdRepository> _logger;

    public SqlReplayIdRepository(IOptions<ReplaySettings> options, ILogger<SqlReplayIdRepository> logger)
    {
        var settings = options.Value;
        _logger = logger;
        _store = new SqlReplayStore(settings.ConnectionString!, settings.Schema, settings.TableName, settings.Application, settings.Instance);
        _logger.LogInformation(
            "SQL replay store configured. Schema: {Schema}, Table: {Table}, Application: {Application}, Instance: {Instance}",
            settings.Schema, settings.TableName, settings.Application, settings.Instance);
    }

    public async Task<long> GetLastReplayIdAsync(string topicName, CancellationToken token = default)
    {
        var state = _topics.GetOrAdd(topicName, _ => new TopicPosition());

        // In-memory is authoritative once loaded — reconnects within the process never re-read SQL.
        lock (state)
        {
            if (state.Loaded)
            {
                _logger.LogDebug("Resolved replay id {ReplayId} for Topic: {Topic} from memory", state.ReplayId, topicName);
                return state.ReplayId;
            }
        }

        // Cold start: fail-loud — a read failure propagates so the listener backs off and retries.
        var stored = await _store.TryGetReplayIdAsync(topicName, token).ConfigureAwait(false);
        if (stored.HasValue)
        {
            lock (state) { state.ReplayId = stored.Value; state.Loaded = true; }
            _logger.LogInformation("Cold-start read replay id {ReplayId} for Topic: {Topic} from SQL", stored.Value, topicName);
            return stored.Value;
        }

        await _store.InsertNewAsync(topicName, NewEventsOnly, DateTime.UtcNow, token).ConfigureAwait(false);
        lock (state) { state.ReplayId = NewEventsOnly; state.Loaded = true; }
        _logger.LogInformation("No stored replay id for Topic: {Topic}; inserted new row at {ReplayId}", topicName, NewEventsOnly);
        return NewEventsOnly;
    }

    public Task ReportKeepAliveResponseAsync(string topicName, long replayId, CancellationToken token = default)
    {
        var state = _topics.GetOrAdd(topicName, _ => new TopicPosition());
        long? lastEvent;
        DateTime? lastEventOn;
        lock (state)
        {
            state.ReplayId = replayId;
            state.Loaded = true;
            lastEvent = state.LastEventReplayId;
            lastEventOn = state.LastEventOn;
        }

        return _store.UpsertAsync(topicName, replayId, lastEvent, lastEventOn, DateTime.UtcNow, token);
    }

    public Task ReportEventsReceivedResponseAsync(string topicName, long replayId, List<long> replayIdsReceived, CancellationToken token = default)
    {
        var lastEventReplayId = replayIdsReceived is { Count: > 0 } ? replayIdsReceived.Max() : replayId;
        var now = DateTime.UtcNow;
        var state = _topics.GetOrAdd(topicName, _ => new TopicPosition());
        lock (state)
        {
            state.ReplayId = replayId;
            state.LastEventReplayId = lastEventReplayId;
            state.LastEventOn = now;
            state.Loaded = true;
        }

        return _store.UpsertAsync(topicName, replayId, lastEventReplayId, now, now, token);
    }

    public Task ResetForNewEventsOnlyAsync(string topicName, CancellationToken token = default)
    {
        var state = _topics.GetOrAdd(topicName, _ => new TopicPosition());
        lock (state) { state.ReplayId = NewEventsOnly; state.Loaded = true; }
        _logger.LogInformation("Resetting replay id to NewEventsOnly for Topic: {Topic}", topicName);
        return _store.UpsertAsync(topicName, NewEventsOnly, null, null, DateTime.UtcNow, token);
    }
}
