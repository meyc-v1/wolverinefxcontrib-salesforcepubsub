using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Wolverine.Runtime;
using Wolverine.Transports;

namespace Wolverine.SalesforcePubSub.Internals;

/// <summary>
/// Wolverine listener over a Salesforce Pub/Sub subscription. Owns the connect/process/backoff/reconnect
/// loop (ported from the original SubscriptionOrchestrator) so Wolverine never has to restart a faulted
/// stream — the loop self-heals internally and only stops on shutdown.
///
/// Replay/ack: a per-envelope <see cref="ReplayCommitTracker"/> watermark commits the replay position
/// only after an envelope is resolved via <see cref="CompleteAsync"/> (success, dead-letter, or discard),
/// never past an in-flight event. <see cref="DeferAsync"/> holds the position (this transport has no
/// native per-message requeue — inline-retry + DLQ are the failure model). Under <c>Inline</c> this yields
/// at-least-once; <c>BufferedInMemory</c> stays at-most-once (its receiver completes on receipt). Commits
/// route to the current transport (the MES stream rotates on reconnect; the topic repository is long-lived).
/// </summary>
internal sealed class SalesforceListener : IListener
{
    private readonly string _resource;
    private readonly bool _resumesFromWatermark;
    private readonly Func<long?, ISubscriptionTransport> _transportFactory;
    private readonly IReceiver _receiver;
    private readonly EventTypeResolver _eventTypes;
    private readonly IReadOnlyList<string> _prewarmTopics;
    private readonly HashSet<string> _unmappedWarned = [];
    private readonly CachingSchemaRepository _schemaRepository;
    private readonly SubscriberComponentsSettings _settings;
    private readonly IBackoffStrategy _backoffStrategy;
    private readonly CachingAuthenticationTokenProvider _tokenProvider;
    private readonly ReplayCommitTracker _commits;
    private readonly ListenerDiagnostics _diagnostics = new(DateTimeOffset.UtcNow);
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;
    private readonly Task _runner;

    private ISubscriptionTransport? _currentTransport;

    public SalesforceListener(
        Uri address,
        string resource,
        bool resumesFromWatermark,
        Func<long?, ISubscriptionTransport> transportFactory,
        IReceiver receiver,
        EventTypeResolver eventTypes,
        IReadOnlyList<string> prewarmTopics,
        CachingSchemaRepository schemaRepository,
        SubscriberComponentsSettings settings,
        IBackoffStrategy backoffStrategy,
        CachingAuthenticationTokenProvider tokenProvider,
        ILogger<SalesforceListener> logger,
        CancellationToken runtimeCancellation)
    {
        Address = address;
        _resource = resource;
        _resumesFromWatermark = resumesFromWatermark;
        _transportFactory = transportFactory;
        _receiver = receiver;
        _eventTypes = eventTypes;
        _prewarmTopics = prewarmTopics;
        _schemaRepository = schemaRepository;
        _settings = settings;
        _backoffStrategy = backoffStrategy;
        _tokenProvider = tokenProvider;
        _logger = logger;
        // MES (which resumes server-side, not from the watermark) must re-commit on idle keep-alives to keep
        // its managed subscription within Salesforce's 1800s no-commit deadline; topic has no such deadline.
        _commits = new ReplayCommitTracker(CommitToCurrentTransportAsync, settings.FetchCount,
            commitKeepAliveWhenIdle: !resumesFromWatermark);
        _cts = CancellationTokenSource.CreateLinkedTokenSource(runtimeCancellation);
        _runner = Task.Run(() => RunAsync(_cts.Token));
    }

    public Uri Address { get; }

    public IHandlerPipeline Pipeline => _receiver.Pipeline;

    /// <summary>Resolved (handled / dead-lettered / discarded) — advance the replay watermark past it.</summary>
    public ValueTask CompleteAsync(Envelope envelope) => new(_commits.CompleteAsync(envelope.Offset));

    /// <summary>
    /// Requeue requested — Wolverine's default retry vehicle (requeue for the first <c>MaximumAttempts</c>−1
    /// attempts, then <c>MoveToErrorQueue</c>). A Salesforce replay stream has no per-message requeue, so —
    /// mirroring Wolverine's Kafka listener — we re-inject the envelope for an in-memory retry. The event was
    /// <see cref="ReplayCommitTracker.Track"/>ed on first receive and is never re-tracked, so the replay
    /// watermark holds below it until it is finally resolved by <see cref="CompleteAsync"/> (handler success,
    /// or the terminal <c>MoveToErrorQueue</c> which also completes). We neither hold the floor idle nor
    /// discard. See DECISIONS #10.
    /// </summary>
    public ValueTask DeferAsync(Envelope envelope)
    {
        _logger.LogDebug("{Resource}: Requeue (defer) for replayId {ReplayId}, attempt {Attempts}; re-injecting for in-memory retry.",
            _resource, envelope.Offset, envelope.Attempts);
        return _receiver.ReceivedAsync(this, envelope);
    }

    public async ValueTask StopAsync()
    {
        // Persist the final committable replay position (the commit throttle may be holding one) BEFORE
        // cancelling. MES commits on the gRPC stream, which cancellation aborts — so the flush must run while
        // the transport is still live; the MES transport serializes this write against any in-flight fetch.
        // (Topic commits out-of-band to the repository, so ordering doesn't matter for it.) Bounded so a dead
        // stream can't hang shutdown — if it can't commit in time we fall through to cancel and rely on
        // at-least-once (the tail re-delivers on restart).
        try
        {
            await _commits.FlushAsync().WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "{Resource}: Final replay commit failed during stop.", _resource);
        }

        if (!_cts.IsCancellationRequested)
            _cts.Cancel();

        try
        {
            await _runner.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "{Resource}: Listener runner faulted during stop.", _resource);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _cts.Dispose();
    }

    private async Task RunAsync(CancellationToken ct)
    {
        _logger.LogInformation("{Resource}: Starting Salesforce listener.", _resource);

        // Observability sidecars (DECISIONS #15): a periodic heartbeat plus a silent-cold watchdog that
        // surfaces a stream receiving nothing at an alertable level. They run on their own linked token so
        // that if the consume loop ever faults, the sidecars stop with it — a dead listener must not keep
        // emitting healthy-looking heartbeats.
        using var sidecarCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var heartbeat = _settings.HeartbeatInterval > TimeSpan.Zero
            ? HeartbeatLoopAsync(sidecarCts.Token)
            : Task.CompletedTask;
        var watchdog = _settings.StaleStreamThreshold > TimeSpan.Zero
            ? WatchdogLoopAsync(sidecarCts.Token)
            : Task.CompletedTask;

        try
        {
            await PrewarmSchemasAsync(ct).ConfigureAwait(false);

            while (!ct.IsCancellationRequested)
            {
                // Cold start → null → the transport reads the durable store. Reconnect → the in-memory handled
                // watermark, so we resume after what was handled rather than the last durably-committed position.
                var resumeFrom = _commits.TryGetResumePosition();
                if (resumeFrom is { } r)
                {
                    // Topic feeds the watermark back into the fetch (see #8); MES ignores it and resumes from the
                    // server-side commit checkpoint — so don't claim an in-memory resume it doesn't perform.
                    if (_resumesFromWatermark)
                        _logger.LogDebug("{Resource}: Reconnecting listener; resuming after handled replayId {ReplayId} (in-memory).", _resource, r);
                    else
                        _logger.LogDebug("{Resource}: Reconnecting listener; resuming from the server-side commit checkpoint (in-memory handled watermark {ReplayId} is not used for MES).", _resource, r);
                }

                using var transport = _transportFactory(resumeFrom);
                _currentTransport = transport;
                try
                {
                    await transport.ConnectAsync(ct).ConfigureAwait(false);
                    await ProcessStreamAsync(transport, ct).ConfigureAwait(false);
                }
                catch (Exception) when (ct.IsCancellationRequested)
                {
                    // shutdown
                }
                catch (Exception ex)
                {
                    await HandleStreamExceptionAsync(transport, ex, ct).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // Should be unreachable — the loop's error handling is designed to never throw out — but if it
            // is ever breached, say so loudly: Wolverine does not restart a faulted listener, so this
            // endpoint is DOWN until the process restarts.
            _logger.LogCritical(ex,
                "{Resource}: The listener loop faulted and will not restart — this endpoint is no longer consuming events. Heartbeat and watchdog are stopped.",
                _resource);
        }
        finally
        {
            sidecarCts.Cancel();
            await heartbeat.ConfigureAwait(false);
            await watchdog.ConfigureAwait(false);
        }

        _logger.LogInformation("{Resource}: Salesforce listener stopped.", _resource);
    }

    private async Task ProcessStreamAsync(ISubscriptionTransport transport, CancellationToken ct)
    {
        await foreach (var response in WithIdleTimeout(transport.ReadAsync(ct), _settings.FetchTimeout, ct).ConfigureAwait(false))
        {
            OnSuccessfulResponse(response.Events.Count);

            if (response.Events.Count == 0)
            {
                // Keep-alive: advance the committed position during idle (respects any in-flight floor).
                await _commits.ObserveKeepAliveAsync(response.LastReplayId).ConfigureAwait(false);
            }
            else
            {
                foreach (var consumerEvent in response.Events)
                {
                    var replayId = BinaryPrimitives.ReadInt64BigEndian(consumerEvent.ReplayId.ToByteArray());
                    _commits.Track(replayId); // in flight until CompleteAsync resolves it

                    var schemaId = consumerEvent.Event.SchemaId;

                    // Ensure the Avro schema is cached *here in the loop* so an auth failure during the fetch
                    // surfaces to the reconnect/token-invalidate path; the serializer then decodes (sync)
                    // from the cached schema downstream in Wolverine's pipeline.
                    await _schemaRepository.GetDeserializationInfoBySchemaIdAsync(schemaId, ct).ConfigureAwait(false);

                    // Per-event type resolution (DECISIONS #16): the record name in the (now-cached) schema
                    // picks the mapped type. An unmapped event is stamped with its raw record name and rides
                    // Wolverine's missing-handler path (dead-letter + complete → the watermark still advances).
                    string messageTypeName;
                    bool mapped;
                    string recordName;
                    try
                    {
                        (messageTypeName, mapped, recordName) = _eventTypes.Resolve(schemaId);
                    }
                    catch (Exception resolveEx)
                    {
                        // Never wedge: the event is already Tracked, so a resolve failure that threw out of
                        // the loop would pin the replay watermark below it and redeliver it forever. Stamp a
                        // fallback name instead — the envelope rides the same missing-handler path as an
                        // unmapped event (dead-letter + complete), and the watermark advances past it.
                        mapped = false;
                        recordName = $"unresolved-schema:{schemaId}";
                        messageTypeName = recordName;
                        if (_unmappedWarned.Add(recordName))
                            _logger.LogError(resolveEx,
                                "{Resource}: Failed to resolve the event type for schema {SchemaId}; stamping '{Fallback}' and deferring to Wolverine's missing-handler policy (further occurrences will not be logged).",
                                _resource, schemaId, recordName);
                    }

                    if (!mapped && _unmappedWarned.Add(recordName))
                        _logger.LogWarning(
                            "{Resource}: Event type '{RecordName}' has no MapEvent registration; stamping the raw name and deferring to Wolverine's missing-handler policy (further occurrences will not be logged).",
                            _resource, recordName);

                    var envelope = new Envelope
                    {
                        // Deterministic Id from the Salesforce event id so a redelivered event is dedup-able.
                        Id = ResolveEnvelopeId(consumerEvent.Event.Id, _resource, replayId),
                        Data = consumerEvent.Event.Payload.ToByteArray(),
                        ContentType = SalesforceAvroSerializer.SalesforceAvroContentType,
                        MessageType = messageTypeName,
                        TopicName = _resource,
                        Offset = replayId
                    };
                    envelope.Headers[SalesforceAvroSerializer.SchemaIdHeader] = schemaId;
                    // Headers are round-tripped by the durable inbox; Offset is not (see ReplayIdHeader).
                    envelope.Headers[SalesforceAvroSerializer.ReplayIdHeader] = replayId.ToString();

                    await _receiver.ReceivedAsync(this, envelope).ConfigureAwait(false);
                }
            }

            // Replay commit is driven by the watermark (CompleteAsync / keep-alive), not a batch ack.

            if (response.PendingNumberRequested <= 0)
                await transport.RequestMoreAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Eager schema pre-warm (DECISIONS #17): fetch each mapped event's schema once at startup so the
    /// cache is warm before any event — and, more importantly, before Wolverine's durability agent replays
    /// recovered envelopes. Best-effort per entry: a failure logs and falls back to the lazy per-event
    /// fetch; an auth failure will equally hit the subscribe that follows, where the reconnect/invalidate
    /// path owns recovery.
    /// </summary>
    private async Task PrewarmSchemasAsync(CancellationToken ct)
    {
        foreach (var topic in _prewarmTopics)
        {
            if (ct.IsCancellationRequested)
                return;

            try
            {
                var info = await _schemaRepository.PrewarmByTopicNameAsync(topic, ct).ConfigureAwait(false);
                _logger.LogDebug("{Resource}: Pre-warmed schema {SchemaId} for {Topic}.", _resource, info.SchemaId, topic);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "{Resource}: Schema pre-warm failed for {Topic}; the schema will be fetched lazily on the first event.", _resource, topic);
            }
        }
    }

    /// <summary>
    /// A response (event batch or keep-alive) arrived: the stream is healthy. Record it in the diagnostics
    /// and, if this ends an error streak, log a single recovery line with the observed downtime.
    /// </summary>
    private void OnSuccessfulResponse(int eventCount)
    {
        if (_diagnostics.RecordSuccess(eventCount, DateTimeOffset.UtcNow) is { } recovery)
        {
            _logger.LogInformation(
                "{Resource}: Stream recovered after {ConsecutiveErrors} consecutive error(s); ~{Downtime} since last successful response.",
                _resource, recovery.ConsecutiveErrors, recovery.Downtime);
        }
    }

    /// <summary>
    /// Periodic health snapshot so a steadily-running listener is visibly alive in the logs (ported from
    /// the original orchestrator's HeartbeatAsync — DECISIONS #15). Never throws out.
    /// </summary>
    private async Task HeartbeatLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_settings.HeartbeatInterval);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
                    return;

                var s = _diagnostics.Snapshot();
                _logger.Log(_settings.HeartbeatLogLevel,
                    "{Resource}: Heartbeat - Running since {Start}, Responses: {Responses}, Events: {Events}, Errors: {Errors}, Reconnects: {Reconnects}, LastSuccess: {LastSuccess}, LastError: {LastError}",
                    _resource, s.StartedOn, s.Responses, s.Events, s.Errors, s.Reconnects, s.LastSuccess, s.LastError);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{Resource}: Heartbeat failed.", _resource);
            }
        }
    }

    /// <summary>
    /// Silent-cold watchdog (ported from the original orchestrator's MonitorAsync — DECISIONS #15): a
    /// stream can be connected yet delivering nothing, which the consume loop's own error logging never
    /// surfaces. Once nothing has succeeded for the stale threshold, log at the alertable level on every
    /// poll until the stream recovers. Never throws out.
    /// </summary>
    private async Task WatchdogLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_settings.WatchdogPollingPeriod);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
                    return;

                if (_diagnostics.CheckStale(_settings.StaleStreamThreshold, DateTimeOffset.UtcNow, out var sinceLastSuccess))
                {
                    _logger.Log(_settings.StaleStreamLogLevel,
                        "{Resource}: Has not received a response in {Duration}",
                        _resource, sinceLastSuccess);
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{Resource}: Stale-stream watchdog failed.", _resource);
            }
        }
    }

    /// <summary>
    /// Commits a replay position to whichever transport is currently connected (the MES stream rotates on
    /// reconnect; the topic repository is long-lived). Best-effort — a commit racing a reconnect is fine.
    /// </summary>
    private async Task CommitToCurrentTransportAsync(long replayId, bool isKeepAlive)
    {
        var transport = _currentTransport;
        if (transport is null)
            return;

        try
        {
            await transport.CommitAsync(replayId, isKeepAlive, CancellationToken.None).ConfigureAwait(false);
            _logger.LogDebug("{Resource}: Committed replay position {ReplayId} (keepAlive: {IsKeepAlive}).",
                _resource, replayId, isKeepAlive);
        }
        catch (Exception ex)
        {
            // The position is re-committed on the next event, or re-derived server-side on reconnect.
            _logger.LogDebug(ex, "{Resource}: Replay commit failed (replayId {ReplayId}); will retry on next commit.", _resource, replayId);
        }
    }

    /// <summary>
    /// Maps the Salesforce event id to a stable <see cref="Envelope.Id"/> so a redelivered event always
    /// yields the same Id (enables inbox dedup). The SF event id is normally a guid; when it isn't, derive
    /// a deterministic guid — falling back to resource+replayId when no id is present.
    /// </summary>
    internal static Guid ResolveEnvelopeId(string? salesforceEventId, string resource, long replayId)
    {
        if (Guid.TryParse(salesforceEventId, out var parsed))
            return parsed;

        var key = string.IsNullOrEmpty(salesforceEventId) ? $"{resource}:{replayId}" : salesforceEventId;
        return new Guid(MD5.HashData(Encoding.UTF8.GetBytes(key)));
    }

    private async Task HandleStreamExceptionAsync(ISubscriptionTransport transport, Exception ex, CancellationToken ct)
    {
        var (consecutiveErrors, sinceLastSuccess) = _diagnostics.RecordError(DateTimeOffset.UtcNow);

        // An auth failure means the token was rejected (expired or revoked). Drop the cached token so the
        // reconnect below fetches a fresh one — gated on the auth status codes so ordinary reconnects keep it.
        if (ex is RpcException rpcEx && SalesforceAuthErrors.IsAuthRejection(rpcEx))
        {
            _logger.LogInformation("{Resource}: Authentication failure; invalidating cached token before reconnect.", _resource);
            _tokenProvider.Invalidate();
        }

        try
        {
            await transport.HandleErrorAsync(ct).ConfigureAwait(false);
        }
        catch (Exception inner)
        {
            _logger.LogError(inner, "{Resource}: Transport error handling failed.", _resource);
        }

        // Past the stale threshold this is no longer a routine reconnect — escalate to the alertable level
        // so the actual exception rides along with the watchdog's duration-only line.
        var level = _settings.StaleStreamThreshold > TimeSpan.Zero && sinceLastSuccess >= _settings.StaleStreamThreshold
            ? _settings.StaleStreamLogLevel
            : LogLevel.Warning;

        _logger.Log(level, ex is TimeoutException ? null : ex,
            "{Resource}: {ExceptionType}, ConsecutiveErrors: {ConsecutiveErrors}, SinceLastSuccess: {SinceLastSuccess}",
            _resource, ex.GetType().Name, consecutiveErrors, sinceLastSuccess);

        try
        {
            await _backoffStrategy.BackoffAsync(consecutiveErrors, sinceLastSuccess, _resource, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception backoffEx)
        {
            // A consumer-implemented strategy must not be able to fault the reconnect loop (Wolverine never
            // restarts a faulted listener). Fall back to the default increment so a permanently-throwing
            // strategy degrades to a paced retry rather than a hot loop.
            _logger.LogWarning(backoffEx,
                "{Resource}: IBackoffStrategy threw; falling back to a {Delay} delay before reconnecting.",
                _resource, FallbackBackoffDelay);
            try
            {
                await Task.Delay(FallbackBackoffDelay, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    /// <summary>Reconnect pacing used when the consumer's <see cref="IBackoffStrategy"/> itself fails.</summary>
    private static readonly TimeSpan FallbackBackoffDelay = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Wraps the transport's read sequence with an idle timeout. If no item arrives within
    /// <paramref name="idleTimeout"/>, throws <see cref="TimeoutException"/> to trigger a reconnect.
    /// Uses a linked CTS to cleanly cancel any in-flight MoveNextAsync before disposing.
    /// </summary>
    private async IAsyncEnumerable<ResponseMessageInfo> WithIdleTimeout(
        IAsyncEnumerable<ResponseMessageInfo> source,
        TimeSpan idleTimeout,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var enumerator = source.GetAsyncEnumerator(linkedCts.Token);

        try
        {
            while (true)
            {
                var moveNext = enumerator.MoveNextAsync().AsTask();
                var timeout = Task.Delay(idleTimeout, ct);

                var completed = await Task.WhenAny(moveNext, timeout).ConfigureAwait(false);

                if (ct.IsCancellationRequested)
                {
                    try { await moveNext.ConfigureAwait(false); }
                    catch (Exception ex) { _logger.LogTrace(ex, "{Resource}: Swallowed exception draining MoveNext on shutdown.", _resource); }
                    yield break;
                }

                if (completed == timeout)
                {
                    linkedCts.Cancel();
                    try { await moveNext.ConfigureAwait(false); }
                    catch (Exception ex) { _logger.LogTrace(ex, "{Resource}: Swallowed exception draining MoveNext on timeout.", _resource); }
                    throw new TimeoutException($"No response received within {idleTimeout}.");
                }

                if (!await moveNext.ConfigureAwait(false))
                    yield break;

                yield return enumerator.Current;
            }
        }
        finally
        {
            try
            {
                linkedCts.Cancel();
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "{Resource}: Stream enumerator disposal failed, possible resource leak.", _resource);
            }
        }
    }
}
