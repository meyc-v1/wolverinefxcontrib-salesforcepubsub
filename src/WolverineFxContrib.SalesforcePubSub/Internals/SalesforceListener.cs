using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Wolverine.Runtime;
using Wolverine.Transports;
using Wolverine.Util;

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
    private readonly Func<long?, ISubscriptionTransport> _transportFactory;
    private readonly IReceiver _receiver;
    private readonly Type _messageType;
    private readonly CachingSchemaRepository _schemaRepository;
    private readonly SubscriberComponentsSettings _settings;
    private readonly IBackoffStrategy _backoffStrategy;
    private readonly CachingAuthenticationTokenProvider _tokenProvider;
    private readonly ReplayCommitTracker _commits;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;
    private readonly Task _runner;

    private long _consecutiveErrors;
    private DateTimeOffset _lastSuccessUtc = DateTimeOffset.UtcNow;
    private ISubscriptionTransport? _currentTransport;

    public SalesforceListener(
        Uri address,
        string resource,
        Func<long?, ISubscriptionTransport> transportFactory,
        IReceiver receiver,
        Type messageType,
        CachingSchemaRepository schemaRepository,
        SubscriberComponentsSettings settings,
        IBackoffStrategy backoffStrategy,
        CachingAuthenticationTokenProvider tokenProvider,
        ILogger<SalesforceListener> logger,
        CancellationToken runtimeCancellation)
    {
        Address = address;
        _resource = resource;
        _transportFactory = transportFactory;
        _receiver = receiver;
        _messageType = messageType;
        _schemaRepository = schemaRepository;
        _settings = settings;
        _backoffStrategy = backoffStrategy;
        _tokenProvider = tokenProvider;
        _logger = logger;
        _commits = new ReplayCommitTracker(CommitToCurrentTransportAsync, settings.FetchCount);
        _cts = CancellationTokenSource.CreateLinkedTokenSource(runtimeCancellation);
        _runner = Task.Run(() => RunAsync(_cts.Token));
    }

    public Uri Address { get; }

    public IHandlerPipeline Pipeline => _receiver.Pipeline;

    /// <summary>Resolved (handled / dead-lettered / discarded) — advance the replay watermark past it.</summary>
    public ValueTask CompleteAsync(Envelope envelope) => new(_commits.CompleteAsync(envelope.Offset));

    /// <summary>
    /// Requeue requested. Leave the event in flight so the watermark holds below it; it re-delivers on the
    /// next reconnect. No native per-message requeue here — inline-retry + DLQ are the failure model (#2).
    /// </summary>
    public ValueTask DeferAsync(Envelope envelope)
    {
        _logger.LogDebug("Defer requested for {Resource} (replayId {ReplayId}); holding replay position.", _resource, envelope.Offset);
        return ValueTask.CompletedTask;
    }

    public async ValueTask StopAsync()
    {
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
            _logger.LogDebug(ex, "Listener runner faulted during stop for {Resource}", _resource);
        }

        // Persist the final committable replay position (the commit throttle may be holding one).
        try
        {
            await _commits.FlushAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Final replay commit failed during stop for {Resource}", _resource);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _cts.Dispose();
    }

    private async Task RunAsync(CancellationToken ct)
    {
        _logger.LogInformation("Starting Salesforce listener for resource: {Resource}", _resource);

        while (!ct.IsCancellationRequested)
        {
            // Cold start → null → the transport reads the durable store. Reconnect → the in-memory handled
            // watermark, so we resume after what was handled rather than the last durably-committed position.
            var resumeFrom = _commits.TryGetResumePosition();
            if (resumeFrom is { } r)
                _logger.LogDebug("Reconnecting listener for {Resource}; resuming after handled replayId {ReplayId} (in-memory).", _resource, r);

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

        _logger.LogInformation("Salesforce listener stopped for resource: {Resource}", _resource);
    }

    private async Task ProcessStreamAsync(ISubscriptionTransport transport, CancellationToken ct)
    {
        await foreach (var response in WithIdleTimeout(transport.ReadAsync(ct), _settings.FetchTimeout, ct).ConfigureAwait(false))
        {
            OnSuccessfulResponse();

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

                    var envelope = new Envelope
                    {
                        // Deterministic Id from the Salesforce event id so a redelivered event is dedup-able.
                        Id = ResolveEnvelopeId(consumerEvent.Event.Id, _resource, replayId),
                        Data = consumerEvent.Event.Payload.ToByteArray(),
                        ContentType = SalesforceAvroSerializer.SalesforceAvroContentType,
                        MessageType = _messageType.ToMessageTypeName(),
                        TopicName = _resource,
                        Offset = replayId
                    };
                    envelope.Headers[SalesforceAvroSerializer.SchemaIdHeader] = schemaId;

                    await _receiver.ReceivedAsync(this, envelope).ConfigureAwait(false);
                }
            }

            // Replay commit is driven by the watermark (CompleteAsync / keep-alive), not a batch ack.

            if (response.PendingNumberRequested <= 0)
                await transport.RequestMoreAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A response (event batch or keep-alive) arrived: the stream is healthy. Stamp the last-success time
    /// and, if we were recovering from errors, log a single recovery line with the observed downtime.
    /// </summary>
    private void OnSuccessfulResponse()
    {
        if (_consecutiveErrors > 0)
        {
            var downtime = DateTimeOffset.UtcNow - _lastSuccessUtc;
            _logger.LogInformation(
                "Stream recovered for {Resource} after {ConsecutiveErrors} consecutive error(s); ~{Downtime} since last successful response.",
                _resource, _consecutiveErrors, downtime);
            _consecutiveErrors = 0;
        }

        _lastSuccessUtc = DateTimeOffset.UtcNow;
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
            _logger.LogDebug("Committed replay position {ReplayId} for {Resource} (keepAlive: {IsKeepAlive}).",
                replayId, _resource, isKeepAlive);
        }
        catch (Exception ex)
        {
            // The position is re-committed on the next event, or re-derived server-side on reconnect.
            _logger.LogDebug(ex, "Replay commit failed for {Resource} (replayId {ReplayId}); will retry on next commit.", _resource, replayId);
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
        _consecutiveErrors++;

        // An auth failure means the token was rejected (expired or revoked). Drop the cached token so the
        // reconnect below fetches a fresh one — gated on the auth status codes so ordinary reconnects keep it.
        if (ex is RpcException { StatusCode: StatusCode.Unauthenticated or StatusCode.PermissionDenied })
        {
            _logger.LogInformation("Authentication failure for {Resource}; invalidating cached token before reconnect.", _resource);
            _tokenProvider.Invalidate();
        }

        try
        {
            await transport.HandleErrorAsync(ct).ConfigureAwait(false);
        }
        catch (Exception inner)
        {
            _logger.LogError(inner, "Transport error handling failed for resource: {Resource}", _resource);
        }

        var sinceLastSuccess = DateTimeOffset.UtcNow - _lastSuccessUtc;

        _logger.Log(LogLevel.Warning, ex is TimeoutException ? null : ex,
            "{ExceptionType} in resource: {Resource}, ConsecutiveErrors: {ConsecutiveErrors}, SinceLastSuccess: {SinceLastSuccess}",
            ex.GetType().Name, _resource, _consecutiveErrors, sinceLastSuccess);

        try
        {
            await _backoffStrategy.BackoffAsync(_consecutiveErrors, sinceLastSuccess, _resource, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

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
                    catch (Exception ex) { _logger.LogTrace(ex, "Swallowed exception draining MoveNext on shutdown for {Resource}", _resource); }
                    yield break;
                }

                if (completed == timeout)
                {
                    linkedCts.Cancel();
                    try { await moveNext.ConfigureAwait(false); }
                    catch (Exception ex) { _logger.LogTrace(ex, "Swallowed exception draining MoveNext on timeout for {Resource}", _resource); }
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
                _logger.LogWarning(ex, "Stream enumerator disposal failed for {Resource}, possible resource leak", _resource);
            }
        }
    }
}
