using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Wolverine.Runtime;
using Wolverine.Transports;

namespace Wolverine.SalesforcePubSub.Internals;

/// <summary>
/// Wolverine listener over a Salesforce Pub/Sub subscription. Owns the connect/process/backoff/reconnect
/// loop (ported from the original SubscriptionOrchestrator) so Wolverine never has to restart a faulted
/// stream — the loop self-heals internally and only stops on shutdown.
///
/// FIRST CUT — replay/ack seam: replay is committed in-loop via <see cref="ISubscriptionTransport.AcknowledgeAsync"/>
/// after the batch is dispatched (and, in Inline mode, processed). This is the agreed at-most-once
/// "free" behavior. The at-least-once refinement — commit per-envelope in <see cref="CompleteAsync"/>
/// after the handler succeeds, a dedicated keepalive-advance path, and a handler-failure policy — is the
/// next iteration. <see cref="CompleteAsync"/>/<see cref="DeferAsync"/> are intentionally no-ops for now.
/// </summary>
internal sealed class SalesforceListener : IListener
{
    private readonly string _resource;
    private readonly Func<ISubscriptionTransport> _transportFactory;
    private readonly IReceiver _receiver;
    private readonly Type _messageType;
    private readonly PlatformEventDeserializer _deserializer;
    private readonly SubscriberComponentsSettings _settings;
    private readonly IBackoffStrategy _backoffStrategy;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts;
    private readonly Task _runner;

    private long _consecutiveErrors;

    public SalesforceListener(
        Uri address,
        string resource,
        Func<ISubscriptionTransport> transportFactory,
        IReceiver receiver,
        Type messageType,
        PlatformEventDeserializer deserializer,
        SubscriberComponentsSettings settings,
        IBackoffStrategy backoffStrategy,
        ILogger logger,
        CancellationToken runtimeCancellation)
    {
        Address = address;
        _resource = resource;
        _transportFactory = transportFactory;
        _receiver = receiver;
        _messageType = messageType;
        _deserializer = deserializer;
        _settings = settings;
        _backoffStrategy = backoffStrategy;
        _logger = logger;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(runtimeCancellation);
        _runner = Task.Run(() => RunAsync(_cts.Token));
    }

    public Uri Address { get; }

    public IHandlerPipeline Pipeline => _receiver.Pipeline;

    // See class remarks: replay is committed in the consume loop for this first cut.
    public ValueTask CompleteAsync(Envelope envelope) => ValueTask.CompletedTask;

    public ValueTask DeferAsync(Envelope envelope) => ValueTask.CompletedTask;

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
            using var transport = _transportFactory();
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
            _consecutiveErrors = 0;

            foreach (var consumerEvent in response.Events)
            {
                var replayId = BinaryPrimitives.ReadInt64BigEndian(consumerEvent.ReplayId.ToByteArray());
                var eventMessage = new EventMessage(_resource, replayId, consumerEvent);
                var deserialized = await _deserializer.DeserializeAsync(eventMessage, _messageType, ct).ConfigureAwait(false);
                deserialized.ReplayId = replayId;

                var envelope = new Envelope
                {
                    Message = deserialized,
                    TopicName = _resource,
                    Offset = replayId
                };

                await _receiver.ReceivedAsync(this, envelope).ConfigureAwait(false);
            }

            await transport.AcknowledgeAsync(response, ct).ConfigureAwait(false);

            if (response.PendingNumberRequested <= 0)
                await transport.RequestMoreAsync(ct).ConfigureAwait(false);
        }
    }

    private async Task HandleStreamExceptionAsync(ISubscriptionTransport transport, Exception ex, CancellationToken ct)
    {
        _consecutiveErrors++;

        try
        {
            await transport.HandleErrorAsync(ct).ConfigureAwait(false);
        }
        catch (Exception inner)
        {
            _logger.LogError(inner, "Transport error handling failed for resource: {Resource}", _resource);
        }

        _logger.Log(LogLevel.Warning, ex is TimeoutException ? null : ex,
            "{ExceptionType} in resource: {Resource}, ConsecutiveErrors: {ConsecutiveErrors}",
            ex.GetType().Name, _resource, _consecutiveErrors);

        try
        {
            await _backoffStrategy.BackoffAsync(_consecutiveErrors, TimeSpan.Zero, _resource, ct).ConfigureAwait(false);
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
