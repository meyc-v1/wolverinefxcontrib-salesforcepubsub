using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using SalesforceGrpc;

namespace Wolverine.SalesforcePubSub.Internals;

internal sealed partial class ManagedEventSubscriptionTransport : ISubscriptionTransport
{
    private readonly PubSub.PubSubClient _client;
    private readonly SubscriberComponentsSettings _settings;
    private readonly ILogger _logger;
    private readonly string _subscriptionName;
    private AsyncDuplexStreamingCall<ManagedFetchRequest, ManagedFetchResponse>? _call;

    public ManagedEventSubscriptionTransport(
        PubSub.PubSubClient client,
        SubscriberComponentsSettings settings,
        ILogger logger,
        string subscriptionName)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _subscriptionName = subscriptionName ?? throw new ArgumentNullException(nameof(subscriptionName));
    }

    public async Task ConnectAsync(CancellationToken ct)
    {
        LogStarted("OpenStream", _subscriptionName);
        _call = _client.ManagedSubscribe(cancellationToken: ct);
        LogFinished("OpenStream", _subscriptionName);

        await WriteFetchRequestAsync(ct).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<ResponseMessageInfo> ReadAsync([EnumeratorCancellation] CancellationToken ct)
    {
        if (_call == null)
            throw new InvalidOperationException("Transport is not connected");

        while (true)
        {
            ManagedFetchResponse response;
            try
            {
                LogStarted("MoveNext", _subscriptionName);
                if (!await _call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
                {
                    LogFinished("MoveNext-StreamEnded", _subscriptionName);
                    break;
                }

                response = _call.ResponseStream.Current;
                LogFinished("MoveNext-ResponseReceived", _subscriptionName);
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && ct.IsCancellationRequested)
            {
                LogGrpcStreamCancelled(ex, _subscriptionName);
                break;
            }

            // Commit acknowledgment responses are protocol noise — log and skip.
            if (response.CommitResponse != null)
            {
                var commitReplayId = BinaryPrimitives.ReadInt64BigEndian(response.CommitResponse.ReplayId.ToByteArray());
                LogCommitAcknowledged(_subscriptionName, commitReplayId);

                // If the commit response also carries events (unlikely but not documented as impossible), yield it.
                if (!response.Events.Any())
                    continue;
            }

            yield return new ResponseMessageInfo
            {
                LastReplayIdByteString = response.LatestReplayId,
                LastReplayId = BinaryPrimitives.ReadInt64BigEndian(response.LatestReplayId.ToByteArray()),
                Events = response.Events,
                PendingNumberRequested = response.PendingNumRequested
            };
        }
    }

    public async Task CommitAsync(long replayId, bool isKeepAlive, CancellationToken ct)
    {
        if (_call == null)
            throw new InvalidOperationException("Transport is not connected");

        // MES replay is server-side; the commit is the same whether driven by events or a keep-alive.
        var commit = new ManagedFetchRequest
        {
            DeveloperName = _subscriptionName,
            CommitReplayIdRequest = new CommitReplayRequest { ReplayId = ToReplayIdByteString(replayId) }
        };

        LogCommittingReplayId(replayId, _subscriptionName);

        // Write the commit regardless of cancellation so the server records our position.
        LogStarted("CommitReplayId", _subscriptionName);
        await _call.RequestStream.WriteAsync(commit).ConfigureAwait(false);
        LogFinished("CommitReplayId", _subscriptionName);
    }

    private static ByteString ToReplayIdByteString(long replayId)
    {
        Span<byte> buffer = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, replayId);
        return ByteString.CopyFrom(buffer);
    }

    public async Task RequestMoreAsync(CancellationToken ct)
    {
        await WriteFetchRequestAsync(ct).ConfigureAwait(false);
    }

    public async Task<string?> HandleErrorAsync(CancellationToken ct)
    {
        if (_call == null)
            return null;

        LogStarted("CompleteStream", _subscriptionName);
        await _call.RequestStream.TryCompleteAsync().ConfigureAwait(false);
        LogFinished("CompleteStream", _subscriptionName);

        _call.TryGetStatus(out var status);

        // Managed subscriptions have no replay id validation recovery.
        return status?.ToString();
    }

    public void Dispose()
    {
        _call?.Dispose();
    }

    private async Task WriteFetchRequestAsync(CancellationToken ct)
    {
        if (_call?.RequestStream == null)
            throw new InvalidOperationException("The request stream has not been initialized");

        var req = new ManagedFetchRequest
        {
            DeveloperName = _subscriptionName,
            NumRequested = _settings.FetchCount
        };

        LogSendingManagedFetchRequest(_subscriptionName, _settings.FetchCount);

        LogStarted("WriteToStream", _subscriptionName);
        await _call.RequestStream.WriteAsync(req, ct).ConfigureAwait(false);
        LogFinished("WriteToStream", _subscriptionName);
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Debug, Message = "gRPC stream cancelled for {Resource}")]
    private partial void LogGrpcStreamCancelled(Exception ex, string resource);

    [LoggerMessage(EventId = 2, Level = LogLevel.Trace, Message = "Commit acknowledged for {Resource}, ReplayId: {ReplayId}")]
    private partial void LogCommitAcknowledged(string resource, long replayId);

    [LoggerMessage(EventId = 3, Level = LogLevel.Trace, Message = "Committing ReplayId: {ReplayId} for {Resource}")]
    private partial void LogCommittingReplayId(long replayId, string resource);

    [LoggerMessage(EventId = 4, Level = LogLevel.Debug, Message = "Sending ManagedFetchRequest for {Resource}, FetchCount: {FetchCount}")]
    private partial void LogSendingManagedFetchRequest(string resource, int fetchCount);

    [LoggerMessage(EventId = 5, Level = LogLevel.Trace, Message = "Started {Operation} for {Resource}")]
    private partial void LogStarted(string operation, string resource);

    [LoggerMessage(EventId = 6, Level = LogLevel.Trace, Message = "Finished {Operation} for {Resource}")]
    private partial void LogFinished(string operation, string resource);
}
