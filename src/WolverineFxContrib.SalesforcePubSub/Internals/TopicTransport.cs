using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using SalesforceGrpc;

namespace Wolverine.SalesforcePubSub.Internals;

internal sealed partial class TopicTransport : ISubscriptionTransport
{
    private readonly PubSub.PubSubClient _client;
    private readonly IReplayIdRepository _replayIdRepository;
    private readonly SubscriberComponentsSettings _settings;
    private readonly ILogger _logger;
    private readonly string _topicName;

    // In-process reconnect resume anchor (the listener's handled watermark). When set, the initial fetch
    // resumes from it instead of re-reading the repository — so a reconnect does not redeliver events that
    // were handled but not yet durably committed. Null on a true cold start (read the repository/SQL).
    private readonly long? _resumeFromReplayId;
    private bool _initialFetchSent;

    private AsyncDuplexStreamingCall<FetchRequest, FetchResponse>? _call;

    public TopicTransport(
        PubSub.PubSubClient client,
        IReplayIdRepository replayIdRepository,
        SubscriberComponentsSettings settings,
        ILogger logger,
        string topicName,
        long? resumeFromReplayId = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _replayIdRepository = replayIdRepository ?? throw new ArgumentNullException(nameof(replayIdRepository));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _topicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
        _resumeFromReplayId = resumeFromReplayId;
    }

    public async Task ConnectAsync(CancellationToken ct)
    {
        LogStarted("OpenStream", _topicName);
        _call = _client.Subscribe(cancellationToken: ct);
        LogFinished("OpenStream", _topicName);

        await WriteFetchRequestAsync(ct).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<ResponseMessageInfo> ReadAsync([EnumeratorCancellation] CancellationToken ct)
    {
        if (_call == null)
            throw new InvalidOperationException("Transport is not connected");

        while (true)
        {
            FetchResponse response;
            try
            {
                LogStarted("MoveNext", _topicName);
                if (!await _call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
                {
                    LogFinished("MoveNext-StreamEnded", _topicName);
                    break;
                }

                response = _call.ResponseStream.Current;
                LogFinished("MoveNext-ResponseReceived", _topicName);
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && ct.IsCancellationRequested)
            {
                LogGrpcStreamCancelled(ex, _topicName);
                break;
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
        LogStarted("SaveReplayId", _topicName);

        // Commit regardless of cancellation so a shutdown still persists progress.
        if (isKeepAlive)
        {
            await _replayIdRepository.ReportKeepAliveResponseAsync(
                _topicName, replayId, CancellationToken.None).ConfigureAwait(false);
        }
        else
        {
            await _replayIdRepository.ReportEventsReceivedResponseAsync(
                _topicName, replayId, [replayId], CancellationToken.None).ConfigureAwait(false);
        }

        LogFinished("SaveReplayId", _topicName);
    }

    public async Task RequestMoreAsync(CancellationToken ct)
    {
        await WriteFetchRequestAsync(ct).ConfigureAwait(false);
    }

    public async Task<string?> HandleErrorAsync(CancellationToken ct)
    {
        if (_call == null)
            return null;

        LogStarted("CompleteStream", _topicName);
        await _call.RequestStream.TryCompleteAsync().ConfigureAwait(false);
        LogFinished("CompleteStream", _topicName);

        _call.TryGetStatus(out var status);

        if (_settings.ProcessNewEventsIfReplayIdValidationFails && _call.TryGetTrailers(out var trailers) && trailers != null)
        {
            if (trailers.Any(x => x.Key == "error-code" && x.Value == _settings.ReplayIdValidationFailedErrorCode))
            {
                LogStarted("ResetReplayId", _topicName);
                await _replayIdRepository.ResetForNewEventsOnlyAsync(_topicName, ct).ConfigureAwait(false);
                LogFinished("ResetReplayId", _topicName);
            }
        }

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

        long replayId;
        if (!_initialFetchSent && _resumeFromReplayId is { } resume)
        {
            // In-process reconnect: resume from the listener's handled watermark, not the (possibly stale)
            // repository — avoids redelivering events already handled since the last durable commit.
            replayId = resume;
        }
        else
        {
            LogStarted("GetReplayId", _topicName);
            replayId = await _replayIdRepository.GetLastReplayIdAsync(_topicName, ct).ConfigureAwait(false);
            LogFinished("GetReplayId", _topicName);
        }

        _initialFetchSent = true;

        var req = new FetchRequest
        {
            TopicName = _topicName,
            NumRequested = _settings.FetchCount
        };

        if (replayId == ReplayIds.NewEventsOnly)
        {
            req.ReplayPreset = _settings.StartFromEarliest ? ReplayPreset.Earliest : ReplayPreset.Latest;
        }
        else
        {
            req.ReplayPreset = ReplayPreset.Custom;
            var converted = BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(replayId) : replayId;
            req.ReplayId = ByteString.CopyFrom(BitConverter.GetBytes(converted));
        }

        LogSendingFetchRequest(_topicName, _settings.FetchCount, replayId);

        LogStarted("WriteToStream", _topicName);
        await _call.RequestStream.WriteAsync(req, ct).ConfigureAwait(false);
        LogFinished("WriteToStream", _topicName);
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Debug, Message = "{Resource}: gRPC stream cancelled.")]
    private partial void LogGrpcStreamCancelled(Exception ex, string resource);

    [LoggerMessage(EventId = 2, Level = LogLevel.Debug, Message = "{Resource}: Sending FetchRequest, FetchCount: {FetchCount}, ReplayId: {ReplayId}")]
    private partial void LogSendingFetchRequest(string resource, int fetchCount, long replayId);

    [LoggerMessage(EventId = 3, Level = LogLevel.Trace, Message = "{Resource}: Started {Operation}")]
    private partial void LogStarted(string operation, string resource);

    [LoggerMessage(EventId = 4, Level = LogLevel.Trace, Message = "{Resource}: Finished {Operation}")]
    private partial void LogFinished(string operation, string resource);
}
