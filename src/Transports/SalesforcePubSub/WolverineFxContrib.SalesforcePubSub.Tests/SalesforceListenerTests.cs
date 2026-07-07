using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Google.Protobuf;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;
using SalesforceGrpc;
using Wolverine;
using Wolverine.Runtime;
using Wolverine.SalesforcePubSub;
using Wolverine.SalesforcePubSub.Events;
using Wolverine.SalesforcePubSub.Internals;
using Wolverine.SalesforcePubSub.Internals.Authentication;
using Wolverine.SalesforcePubSub.Internals.Schema;
using Wolverine.SalesforcePubSub.Internals.Transports;
using Wolverine.Transports;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// Deterministic listener tests over a scripted <see cref="ISubscriptionTransport"/> — no gRPC, no org.
/// The fake receiver hands the test exact control over completion interleavings, which the live suite
/// can only observe probabilistically. The centerpiece is the regression pin for the DECISIONS
/// stale-commit gap: Wolverine's no-drain backpressure path (MarkAsTooBusyAndStopReceivingAsync,
/// traced at V6.12.0) disposes a listener whose receiver queue keeps completing envelopes while a
/// replacement listener commits newer positions — a late stale commit must never regress the row.
/// </summary>
public class SalesforceListenerTests
{
    private const string SchemaJson = """{"name":"WIT_Event_A__e","namespace":"com.sforce.eventbus","type":"record","fields":[]}""";

    public sealed class ListenerTestEvent : PlatformEvent;

    // ---------- fakes ----------

    private sealed class FakeSchemaRepo : ISchemaRepository
    {
        public Task<SchemaInfo> GetDeserializationInfoBySchemaIdAsync(string schemaId, CancellationToken cancellationToken = default)
            => Task.FromResult(new SchemaInfo { SchemaId = schemaId, SchemaJson = SchemaJson });

        public Task<SchemaInfo> GetDeserializationInfoByTopicNameAsync(string topicName, CancellationToken cancellationToken = default)
            => Task.FromResult(new SchemaInfo { SchemaId = "test-schema", SchemaJson = SchemaJson });
    }

    private sealed class FakeTokenHandler : IAuthenticationTokenHandler
    {
        public Task<AuthenticationTokenResponse> GetAuthenticationTokenAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new AuthenticationTokenResponse("token", "https://instance", "tenant"));
    }

    private sealed class NoDelayBackoff : IBackoffStrategy
    {
        public Task BackoffAsync(long consecutiveFailuresWithoutResponse, TimeSpan durationSinceLastSuccessfulResponseOrStart, string resource, CancellationToken token)
            => Task.CompletedTask;
    }

    /// <summary>Records dispatched envelopes; the TEST decides when (and whether) to complete them.</summary>
    private sealed class RecordingReceiver : IReceiver
    {
        public readonly ConcurrentQueue<Envelope> Envelopes = new();

        public ValueTask ReceivedAsync(IListener listener, Envelope envelope)
        {
            Envelopes.Enqueue(envelope);
            return ValueTask.CompletedTask;
        }

        public ValueTask ReceivedAsync(IListener listener, Envelope[] messages)
        {
            foreach (var envelope in messages)
                Envelopes.Enqueue(envelope);
            return ValueTask.CompletedTask;
        }

        public ValueTask DrainAsync() => ValueTask.CompletedTask;

        public IHandlerPipeline Pipeline => null!;

        public void Dispose()
        {
        }
    }

    /// <summary>
    /// Scripted stream: the test writes responses; commits are recorded to a (possibly shared) ledger,
    /// which stands in for the one replay row that successive listener generations write to.
    /// </summary>
    private sealed class ScriptedTransport(List<(long ReplayId, bool KeepAlive)> commits, long? resumeFrom) : ISubscriptionTransport
    {
        private readonly Channel<ResponseMessageInfo> _responses = Channel.CreateUnbounded<ResponseMessageInfo>();

        public long? ResumeFrom { get; } = resumeFrom;

        /// <summary>DECISIONS #23: commits that never complete — the black-holed SQL connection.</summary>
        public bool HangCommits { get; set; }

        public void Yield(ResponseMessageInfo response) => _responses.Writer.TryWrite(response);

        public void EndStream() => _responses.Writer.TryComplete();

        public Task ConnectAsync(CancellationToken ct) => Task.CompletedTask;

        public async IAsyncEnumerable<ResponseMessageInfo> ReadAsync([EnumeratorCancellation] CancellationToken ct)
        {
            await foreach (var response in _responses.Reader.ReadAllAsync(ct))
                yield return response;
        }

        public Task CommitAsync(long replayId, bool isKeepAlive, CancellationToken ct)
        {
            if (HangCommits)
                return new TaskCompletionSource().Task; // half-open TCP: never completes, never throws

            lock (commits)
                commits.Add((replayId, isKeepAlive));
            return Task.CompletedTask;
        }

        public Task RequestMoreAsync(CancellationToken ct) => Task.CompletedTask;

        public Task<string?> HandleErrorAsync(CancellationToken ct) => Task.FromResult<string?>(null);

        public void Dispose()
        {
        }
    }

    // ---------- rig ----------

    private sealed class Rig : IAsyncDisposable
    {
        public readonly List<(long ReplayId, bool KeepAlive)> Commits;
        public readonly List<ScriptedTransport> Transports = [];
        public readonly RecordingReceiver Receiver = new();
        public readonly SalesforceListener Listener;

        public ScriptedTransport Transport => Transports[^1];

        public Rig(bool resumesFromWatermark = true, List<(long, bool)>? sharedCommits = null,
            TimeSpan? repositoryCallTimeout = null)
        {
            Commits = sharedCommits ?? [];
            var settings = new SubscriberComponentsSettings
            {
                FetchCount = 1,                            // commit throttle = every completion
                FetchTimeout = TimeSpan.FromMinutes(10),
                HeartbeatInterval = TimeSpan.Zero,         // sidecars off
                WatchdogThreshold = TimeSpan.Zero,
                RepositoryCallTimeout = repositoryCallTimeout ?? SubscriberComponentsSettings.DefaultRepositoryCallTimeout
            };

            var schemas = new CachingSchemaRepository(new FakeSchemaRepo(), new MemoryCache(new MemoryCacheOptions()));
            var resolver = new EventTypeResolver(
                new Dictionary<string, Type> { ["WIT_Event_A__e"] = typeof(ListenerTestEvent) }, schemas);

            Listener = new SalesforceListener(
                new Uri("sfpubsub://topic/test"),
                "/event/WIT_Event_A__e",
                resumesFromWatermark,
                resumeFrom =>
                {
                    var transport = new ScriptedTransport(Commits, resumeFrom);
                    lock (Transports)
                        Transports.Add(transport);
                    return transport;
                },
                Receiver,
                resolver,
                [],                                        // no prewarm
                schemas,
                settings,
                new NoDelayBackoff(),
                new CachingAuthenticationTokenProvider(new FakeTokenHandler(), settings),
                NullLogger<SalesforceListener>.Instance,
                CancellationToken.None);
            Listener.Start();
        }

        public async Task<Envelope> ReceiveAsync(long replayId)
        {
            // Start() runs the connect loop on a background task; wait for the first transport build.
            await WaitUntilAsync(() => { lock (Transports) return Transports.Count > 0; });

            var before = Receiver.Envelopes.Count;
            Transport.Yield(Batch(replayId));
            await WaitUntilAsync(() => Receiver.Envelopes.Count > before);
            return Receiver.Envelopes.Last(e => e.Offset == replayId);
        }

        public IReadOnlyList<long> CommittedPositions()
        {
            lock (Commits)
                return Commits.Select(c => c.ReplayId).ToList();
        }

        public async ValueTask DisposeAsync() => await Listener.DisposeAsync();
    }

    private static ResponseMessageInfo Batch(params long[] replayIds) => new()
    {
        LastReplayIdByteString = ReplayIdBytes(replayIds.Max()),
        LastReplayId = replayIds.Max(),
        PendingNumberRequested = 100, // suppress RequestMore chatter
        Events = replayIds.Select(id => new ConsumerEvent
        {
            ReplayId = ReplayIdBytes(id),
            Event = new ProducerEvent
            {
                Id = Guid.NewGuid().ToString(),
                SchemaId = "test-schema",
                Payload = ByteString.Empty
            }
        }).ToList()
    };

    private static ResponseMessageInfo KeepAlive(long lastReplayId) => new()
    {
        LastReplayIdByteString = ReplayIdBytes(lastReplayId),
        LastReplayId = lastReplayId,
        PendingNumberRequested = 100,
        Events = []
    };

    private static ByteString ReplayIdBytes(long replayId)
    {
        var buffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, replayId);
        return ByteString.CopyFrom(buffer);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, int timeoutMs = 5000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met in time.");
            await Task.Delay(10);
        }
    }

    // ---------- the regression pins ----------

    [Fact]
    public async Task A_disposed_listener_cannot_regress_a_replacement_listeners_committed_position()
    {
        // Both listener generations write to the same replay row — the shared ledger.
        var row = new List<(long, bool)>();

        // Generation 1 receives an event but is disposed BEFORE the envelope completes: Wolverine's
        // no-drain backpressure path (StopAsync → DisposeAsync with the receiver queue still working).
        var gen1 = new Rig(sharedCommits: row);
        var inflight = await gen1.ReceiveAsync(10);
        await gen1.Listener.DisposeAsync();

        // The replacement listener commits a newer position.
        var gen2 = new Rig(sharedCommits: row);
        var next = await gen2.ReceiveAsync(11);
        await gen2.Listener.CompleteAsync(next);
        await WaitUntilAsync(() => gen1.CommittedPositions().Contains(11));

        // The old receiver queue finally completes the stale envelope on the DISPOSED listener.
        await gen1.Listener.CompleteAsync(inflight);
        await Task.Delay(100, TestContext.Current.CancellationToken); // give a (wrong) late write every chance to land

        var positions = gen1.CommittedPositions();
        Assert.Equal(positions.OrderBy(p => p), positions);       // the row never regressed
        Assert.Equal(11, positions[^1]);                          // the replacement's position stands
        await gen2.DisposeAsync();
    }

    [Fact]
    public async Task A_stopped_but_undisposed_listener_still_commits_drain_window_completions()
    {
        // Wolverine's stop-and-drain path: StopAsync → receiver.DrainAsync → DisposeAsync. Completions
        // that arrive during the drain — after stop, before dispose — must still commit.
        var rig = new Rig();
        var envelope = await rig.ReceiveAsync(10);

        await rig.Listener.StopAsync();
        await rig.Listener.CompleteAsync(envelope);
        await WaitUntilAsync(() => rig.CommittedPositions().Contains(10));

        Assert.Contains(10, rig.CommittedPositions());
        await rig.DisposeAsync();
    }

    [Fact]
    public async Task Concurrent_out_of_order_completions_never_write_a_regressing_commit()
    {
        var rig = new Rig();

        var envelopes = new List<Envelope>();
        for (long id = 1; id <= 20; id++)
            envelopes.Add(await rig.ReceiveAsync(id));

        // Complete in reverse order, all racing through the tracker's write gate at once.
        await Task.WhenAll(envelopes.AsEnumerable().Reverse()
            .Select(e => rig.Listener.CompleteAsync(e).AsTask()));
        await WaitUntilAsync(() => rig.CommittedPositions().Count > 0 && rig.CommittedPositions()[^1] == 20);

        var positions = rig.CommittedPositions();
        Assert.Equal(positions.OrderBy(p => p), positions);       // writer-side monotonicity
        Assert.Equal(20, positions[^1]);
        await rig.DisposeAsync();
    }

    [Fact]
    public async Task The_monotonic_guard_still_allows_the_MES_equal_position_reaffirm()
    {
        // MES (resumesFromWatermark: false) re-sends its LAST committed position on idle keep-alives to
        // reset the server's 1800s no-commit deadline — an equal-value write the guard must not drop.
        var rig = new Rig(resumesFromWatermark: false);

        var envelope = await rig.ReceiveAsync(10);
        await rig.Listener.CompleteAsync(envelope);
        await WaitUntilAsync(() => rig.CommittedPositions().Contains(10));

        rig.Transport.Yield(KeepAlive(10)); // no advance → pure re-affirm of the same position
        await WaitUntilAsync(() => rig.Commits.Count >= 2);

        (long replayId, bool keepAlive)[] commits;
        lock (rig.Commits)
            commits = rig.Commits.ToArray();
        Assert.Equal(10, commits[^1].replayId);
        Assert.True(commits[^1].keepAlive);                       // the re-affirm went through
        await rig.DisposeAsync();
    }

    [Fact]
    public async Task A_hanging_repository_commit_cannot_wedge_the_read_loop_or_the_completion_path()
    {
        // DECISIONS #23, reproduced from the 13.6h soak: a VPN drop black-holed the SQL replay store's
        // pooled connections — commits hung 34-57+ minutes without throwing. The commit was awaited on
        // the response path, so the read loop went deaf (no fetches, no keep-alives, no reconnect —
        // nothing threw, and the idle wrapper guards only MoveNext). Liveness must never depend on a
        // consumer repository being prompt.
        var rig = new Rig(repositoryCallTimeout: TimeSpan.FromMilliseconds(300));
        var first = await rig.ReceiveAsync(10);
        rig.Transport.HangCommits = true;

        // Completion must return promptly even though its commit write hangs forever.
        await rig.Listener.CompleteAsync(first).AsTask().WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        // A keep-alive routes another (hung) commit through the read loop itself...
        rig.Transport.Yield(KeepAlive(11));

        // ...and the loop must still be alive to receive and dispatch the next event.
        var second = await rig.ReceiveAsync(12);
        Assert.Equal(12, second.Offset);

        // And the writer must RECOVER: the RepositoryCallTimeout abandons the hung write, so once the
        // repository heals, the next commit lands (the soak's designed retry path, now time-bounded).
        rig.Transport.HangCommits = false;
        await rig.Listener.CompleteAsync(second);
        await WaitUntilAsync(() => rig.CommittedPositions().Contains(12));

        await rig.DisposeAsync(); // shutdown stays bounded even with a write still hung
    }

    [Fact]
    public async Task An_in_process_reconnect_resumes_from_the_handled_watermark()
    {
        var rig = new Rig();
        var envelope = await rig.ReceiveAsync(10);
        await rig.Listener.CompleteAsync(envelope);

        rig.Transport.EndStream(); // stream ends → the loop builds a fresh transport
        await WaitUntilAsync(() => rig.Transports.Count == 2);

        Assert.Null(rig.Transports[0].ResumeFrom);                // cold start read the durable store
        Assert.Equal(10, rig.Transports[1].ResumeFrom);           // reconnect resumed after the handled tail
        await rig.DisposeAsync();
    }
}
