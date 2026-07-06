using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Replay;

/// <summary>
/// Layer-A fault injector for the "corrupt replay id" resiliency test. On the first topic replay read it
/// hands back a bogus id (e.g. 9999) so Salesforce answers <c>InvalidArgument</c> with the corrupted-replay
/// error-code trailer — driving the transport's <c>ResetForNewEventsOnlyAsync</c> path so we can observe the
/// reset-to-Latest recovery. After firing once it behaves as a plain in-memory repository; the reset also
/// disarms it. Self-contained (no persistence) — the bad-replay test is run in isolation.
/// </summary>
internal sealed class FaultInjectingReplayIdRepository : IReplayIdRepository
{
    private const long NewEventsOnly = -1;

    private readonly ConcurrentDictionary<string, long> _replayIds = new();
    private readonly long _badReplayId;
    private readonly ILogger<FaultInjectingReplayIdRepository> _logger;
    private int _fired; // 0 = armed, 1 = already injected

    public FaultInjectingReplayIdRepository(long badReplayId, ILogger<FaultInjectingReplayIdRepository> logger)
    {
        _badReplayId = badReplayId;
        _logger = logger;
    }

    public Task<long> GetLastReplayIdAsync(string topicName, CancellationToken token = default)
    {
        if (Interlocked.CompareExchange(ref _fired, 1, 0) == 0)
        {
            _logger.LogWarning(
                "[fault] Seeding bad replay id {BadReplayId} for {Topic} to force the InvalidArgument reset path.",
                _badReplayId, topicName);
            return Task.FromResult(_badReplayId);
        }

        return Task.FromResult(_replayIds.GetOrAdd(topicName, NewEventsOnly));
    }

    public Task ReportKeepAliveResponseAsync(string topicName, long replayId, CancellationToken token = default)
        => Set(topicName, replayId);

    public Task ReportEventsReceivedResponseAsync(string topicName, long replayId, List<long> replayIdsReceived, CancellationToken token = default)
        => Set(topicName, replayId);

    public Task ResetForNewEventsOnlyAsync(string topicName, CancellationToken token = default)
    {
        Interlocked.Exchange(ref _fired, 1); // reset means the bad id was rejected — stay disarmed
        _logger.LogInformation("[fault] Reset {Topic} to NewEventsOnly after the seeded bad replay id was rejected.", topicName);
        return Set(topicName, NewEventsOnly);
    }

    private Task Set(string topicName, long replayId)
    {
        _replayIds.AddOrUpdate(topicName, replayId, (_, _) => replayId);
        return Task.CompletedTask;
    }
}
