using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TestHost;

/// <summary>
/// Periodically logs a snapshot of <see cref="RunMetrics"/> (published vs handled counts + last replay id per
/// message type). Mirrors the original runner's heartbeat so an operator can watch a long resiliency run and
/// see, at a glance, whether delivery kept up across an induced outage.
/// </summary>
public sealed class HeartbeatService(RunMetrics metrics, ILogger<HeartbeatService> logger) : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(60);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Interval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
                Snapshot();
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }

        Snapshot(); // final snapshot on stop
    }

    private void Snapshot()
    {
        var published = string.Join(", ", metrics.Published.Select(kv => $"{kv.Key}={kv.Value}"));
        var handled = string.Join(", ", metrics.Handled.Select(kv =>
            $"{kv.Key}={kv.Value} (lastReplayId {metrics.LastReplayId.GetValueOrDefault(kv.Key, -1)})"));

        logger.LogInformation("[heartbeat] published: [{Published}] | handled: [{Handled}]",
            string.IsNullOrEmpty(published) ? "none" : published,
            string.IsNullOrEmpty(handled) ? "none" : handled);
    }
}
