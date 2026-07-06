using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Salesforce;
using TestHost.Settings;

namespace TestHost;

/// <summary>
/// Timed test publisher (revived from the original runner's RandomWriterService). On an interval it POSTs
/// every configured platform event via <see cref="ISalesforceClient"/>, so a resiliency run has steady
/// traffic on every subscription kind. Opt-in via <see cref="PublisherSettings"/>.
/// </summary>
public sealed class PublisherWorker(
    IServiceProvider services,
    IOptions<PublisherSettings> options,
    RunMetrics metrics,
    ILogger<PublisherWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;

        if (!settings.Enabled || settings.Events.Count == 0)
        {
            logger.LogInformation("Publisher disabled (or no events configured); host is listen-only.");
            return;
        }

        var interval = TimeSpan.FromSeconds(Math.Max(1, settings.IntervalSeconds));
        logger.LogInformation("Publisher started: every {Interval}, events [{Events}].",
            interval, string.Join(", ", settings.Events));

        var count = 0;
        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                // Every configured event publishes each tick, so multi-type endpoints (the channel,
                // the channel MES) see interleaved traffic every interval.
                foreach (var eventName in settings.Events)
                {
                    var message = $"msg-{++count}-{Guid.NewGuid():N}";
                    try
                    {
                        // Resolve per-iteration: the REST client is a typed HttpClient (transient handler chain).
                        var client = services.GetRequiredService<ISalesforceClient>();
                        await client.SendPlatformEventAsync(eventName, message, stoppingToken).ConfigureAwait(false);
                        metrics.RecordPublished(eventName);
                        logger.LogInformation("Published {Event}: {Message}", eventName, message);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        // Terse — during a network-death test this fires every interval; the full stack is noise.
                        logger.LogWarning("Failed to publish {Event}: {Error}", eventName, ex.Message);
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // shutting down
        }
    }
}
