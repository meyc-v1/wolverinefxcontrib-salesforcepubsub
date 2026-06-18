using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// Console trigger for manual verification: press [1] to publish Test Event One, [2] for Test Event Two.
/// Skipped automatically when console input is redirected (e.g. running detached).
/// </summary>
public sealed class PublisherWorker(IServiceProvider services, ILogger<PublisherWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (Console.IsInputRedirected)
        {
            logger.LogInformation("Console input is redirected; publisher key triggers are disabled.");
            return;
        }

        logger.LogInformation("Publisher ready. Press [1] = Test Event One, [2] = Test Event Two, [Q] = stop publishing.");

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsoleKeyInfo key;
            try
            {
                key = await Task.Run(() => Console.ReadKey(intercept: true), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            var publisher = services.GetRequiredService<PlatformEventPublisher>();

            try
            {
                switch (key.Key)
                {
                    case ConsoleKey.D1 or ConsoleKey.NumPad1:
                        await publisher.PublishTestEventOneAsync(stoppingToken).ConfigureAwait(false);
                        break;
                    case ConsoleKey.D2 or ConsoleKey.NumPad2:
                        await publisher.PublishTestEventTwoAsync(stoppingToken).ConfigureAwait(false);
                        break;
                    case ConsoleKey.Q:
                        logger.LogInformation("Publisher key triggers stopped.");
                        return;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to publish platform event");
            }
        }
    }
}
