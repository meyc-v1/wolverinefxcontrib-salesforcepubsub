using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using WolverineFxContrib.SalesforcePubSub.TestHost.Salesforce;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// Console trigger for manual verification: press [1] to publish Test Event One, [2] for Test Event Two,
/// via the lifted <see cref="ISalesforceClient"/>. Skipped when console input is redirected.
/// </summary>
public sealed class PublisherWorker(IServiceProvider services, ILogger<PublisherWorker> logger) : BackgroundService
{
    private int _count;

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

            if (key.Key == ConsoleKey.Q)
            {
                logger.LogInformation("Publisher key triggers stopped.");
                return;
            }

            var isOne = key.Key is ConsoleKey.D1 or ConsoleKey.NumPad1;
            var isTwo = key.Key is ConsoleKey.D2 or ConsoleKey.NumPad2;
            if (!isOne && !isTwo)
                continue;

            var message = $"msg-{++_count}-{Guid.NewGuid():N}";
            try
            {
                var client = services.GetRequiredService<ISalesforceClient>();
                if (isOne)
                    await client.SendPlatformTestEventOneAsync(message, stoppingToken).ConfigureAwait(false);
                else
                    await client.SendPlatformTestEventTwoAsync(message, stoppingToken).ConfigureAwait(false);

                logger.LogInformation("Published {Event}: {Message}", isOne ? "Test Event One" : "Test Event Two", message);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to publish platform event");
            }
        }
    }
}
