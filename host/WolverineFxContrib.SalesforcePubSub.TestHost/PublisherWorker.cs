using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// Console trigger for manual verification: press the number key for a configured subscription to
/// publish its platform event. Skipped automatically when console input is redirected.
/// </summary>
public sealed class PublisherWorker(IServiceProvider services, SalesforceTestHostOptions options, ILogger<PublisherWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (Console.IsInputRedirected)
        {
            logger.LogInformation("Console input is redirected; publisher key triggers are disabled.");
            return;
        }

        var subs = options.Subscriptions;
        if (subs.Count == 0)
        {
            logger.LogInformation("No subscriptions configured; nothing to publish.");
            return;
        }

        for (var i = 0; i < subs.Count; i++)
            logger.LogInformation("Press [{Key}] to publish {Type} → {Channel}", i + 1, subs[i].MessageType, subs[i].Channel);
        logger.LogInformation("Press [Q] to stop publishing.");

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

            if (!char.IsDigit(key.KeyChar))
                continue;

            var index = key.KeyChar - '1';
            if (index < 0 || index >= subs.Count)
                continue;

            var sub = subs[index];
            var sObject = sub.PublishSObject;
            if (sObject is null)
            {
                logger.LogWarning("No sObject available for '{Channel}'; set its SObject to publish.", sub.Channel);
                continue;
            }

            try
            {
                var publisher = services.GetRequiredService<PlatformEventPublisher>();
                await publisher.PublishAsync(sObject, new { }, stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to publish platform event to {SObject}", sObject);
            }
        }
    }
}
