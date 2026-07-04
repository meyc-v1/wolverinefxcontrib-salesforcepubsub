using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wolverine;
using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

public static class TestHosts
{
    /// <summary>
    /// The listener subscribes asynchronously after host start; with client-managed replay and no stored
    /// position it subscribes at Latest, so an event published before the subscribe completes is never
    /// delivered. Give the connect/pre-warm/first-fetch a moment before publishing.
    /// </summary>
    public static readonly TimeSpan SubscribeGracePeriod = TimeSpan.FromSeconds(6);

    /// <summary>
    /// Builds and starts a listening host: transport wired with the subscriber ECA, conventional handler
    /// discovery disabled (each test includes exactly the handlers it needs), and the given sink
    /// registered for them. The <paramref name="configure"/> callback declares the endpoints.
    /// </summary>
    public static async Task<IHost> StartListeningAsync(
        SalesforceTestContext ctx, EventSink sink, Action<WolverineOptions> configure)
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.Logging.AddFilter("Wolverine.SalesforcePubSub", LogLevel.Information);

        builder.Services.AddSingleton(sink);
        builder.Services.AddSingleton(ctx.SubscriberCredentials);

        builder.UseWolverine(opts =>
        {
            opts.UseSalesforcePubSub(ctx.PubSubUri)
                .UseAuthenticationHandler<SubscriberTokenHandler>();

            opts.Discovery.DisableConventionalDiscovery();

            configure(opts);
        });

        var host = builder.Build();
        await host.StartAsync();
        await Task.Delay(SubscribeGracePeriod);
        return host;
    }

    public static async Task StopAsync(IHost host)
    {
        await host.StopAsync();
        host.Dispose();
    }
}
