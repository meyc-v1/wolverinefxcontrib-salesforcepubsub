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
    /// delivered — and can never be recovered by waiting. Fallback pacing for hosts that opt out of the
    /// sentinel (<c>readyEvents: 0</c>, i.e. the MES facts, whose slot-held skip wraps their own wait).
    /// </summary>
    public static readonly TimeSpan SubscribeGracePeriod = TimeSpan.FromSeconds(6);

    /// <summary>
    /// Overall budget for proving readiness. Generous because StartFromEarliest hosts replay the
    /// retained history before reaching the sentinels at the tip.
    /// </summary>
    public static readonly TimeSpan ReadyTimeout = TimeSpan.FromSeconds(90);

    /// <summary>Per-sentinel wait before concluding an endpoint missed it and publishing a fresh one.</summary>
    public static readonly TimeSpan ReadyAttemptWindow = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Builds and starts a listening host: transport wired with the subscriber ECA, conventional handler
    /// discovery disabled (each test includes exactly the handlers it needs), and the given sink
    /// registered for them. The <paramref name="configure"/> callback declares the endpoints.
    ///
    /// <para>Readiness is proven, not assumed: after start, a <c>ready-</c> sentinel event is published
    /// and the call returns only once the sink has seen it <paramref name="readyEvents"/> times — the
    /// subscription is demonstrably live before the test publishes anything it must not miss (a fixed
    /// grace period raced subscribe-to-live latency and lost intermittently on cold environments).
    /// Pass the number of endpoints that will deliver the sentinel (2 for an IdAndDestination fan-out
    /// pair; 1 covers an IdOnly pair, which dedups the sentinel), or 0 to fall back to
    /// <see cref="SubscribeGracePeriod"/> for hosts whose facts manage their own first wait (MES).</para>
    ///
    /// <para>Two contract points, both learned the hard way: <paramref name="readyEventName"/> must be
    /// an event the host actually listens to (a WIT_Event_B__e-only host never sees an A sentinel), and
    /// the handler for it must RECORD <c>ready-</c> messages to the sink — a specialized handler that
    /// filters them out makes readiness unprovable and the host fail every run.</para>
    /// </summary>
    public static async Task<IHost> StartListeningAsync(
        SalesforceTestContext ctx, EventSink sink, Action<WolverineOptions> configure,
        Action<IServiceCollection>? configureServices = null, LogSink? logSink = null,
        int readyEvents = 1, string readyEventName = "WIT_Event_A__e")
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        // The transport's Trace instrumentation exists precisely to show where a stream is in its
        // lifecycle (fetch written / response received / dispatch); Wolverine at Debug adds dispatch +
        // codegen activity. A stalled host's failure tail then reads as a timeline instead of a blank.
        builder.Logging.AddFilter("Wolverine.SalesforcePubSub", LogLevel.Trace);
        builder.Logging.AddFilter("Wolverine", LogLevel.Debug);

        // Always capture the host's log: readiness failures attach the tail, since a listener that
        // receives nothing reports why only here (the reconnect loop never throws out).
        var diagnostics = logSink ?? new LogSink();
        builder.Logging.AddProvider(new SinkLoggerProvider(diagnostics));

        builder.Services.AddSingleton(sink);
        builder.Services.AddSingleton(ctx.SubscriberCredentials);

        // Before UseWolverine so a test-supplied registration (e.g. a shared IReplayIdRepository
        // standing in for the durable store) wins over the transport's TryAdd defaults.
        configureServices?.Invoke(builder.Services);

        builder.UseWolverine(opts =>
        {
            opts.UseSalesforcePubSub(ctx.PubSubUri)
                .UseAuthenticationHandler<SubscriberTokenHandler>();

            opts.Discovery.DisableConventionalDiscovery();

            configure(opts);
        });

        var host = builder.Build();
        await host.StartAsync();

        if (readyEvents > 0)
        {
            // A single sentinel races the subscribe on multi-endpoint hosts: an endpoint that goes
            // live after the publish misses its copy forever (Latest semantics). So retry with a FRESH
            // sentinel per attempt — once every endpoint is live, the newest sentinel arrives on all
            // of them within one window and readiness is proven.
            var deadline = DateTime.UtcNow + ReadyTimeout;
            while (true)
            {
                var sentinel = $"ready-{Guid.NewGuid():N}";
                await ctx.PublishAsync(readyEventName, sentinel);
                try
                {
                    await sink.WaitForAsync(e => e.Message == sentinel, readyEvents, ReadyAttemptWindow);
                    break;
                }
                catch (TimeoutException ex)
                {
                    if (DateTime.UtcNow + ReadyAttemptWindow <= deadline)
                        continue;

                    await StopAsync(host);
                    var tail = string.Join(Environment.NewLine, diagnostics.Tail(200));
                    throw new TimeoutException(
                        $"Listener readiness was not proven within {ReadyTimeout} — no sentinel was delivered {readyEvents} time(s). {ex.Message}{Environment.NewLine}" +
                        $"--- host log tail ---{Environment.NewLine}{tail}");
                }
            }
        }
        else
        {
            await Task.Delay(SubscribeGracePeriod);
        }

        return host;
    }

    public static async Task StopAsync(IHost host)
    {
        await host.StopAsync();
        host.Dispose();
    }
}
