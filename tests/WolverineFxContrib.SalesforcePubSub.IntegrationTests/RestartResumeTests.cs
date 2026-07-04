using Microsoft.Extensions.DependencyInjection;
using Wolverine;
using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// Kafka-suite equivalent: commit_strategy_end_to_end (their can_stop_and_restart_listeners compliance
/// test is commented out — this goes further). A graceful stop flushes the handled watermark to the
/// replay repository; a fresh host resumes from it, receiving exactly what was published while it was
/// down and never redelivering the already-handled tail.
/// </summary>
public class RestartResumeTests(SalesforceTestContext ctx)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task A_restart_resumes_from_the_committed_position_and_receives_only_the_gap()
    {
        // Shared across both hosts — the durable store's stand-in for this test.
        var repo = new RecordingReplayIdRepository();

        Action<WolverineOptions> endpoint = opts =>
        {
            opts.Discovery.IncludeType<WitEventAHandler>();
            opts.ListenToSalesforceTopic("/event/WIT_Event_A__e").MapEvent<WitEventA>();
        };
        Action<IServiceCollection> sharedRepo = s => s.AddSingleton<IReplayIdRepository>(repo);

        var handledBeforeStop = $"handled-before-stop-{Guid.NewGuid():N}";
        var publishedWhileDown = $"published-while-down-{Guid.NewGuid():N}";

        // First life: handle one event, then stop gracefully (the stop flushes the watermark).
        var sink1 = new EventSink();
        var host1 = await TestHosts.StartListeningAsync(ctx, sink1, endpoint, sharedRepo);
        try
        {
            await ctx.PublishAsync("WIT_Event_A__e", handledBeforeStop, TestContext.Current.CancellationToken);
            await sink1.WaitForAsync(e => e.Message == handledBeforeStop, count: 1, ReceiveTimeout);
        }
        finally
        {
            await TestHosts.StopAsync(host1);
        }

        // The gap: published while nothing is listening.
        await ctx.PublishAsync("WIT_Event_A__e", publishedWhileDown, TestContext.Current.CancellationToken);

        // Second life: cold-starts from the repository's committed position.
        var sink2 = new EventSink();
        var host2 = await TestHosts.StartListeningAsync(ctx, sink2, endpoint, sharedRepo);
        try
        {
            var received = await sink2.WaitForAsync(e => e.Message == publishedWhileDown, count: 1, ReceiveTimeout);

            Assert.Single(received);

            // Resuming after the committed position means the handled tail is NOT redelivered.
            Assert.DoesNotContain(sink2.Snapshot(), e => e.Message == handledBeforeStop);

            // The stop-flush covered a handled event, so it must have reported events-received —
            // the live pin on the commit-flag semantics (one keep-alive drift commit is also fine).
            Assert.Contains(repo.Commits, c => c.Method == "EventsReceived");
        }
        finally
        {
            await TestHosts.StopAsync(host2);
        }
    }
}
