using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// No Kafka analogue — managed event subscriptions (Salesforce-managed replay) are this transport's own
/// kind. The MES slot is exclusive per client and an unclean disconnect holds it ~15 minutes, so a
/// timed-out receive with ALREADY_EXISTS in the listener log skips (yellow) rather than fails (red).
/// One wrinkle to expect: the server-side checkpoint survives between runs, so each subscribe first
/// replays every WIT event published since the previous run's final commit — correlation matching makes
/// the backlog harmless.
/// </summary>
public class MesReceiveTests(SalesforceTestContext ctx)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(90);

    [Fact]
    public async Task Receives_a_published_event_through_a_managed_subscription()
    {
        var sink = new EventSink();
        var logs = new LogSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Discovery.IncludeType<WitEventAHandler>();
            opts.ListenToManagedSubscription("WIT_Event_A_Sub").MapEvent<WitEventA>();
        }, logSink: logs, readyEvents: 0); // slot-held detection wraps the fact's own first wait

        try
        {
            var correlation = $"mes-receive-{Guid.NewGuid():N}";
            await ctx.PublishAsync("WIT_Event_A__e", correlation, TestContext.Current.CancellationToken);

            IReadOnlyList<ReceivedEvent> received;
            try
            {
                received = await sink.WaitForAsync(e => e.Message == correlation, count: 1, ReceiveTimeout);
            }
            catch (TimeoutException) when (logs.SawMesSlotHeld())
            {
                Assert.Skip("The WIT_Event_A_Sub MES slot is held by another client (released ~15 min after an unclean disconnect) — rerun later.");
                return;
            }

            Assert.Equal(nameof(WitEventA), Assert.Single(received).EventType);
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }

    [Fact]
    public async Task Decodes_both_event_types_through_a_managed_subscription_over_a_channel()
    {
        var sink = new EventSink();
        var logs = new LogSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Discovery.IncludeType<WitEventAHandler>();
            opts.Discovery.IncludeType<WitEventBHandler>();
            opts.ListenToManagedSubscription("WIT_Channel_Sub")
                .MapEvent<WitEventA>()
                .MapEvent<WitEventB>();
        }, logSink: logs, readyEvents: 0); // slot-held detection wraps the fact's own first wait

        try
        {
            var correlationA = $"mes-channel-a-{Guid.NewGuid():N}";
            var correlationB = $"mes-channel-b-{Guid.NewGuid():N}";
            await ctx.PublishAsync("WIT_Event_A__e", correlationA, TestContext.Current.CancellationToken);
            await ctx.PublishAsync("WIT_Event_B__e", correlationB, TestContext.Current.CancellationToken);

            IReadOnlyList<ReceivedEvent> receivedA;
            try
            {
                receivedA = await sink.WaitForAsync(e => e.Message == correlationA, count: 1, ReceiveTimeout);
            }
            catch (TimeoutException) when (logs.SawMesSlotHeld())
            {
                Assert.Skip("The WIT_Channel_Sub MES slot is held by another client (released ~15 min after an unclean disconnect) — rerun later.");
                return;
            }

            var receivedB = await sink.WaitForAsync(e => e.Message == correlationB, count: 1, ReceiveTimeout);

            Assert.Equal(nameof(WitEventA), Assert.Single(receivedA).EventType);
            Assert.Equal(nameof(WitEventB), Assert.Single(receivedB).EventType);
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }
}
