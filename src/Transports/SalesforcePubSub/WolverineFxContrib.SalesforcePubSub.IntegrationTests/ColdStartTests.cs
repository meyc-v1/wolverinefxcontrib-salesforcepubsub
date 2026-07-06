using Wolverine;
using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// Kafka-suite equivalent: cold_start_and_hot_tail — where a fresh consumer begins. With no stored
/// replay id the transport subscribes at Latest (new events only); StartFromEarliest opts a cold start
/// into replaying the retained history (Salesforce keeps 72h).
/// </summary>
public class ColdStartTests(SalesforceTestContext ctx)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(60);

    /// <summary>Replaying history first can take a while — give the earliest case more room.</summary>
    private static readonly TimeSpan EarliestTimeout = TimeSpan.FromSeconds(90);

    [Fact]
    public async Task A_cold_start_defaults_to_latest_and_never_sees_earlier_events()
    {
        Action<WolverineOptions> endpoint = opts =>
        {
            opts.Discovery.IncludeType<WitEventAHandler>();
            opts.ListenToSalesforceTopic("/event/WIT_Event_A__e").MapEvent<WitEventA>();
        };

        // Salesforce publishes are async-queued: the REST accept does NOT mean the event is on the bus
        // yet, and a subscribe that pins Latest before the queued event lands will (correctly) deliver
        // it — failing this fact for the wrong reason (observed live: the "before" event bus-stamped a
        // second AFTER the readiness sentinel). So prove the before-event is on the bus with a probe
        // subscriber before the fact's host ever subscribes.
        var beforeSubscribe = $"before-subscribe-{Guid.NewGuid():N}";
        var probeSink = new EventSink();
        var probe = await TestHosts.StartListeningAsync(ctx, probeSink, endpoint);
        try
        {
            await ctx.PublishAsync("WIT_Event_A__e", beforeSubscribe, TestContext.Current.CancellationToken);
            await probeSink.WaitForAsync(e => e.Message == beforeSubscribe, count: 1, ReceiveTimeout);
        }
        finally
        {
            await TestHosts.StopAsync(probe);
        }

        var sink = new EventSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, endpoint);

        try
        {
            var afterSubscribe = $"after-subscribe-{Guid.NewGuid():N}";
            await ctx.PublishAsync("WIT_Event_A__e", afterSubscribe, TestContext.Current.CancellationToken);

            // The after-event arriving proves the stream is flowing; replay order means the
            // before-event would already be here if Latest had (wrongly) included it.
            await sink.WaitForAsync(e => e.Message == afterSubscribe, count: 1, ReceiveTimeout);

            Assert.DoesNotContain(sink.Snapshot(), e => e.Message == beforeSubscribe);
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }

    [Fact]
    public async Task StartFromEarliest_replays_events_published_before_the_subscribe()
    {
        var historical = $"historical-{Guid.NewGuid():N}";
        await ctx.PublishAsync("WIT_Event_A__e", historical, TestContext.Current.CancellationToken);

        var sink = new EventSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Discovery.IncludeType<WitEventAHandler>();
            opts.ListenToSalesforceTopic("/event/WIT_Event_A__e")
                .MapEvent<WitEventA>()
                .StartFromEarliest(true);
        });

        try
        {
            // The exact event the Latest test proves is NEVER delivered arrives here — after the
            // rest of the topic's 72h retained history replays through the same handler.
            var received = await sink.WaitForAsync(e => e.Message == historical, count: 1, EarliestTimeout);
            Assert.Single(received);
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }
}
