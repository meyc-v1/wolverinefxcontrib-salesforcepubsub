using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// Kafka-suite equivalent: multi_topic_listening — one stream carrying multiple message types, each
/// routed to its own handler. A custom channel delivers both WIT events; per-event schema resolution
/// must decode each to its mapped type.
/// </summary>
public class ChannelReceiveTests(SalesforceTestContext ctx)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task Decodes_both_event_types_from_one_channel_stream()
    {
        var sink = new EventSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Discovery.IncludeType<WitEventAHandler>();
            opts.Discovery.IncludeType<WitEventBHandler>();
            opts.ListenToSalesforceTopic("/event/WIT_Channel__chn")
                .MapEvent<WitEventA>()
                .MapEvent<WitEventB>();
        });

        try
        {
            var correlationA = $"channel-a-{Guid.NewGuid():N}";
            var correlationB = $"channel-b-{Guid.NewGuid():N}";
            await ctx.PublishAsync("WIT_Event_A__e", correlationA, TestContext.Current.CancellationToken);
            await ctx.PublishAsync("WIT_Event_B__e", correlationB, TestContext.Current.CancellationToken);

            var receivedA = await sink.WaitForAsync(e => e.Message == correlationA, count: 1, ReceiveTimeout);
            var receivedB = await sink.WaitForAsync(e => e.Message == correlationB, count: 1, ReceiveTimeout);

            Assert.Equal(nameof(WitEventA), Assert.Single(receivedA).EventType);
            Assert.Equal(nameof(WitEventB), Assert.Single(receivedB).EventType);
            Assert.All(receivedA.Concat(receivedB), e => Assert.Equal("/event/WIT_Channel__chn", e.TopicName));
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }
}
