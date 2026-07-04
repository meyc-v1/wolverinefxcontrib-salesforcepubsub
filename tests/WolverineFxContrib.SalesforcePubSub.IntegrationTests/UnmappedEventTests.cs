using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// Kafka-suite equivalent: moving_unknown_cloudevents_type_to_dlq. An event arriving on a multi-type
/// stream with no MapEvent registration rides Wolverine's missing-handler policy (dead-letter +
/// complete) and the replay watermark advances past it — the stream must not wedge.
/// </summary>
public class UnmappedEventTests(SalesforceTestContext ctx)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task An_unmapped_event_is_skipped_and_the_stream_continues()
    {
        var sink = new EventSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Discovery.IncludeType<WitEventAHandler>();

            // The channel carries A and B, but this endpoint maps ONLY A — a published B is the
            // unmapped event.
            opts.ListenToSalesforceTopic("/event/WIT_Channel__chn").MapEvent<WitEventA>();
        });

        try
        {
            var unmapped = $"unmapped-{Guid.NewGuid():N}";
            var mapped = $"mapped-{Guid.NewGuid():N}";

            // B first, A second: replay order guarantees B is resolved (dead-lettered, watermark
            // advanced) before A can arrive. If the unmapped event wedged the stream, A never shows.
            await ctx.PublishAsync("WIT_Event_B__e", unmapped, TestContext.Current.CancellationToken);
            await ctx.PublishAsync("WIT_Event_A__e", mapped, TestContext.Current.CancellationToken);

            var received = await sink.WaitForAsync(e => e.Message == mapped, count: 1, ReceiveTimeout);

            Assert.Equal(nameof(WitEventA), Assert.Single(received).EventType);
            Assert.DoesNotContain(sink.Snapshot(), e => e.Message == unmapped); // B never reached a handler
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }
}
