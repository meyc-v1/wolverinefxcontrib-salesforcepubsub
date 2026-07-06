using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// Kafka-suite equivalent: basic receive + envelope metadata (multi_topic_listening /
/// received_message_from_topic_group_has_partition_id). Publish a platform event via REST and assert it
/// arrives as a typed Wolverine message on its __e topic, carrying the transport's envelope facts.
/// </summary>
public class TopicReceiveTests(SalesforceTestContext ctx)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task Receives_a_published_event_as_a_typed_message_with_envelope_metadata()
    {
        var sink = new EventSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Discovery.IncludeType<WitEventAHandler>();
            opts.ListenToSalesforceTopic("/event/WIT_Event_A__e").MapEvent<WitEventA>();
        });

        try
        {
            var correlation = $"topic-receive-{Guid.NewGuid():N}";
            await ctx.PublishAsync("WIT_Event_A__e", correlation, TestContext.Current.CancellationToken);

            var received = await sink.WaitForAsync(e => e.Message == correlation, count: 1, ReceiveTimeout);

            var evt = Assert.Single(received);
            Assert.Equal(nameof(WitEventA), evt.EventType);
            Assert.True(evt.ReplayId > 0, "the transport stamps the stream position as ReplayId");
            Assert.Equal("/event/WIT_Event_A__e", evt.TopicName);
            Assert.NotEqual(Guid.Empty, evt.EnvelopeId); // deterministic id derived from the SF event id
            Assert.NotNull(evt.SentAt);                  // stamped from the platform event's CreatedDate
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }
}
