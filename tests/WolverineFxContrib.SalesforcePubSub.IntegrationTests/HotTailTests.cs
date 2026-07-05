using Wolverine;
using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// Kafka-suite equivalent: hot_tail_delivers_all_messages_to_every_node — but where Kafka needs
/// ephemeral consumer groups to get broadcast semantics, Salesforce pub/sub is broadcast by nature:
/// every subscriber to a topic receives every event. Two independent hosts, one publish, two deliveries.
/// </summary>
public class HotTailTests(SalesforceTestContext ctx)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task Every_subscriber_receives_every_event()
    {
        Action<WolverineOptions> endpoint = opts =>
        {
            opts.Discovery.IncludeType<WitEventAHandler>();
            opts.ListenToSalesforceTopic("/event/WIT_Event_A__e").MapEvent<WitEventA>();
        };

        var sink1 = new EventSink();
        var sink2 = new EventSink();
        var host1 = await TestHosts.StartListeningAsync(ctx, sink1, endpoint);
        var host2 = await TestHosts.StartListeningAsync(ctx, sink2, endpoint);

        try
        {
            var correlation = $"hot-tail-{Guid.NewGuid():N}";
            await ctx.PublishAsync("WIT_Event_A__e", correlation, TestContext.Current.CancellationToken);

            var received1 = await sink1.WaitForAsync(e => e.Message == correlation, count: 1, ReceiveTimeout);
            var received2 = await sink2.WaitForAsync(e => e.Message == correlation, count: 1, ReceiveTimeout);

            // The same event, same bus position, delivered independently to both subscribers.
            Assert.Equal(Assert.Single(received1).ReplayId, Assert.Single(received2).ReplayId);
        }
        finally
        {
            await TestHosts.StopAsync(host1);
            await TestHosts.StopAsync(host2);
        }
    }
}
