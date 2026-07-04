using System.Collections.Concurrent;
using Wolverine;
using Wolverine.ErrorHandling;
using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// Kafka-suite equivalent: DeadLetterQueueTests — the no-store half of the README delivery matrix. An
/// Inline endpoint with no message store moves a poison message to Wolverine's error queue, which is a
/// no-op store: the message is discarded, the replay watermark advances, and the stream keeps flowing.
/// (Preservation of poison messages is the Durable-mode test, which asserts the real DLQ table.)
/// </summary>
public class PoisonMessageTests(SalesforceTestContext ctx)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task A_poison_message_is_discarded_and_the_stream_continues()
    {
        var sink = new EventSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Discovery.IncludeType<PoisonEventAHandler>();
            opts.Policies.OnException<PoisonTestException>().MoveToErrorQueue();
            opts.ListenToSalesforceTopic("/event/WIT_Event_A__e").MapEvent<WitEventA>();
        });

        try
        {
            var poison = $"poison-{Guid.NewGuid():N}";
            var good = $"good-{Guid.NewGuid():N}";

            // Poison first, good second: replay order guarantees the poison event is resolved
            // (dead-lettered to the no-op store, watermark advanced) before the good one arrives.
            // Spaced because Salesforce can order near-simultaneous POSTs opposite to publish order.
            await ctx.PublishAsync("WIT_Event_A__e", poison, TestContext.Current.CancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            await ctx.PublishAsync("WIT_Event_A__e", good, TestContext.Current.CancellationToken);

            var received = await sink.WaitForAsync(e => e.Message == good, count: 1, ReceiveTimeout);

            Assert.Single(received);
            Assert.Equal(1, PoisonEventAHandler.Invocations(poison));            // one attempt, straight to the error queue
            Assert.DoesNotContain(sink.Snapshot(), e => e.Message == poison);    // never handled successfully
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }
}

public class PoisonTestException(string message) : Exception(message);

/// <summary>Throws forever for poison-prefixed correlations; records everything else.</summary>
public class PoisonEventAHandler
{
    private static readonly ConcurrentDictionary<string, int> Attempts = new();

    public static int Invocations(string correlation) => Attempts.GetValueOrDefault(correlation);

    public static void Handle(WitEventA evt, Envelope envelope, EventSink sink)
    {
        if (evt.Message__c is { } correlation && correlation.StartsWith("poison-"))
        {
            Attempts.AddOrUpdate(correlation, 1, (_, n) => n + 1);
            throw new PoisonTestException($"Intentional poison failure for {correlation}");
        }

        sink.Record(new ReceivedEvent(
            nameof(WitEventA), evt.Message__c, evt.ReplayId, envelope.Id, envelope.TopicName, envelope.SentAt));
    }
}
