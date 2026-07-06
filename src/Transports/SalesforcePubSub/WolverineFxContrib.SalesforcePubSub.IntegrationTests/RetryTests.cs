using System.Collections.Concurrent;
using Wolverine;
using Wolverine.ErrorHandling;
using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Events;
using WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests;

/// <summary>
/// Compliance-suite equivalent: will_requeue_and_increment_attempts. A requeue policy rides the
/// transport's DeferAsync, which has no native per-message redelivery on a replay stream and instead
/// re-injects the envelope for an in-memory retry (DECISIONS #10) — the replay watermark holds below
/// the event until it finally resolves.
/// </summary>
public class RetryTests(SalesforceTestContext ctx)
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task A_requeued_event_retries_in_memory_and_succeeds()
    {
        var sink = new EventSink();
        var host = await TestHosts.StartListeningAsync(ctx, sink, opts =>
        {
            opts.Discovery.IncludeType<FlakyEventAHandler>();
            opts.Policies.OnException<FlakyTestException>().Requeue(maxAttempts: 3);
            opts.ListenToSalesforceTopic("/event/WIT_Event_A__e").MapEvent<WitEventA>();
        });

        try
        {
            var correlation = $"retry-{Guid.NewGuid():N}";
            await ctx.PublishAsync("WIT_Event_A__e", correlation, TestContext.Current.CancellationToken);

            var received = await sink.WaitForAsync(e => e.Message == correlation, count: 1, ReceiveTimeout);

            Assert.Single(received);                                          // handled exactly once in the end
            Assert.Equal(3, FlakyEventAHandler.Invocations(correlation));     // failed twice, succeeded third
        }
        finally
        {
            await TestHosts.StopAsync(host);
        }
    }
}

public class FlakyTestException(string message) : Exception(message);

/// <summary>Throws on the first two attempts for a correlation, succeeds on the third.</summary>
public class FlakyEventAHandler
{
    private static readonly ConcurrentDictionary<string, int> Attempts = new();

    public static int Invocations(string correlation) => Attempts.GetValueOrDefault(correlation);

    public static void Handle(WitEventA evt, Envelope envelope, EventSink sink)
    {
        if (evt.Message__c is { } m && m.StartsWith("ready-"))
        {
            // The harness readiness sentinel must reach the sink or the host can never prove itself live.
            sink.Record(new ReceivedEvent(
                nameof(WitEventA), evt.Message__c, evt.ReplayId, envelope.Id, envelope.TopicName, envelope.SentAt));
            return;
        }

        if (evt.Message__c is not { } correlation || !correlation.StartsWith("retry-"))
            return; // stray event from another run — ignore, don't fail it

        var attempt = Attempts.AddOrUpdate(correlation, 1, (_, n) => n + 1);
        if (attempt < 3)
            throw new FlakyTestException($"Intentional failure, attempt {attempt} for {correlation}");

        sink.Record(new ReceivedEvent(
            nameof(WitEventA), evt.Message__c, evt.ReplayId, envelope.Id, envelope.TopicName, envelope.SentAt));
    }
}
