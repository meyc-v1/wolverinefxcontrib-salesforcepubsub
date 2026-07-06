using Microsoft.Extensions.Logging.Abstractions;
using Wolverine.SalesforcePubSub.Internals.Backoff;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// The default linear backoff: no wait on the first failure, then a growing (capped) delay.
/// Timing-sensitive waits are exercised only via the no-delay branch and a pre-cancelled token,
/// so the tests stay fast and deterministic.
/// </summary>
public class DefaultBackoffStrategyTests
{
    private static DefaultBackoffStrategy Create() => new(NullLogger<DefaultBackoffStrategy>.Instance);

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void First_failure_does_not_delay(long consecutiveFailures)
    {
        var strategy = Create();

        var task = strategy.BackoffAsync(consecutiveFailures, TimeSpan.Zero, "topic-a", TestContext.Current.CancellationToken);

        // The no-delay branch returns before any await, so the task is already complete.
        Assert.True(task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Subsequent_failures_enter_the_delay_path()
    {
        var strategy = Create();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // consecutive >= 2 computes a positive delay, so Task.Delay observes the cancelled token.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => strategy.BackoffAsync(2, TimeSpan.Zero, "topic-a", cts.Token));
    }
}
