using Wolverine.SalesforcePubSub.Internals.Replay;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>Behavior of the in-memory replay fallback used when a consumer registers no repository.</summary>
public class InMemoryReplayIdRepositoryTests
{
    private const long NewEventsOnly = -1;

    [Fact]
    public async Task GetLastReplayId_returns_new_events_only_when_nothing_is_stored()
    {
        var ct = TestContext.Current.CancellationToken;
        var repo = new InMemoryReplayIdRepository();

        Assert.Equal(NewEventsOnly, await repo.GetLastReplayIdAsync("topic-a", ct));
    }

    [Fact]
    public async Task ReportEventsReceived_advances_the_stored_position()
    {
        var ct = TestContext.Current.CancellationToken;
        var repo = new InMemoryReplayIdRepository();

        await repo.ReportEventsReceivedResponseAsync("topic-a", 42, [42], ct);

        Assert.Equal(42, await repo.GetLastReplayIdAsync("topic-a", ct));
    }

    [Fact]
    public async Task ReportKeepAlive_advances_the_stored_position()
    {
        var ct = TestContext.Current.CancellationToken;
        var repo = new InMemoryReplayIdRepository();

        await repo.ReportKeepAliveResponseAsync("topic-a", 7, ct);

        Assert.Equal(7, await repo.GetLastReplayIdAsync("topic-a", ct));
    }

    [Fact]
    public async Task Reset_returns_the_position_to_new_events_only()
    {
        var ct = TestContext.Current.CancellationToken;
        var repo = new InMemoryReplayIdRepository();

        await repo.ReportEventsReceivedResponseAsync("topic-a", 99, [99], ct);
        await repo.ResetForNewEventsOnlyAsync("topic-a", ct);

        Assert.Equal(NewEventsOnly, await repo.GetLastReplayIdAsync("topic-a", ct));
    }

    [Fact]
    public async Task Positions_are_tracked_independently_per_topic()
    {
        var ct = TestContext.Current.CancellationToken;
        var repo = new InMemoryReplayIdRepository();

        await repo.ReportEventsReceivedResponseAsync("topic-a", 10, [10], ct);
        await repo.ReportEventsReceivedResponseAsync("topic-b", 20, [20], ct);

        Assert.Equal(10, await repo.GetLastReplayIdAsync("topic-a", ct));
        Assert.Equal(20, await repo.GetLastReplayIdAsync("topic-b", ct));
        Assert.Equal(NewEventsOnly, await repo.GetLastReplayIdAsync("topic-c", ct));
    }
}
