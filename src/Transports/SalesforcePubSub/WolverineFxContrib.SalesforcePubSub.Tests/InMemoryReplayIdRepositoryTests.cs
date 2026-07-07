using Wolverine.SalesforcePubSub;
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
    public async Task Store_for_handled_events_advances_the_stored_position()
    {
        var ct = TestContext.Current.CancellationToken;
        var repo = new InMemoryReplayIdRepository();

        await repo.StoreReplayIdAsync("topic-a", 42, ReplayCommitKind.EventsHandled, ct);

        Assert.Equal(42, await repo.GetLastReplayIdAsync("topic-a", ct));
    }

    [Fact]
    public async Task Store_for_keep_alive_advances_the_stored_position()
    {
        var ct = TestContext.Current.CancellationToken;
        var repo = new InMemoryReplayIdRepository();

        await repo.StoreReplayIdAsync("topic-a", 7, ReplayCommitKind.KeepAlive, ct);

        Assert.Equal(7, await repo.GetLastReplayIdAsync("topic-a", ct));
    }

    [Fact]
    public async Task Reset_returns_the_position_to_new_events_only()
    {
        var ct = TestContext.Current.CancellationToken;
        var repo = new InMemoryReplayIdRepository();

        await repo.StoreReplayIdAsync("topic-a", 99, ReplayCommitKind.EventsHandled, ct);
        await repo.ResetForNewEventsOnlyAsync("topic-a", ct);

        Assert.Equal(NewEventsOnly, await repo.GetLastReplayIdAsync("topic-a", ct));
    }

    [Fact]
    public async Task Positions_are_tracked_independently_per_topic()
    {
        var ct = TestContext.Current.CancellationToken;
        var repo = new InMemoryReplayIdRepository();

        await repo.StoreReplayIdAsync("topic-a", 10, ReplayCommitKind.EventsHandled, ct);
        await repo.StoreReplayIdAsync("topic-b", 20, ReplayCommitKind.EventsHandled, ct);

        Assert.Equal(10, await repo.GetLastReplayIdAsync("topic-a", ct));
        Assert.Equal(20, await repo.GetLastReplayIdAsync("topic-b", ct));
        Assert.Equal(NewEventsOnly, await repo.GetLastReplayIdAsync("topic-c", ct));
    }
}
