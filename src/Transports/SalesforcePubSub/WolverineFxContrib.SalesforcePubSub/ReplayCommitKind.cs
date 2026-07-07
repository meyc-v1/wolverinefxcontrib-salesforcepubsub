namespace Wolverine.SalesforcePubSub;

/// <summary>
/// What a committed replay position covers, so an <see cref="IReplayIdRepository"/> can
/// distinguish real event progress from idle drift (e.g. for last-event diagnostics).
/// </summary>
public enum ReplayCommitKind
{
    /// <summary>The commit covers one or more handled events.</summary>
    EventsHandled,

    /// <summary>
    /// No handled event contributed — the position advanced through keep-alive drift alone
    /// (replay ids are global across all Salesforce streaming events, so the position moves
    /// during idle). Store the position; do not update any last-event diagnostics.
    /// </summary>
    KeepAlive
}
