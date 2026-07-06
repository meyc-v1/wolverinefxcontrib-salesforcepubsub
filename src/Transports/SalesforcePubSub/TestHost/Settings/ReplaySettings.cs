namespace TestHost.Settings;

/// <summary>
/// The TestHost's replay wiring decision, bound from "salesforceReplaySettings". The SQL persistence
/// detail lives in the SqlReplay lib's <c>SqlReplaySettings</c>, bound from the same section.
/// </summary>
public sealed class ReplaySettings
{
    /// <summary>SQL Server connection for persistent topic replay (resume-across-restart). Empty = in-memory.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Layer-A bad-replay-id test: when set, the first topic replay read returns this bogus id (e.g. 9999),
    /// forcing Salesforce to answer <c>InvalidArgument</c> so we can watch the transport reset to Latest and
    /// recover. One-shot per process; ignored for MES (server-side replay).
    /// </summary>
    public long? SeedBadReplayId { get; set; }
}
