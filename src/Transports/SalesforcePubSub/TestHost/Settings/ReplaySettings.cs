namespace TestHost.Settings;

/// <summary>
/// The TestHost's replay wiring decision, bound from "salesforceReplaySettings". The SQL persistence
/// detail lives in the MssqlReplay lib's <c>MssqlReplaySettings</c>, bound from the same section.
/// </summary>
public sealed class ReplaySettings
{
    /// <summary>SQL Server connection for persistent topic replay (resume-across-restart). Empty = in-memory.</summary>
    public string? ConnectionString { get; set; }
}
