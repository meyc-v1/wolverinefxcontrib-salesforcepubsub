namespace WolverineFxContrib.SalesforcePubSub.TestHost.Settings;

/// <summary>
/// Replay-id persistence + fault-injection knobs for the durability runs, bound from "salesforceReplaySettings".
/// </summary>
public sealed class ReplaySettings
{
    /// <summary>SQL Server connection for persistent topic replay (resume-across-restart). Empty = in-memory.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Logical consumer name — first PK column of the replay table.</summary>
    public string Application { get; set; } = "wolverine-salesforcepubsub-testhost";

    /// <summary>Logical instance id recorded alongside persisted replay ids — second PK column.</summary>
    public string Instance { get; set; } = "wolverine-testhost";

    /// <summary>Replay table schema.</summary>
    public string Schema { get; set; } = "dbo";

    /// <summary>Replay table name (DDL owned by the deployer; see CreateReplayTable.sql).</summary>
    public string TableName { get; set; } = "SalesforceSubscriberReplay";

    /// <summary>
    /// Layer-A bad-replay-id test: when set, the first topic replay read returns this bogus id (e.g. 9999),
    /// forcing Salesforce to answer <c>InvalidArgument</c> so we can watch the transport reset to Latest and
    /// recover. One-shot per process; ignored for MES (server-side replay).
    /// </summary>
    public long? SeedBadReplayId { get; set; }
}
