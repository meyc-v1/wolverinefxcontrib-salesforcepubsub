namespace SqlReplay;

/// <summary>
/// SQL Server persistence settings for <see cref="SqlReplayIdRepository"/>. The host binds these from
/// its replay configuration section (the TestHost shares the "salesforceReplaySettings" section with
/// its own fault-injection knobs).
/// </summary>
public sealed class SqlReplaySettings
{
    /// <summary>SQL Server connection for persistent topic replay (resume-across-restart).</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Logical consumer name — first PK column of the replay table.</summary>
    public string Application { get; set; } = "";

    /// <summary>Logical instance id recorded alongside persisted replay ids — second PK column.</summary>
    public string Instance { get; set; } = "";

    /// <summary>Replay table schema.</summary>
    public string Schema { get; set; } = "dbo";

    /// <summary>Replay table name (DDL owned by the deployer; see CreateReplayTable.sql).</summary>
    public string TableName { get; set; } = "SalesforceSubscriberReplay";
}
