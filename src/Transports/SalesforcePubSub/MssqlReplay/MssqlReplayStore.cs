using Microsoft.Data.SqlClient;

namespace MssqlReplay;

/// <summary>
/// Low-level SQL gateway for the replay table, ported from the original
/// <c>Salesforce.Subscriber.Services.Replay.Sql</c>. Opens a fresh pooled connection per call; auth is the
/// connection string's concern (AAD via <see cref="MssqlAadAuthentication"/>). Identity is (Application,
/// Instance, Topic).
///
/// Position writes are monotonic (DECISIONS #23 addendum): the transport's RepositoryCallTimeout can
/// ABANDON a hung write, which then completes late — after newer positions have landed — and would
/// regress the row (bounded duplicates on the next cold start). The upsert clamps with CASE rather than
/// filtering in the WHERE so the row always matches and the IF @@ROWCOUNT = 0 insert fallback stays
/// reliable; a blocked write still refreshes UpdatedOn (the row's liveness heartbeat). Equal positions
/// pass, matching the tracker's guard. LastEvent* diagnostics get the same clamp (NULL never clobbers).
/// The reset path is the one deliberate regression and uses its own unguarded statement.
/// </summary>
internal sealed class MssqlReplayStore
{
    private readonly string _connectionString;
    private readonly string _application;
    private readonly string _instance;
    private readonly string _selectSql;
    private readonly string _insertNewSql;
    private readonly string _upsertSql;
    private readonly string _resetSql;

    public MssqlReplayStore(string connectionString, string schema, string table, string application, string instance)
    {
        _connectionString = connectionString;
        _application = application;
        _instance = instance;

        var qualified = $"[{Escape(schema)}].[{Escape(table)}]";

        _selectSql =
            $"SELECT [ReplayId] FROM {qualified} " +
            "WHERE [Application] = @app AND [Instance] = @inst AND [Topic] = @topic;";

        _insertNewSql =
            $"INSERT INTO {qualified} " +
            "([Application],[Instance],[Topic],[ReplayId],[CreatedOn],[UpdatedOn],[LastEventReplayId],[LastEventOn]) " +
            "VALUES (@app,@inst,@topic,@rid,@now,@now,NULL,NULL);";

        // All SET expressions read pre-update column values, so the clamps evaluate consistently
        // against the old row; the single-row UPDATE's lock serializes a zombie against a current
        // writer without hints.
        _upsertSql =
            $"UPDATE {qualified} SET " +
            "[ReplayId] = CASE WHEN [ReplayId] <= @rid THEN @rid ELSE [ReplayId] END, " +
            "[UpdatedOn] = @now, " +
            "[LastEventReplayId] = CASE WHEN @lev IS NULL THEN [LastEventReplayId] " +
            "WHEN [LastEventReplayId] IS NULL OR [LastEventReplayId] <= @lev THEN @lev " +
            "ELSE [LastEventReplayId] END, " +
            "[LastEventOn] = CASE WHEN @lev IS NULL THEN [LastEventOn] " +
            "WHEN [LastEventReplayId] IS NULL OR [LastEventReplayId] <= @lev THEN @levon " +
            "ELSE [LastEventOn] END " +
            "WHERE [Application] = @app AND [Instance] = @inst AND [Topic] = @topic; " +
            "IF @@ROWCOUNT = 0 " +
            $"INSERT INTO {qualified} " +
            "([Application],[Instance],[Topic],[ReplayId],[CreatedOn],[UpdatedOn],[LastEventReplayId],[LastEventOn]) " +
            "VALUES (@app,@inst,@topic,@rid,@now,@now,@lev,@levon);";

        // Deliberate regression to new-events-only (Salesforce rejected the stored id) — must bypass
        // the monotonic clamp or every cold start re-reads the stale id and fails validation again.
        // Diagnostics columns are left untouched.
        _resetSql =
            $"UPDATE {qualified} SET [ReplayId] = @rid, [UpdatedOn] = @now " +
            "WHERE [Application] = @app AND [Instance] = @inst AND [Topic] = @topic; " +
            "IF @@ROWCOUNT = 0 " +
            $"INSERT INTO {qualified} " +
            "([Application],[Instance],[Topic],[ReplayId],[CreatedOn],[UpdatedOn],[LastEventReplayId],[LastEventOn]) " +
            "VALUES (@app,@inst,@topic,@rid,@now,@now,NULL,NULL);";
    }

    /// <summary>Reads the persisted replay id for a topic. Returns null when no row exists yet.</summary>
    public async Task<long?> TryGetReplayIdAsync(string topic, CancellationToken ct)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        await using var cmd = new SqlCommand(_selectSql, conn);
        AddScope(cmd, topic);

        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is null or DBNull ? null : (long)result;
    }

    /// <summary>Inserts the initial row for a topic (cold start, typically -1).</summary>
    public async Task InsertNewAsync(string topic, long replayId, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        await using var cmd = new SqlCommand(_insertNewSql, conn);
        AddScope(cmd, topic);
        cmd.Parameters.AddWithValue("@rid", replayId);
        cmd.Parameters.AddWithValue("@now", nowUtc);

        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Persists the latest position (and last-event diagnostics) for a topic.</summary>
    public async Task UpsertAsync(string topic, long replayId, long? lastEventReplayId, DateTime? lastEventOnUtc, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        await using var cmd = new SqlCommand(_upsertSql, conn);
        AddScope(cmd, topic);
        cmd.Parameters.AddWithValue("@rid", replayId);
        cmd.Parameters.AddWithValue("@now", nowUtc);
        cmd.Parameters.AddWithValue("@lev", (object?)lastEventReplayId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@levon", (object?)lastEventOnUtc ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Unconditionally rewinds the stored position (replay-id validation failure recovery).</summary>
    public async Task ResetAsync(string topic, long replayId, DateTime nowUtc, CancellationToken ct)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct).ConfigureAwait(false);

        await using var cmd = new SqlCommand(_resetSql, conn);
        AddScope(cmd, topic);
        cmd.Parameters.AddWithValue("@rid", replayId);
        cmd.Parameters.AddWithValue("@now", nowUtc);

        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private void AddScope(SqlCommand cmd, string topic)
    {
        cmd.Parameters.AddWithValue("@app", _application);
        cmd.Parameters.AddWithValue("@inst", _instance);
        cmd.Parameters.AddWithValue("@topic", topic);
    }

    private static string Escape(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("SQL identifier cannot be null or whitespace", nameof(identifier));

        return identifier.Replace("]", "]]");
    }
}
