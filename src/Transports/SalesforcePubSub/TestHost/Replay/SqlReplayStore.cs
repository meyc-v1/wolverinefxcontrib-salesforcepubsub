using Microsoft.Data.SqlClient;

namespace TestHost.Replay;

/// <summary>
/// Low-level SQL gateway for the replay table, ported from the original
/// <c>Salesforce.Subscriber.Services.Replay.Sql</c>. Opens a fresh pooled connection per call; auth is the
/// connection string's concern (AAD via <see cref="SqlAadAuthentication"/>). Identity is (Application,
/// Instance, Topic). LastEvent* columns use COALESCE so a keepalive/reset write never clobbers the last
/// real-event diagnostics with NULL.
/// </summary>
internal sealed class SqlReplayStore
{
    private readonly string _connectionString;
    private readonly string _application;
    private readonly string _instance;
    private readonly string _selectSql;
    private readonly string _insertNewSql;
    private readonly string _upsertSql;

    public SqlReplayStore(string connectionString, string schema, string table, string application, string instance)
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

        _upsertSql =
            $"UPDATE {qualified} SET [ReplayId] = @rid, [UpdatedOn] = @now, " +
            "[LastEventReplayId] = COALESCE(@lev, [LastEventReplayId]), " +
            "[LastEventOn] = COALESCE(@levon, [LastEventOn]) " +
            "WHERE [Application] = @app AND [Instance] = @inst AND [Topic] = @topic; " +
            "IF @@ROWCOUNT = 0 " +
            $"INSERT INTO {qualified} " +
            "([Application],[Instance],[Topic],[ReplayId],[CreatedOn],[UpdatedOn],[LastEventReplayId],[LastEventOn]) " +
            "VALUES (@app,@inst,@topic,@rid,@now,@now,@lev,@levon);";
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
