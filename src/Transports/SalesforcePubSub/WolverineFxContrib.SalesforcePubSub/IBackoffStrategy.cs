namespace Wolverine.SalesforcePubSub;

/// <summary>
/// <see cref="BackoffAsync"/> is awaited before re-establishing a connection to the Pub/Sub API.
/// Implementations decide how long to wait based on the number of consecutive failures.
/// <para>
/// The listener guards this call: an implementation that throws cannot fault the reconnect loop —
/// the listener logs the exception and falls back to its default pacing for that attempt. Honor
/// <c>token</c> (it signals listener shutdown); an <see cref="OperationCanceledException"/> during
/// the wait is treated as shutdown, not a failure.
/// </para>
/// </summary>
public interface IBackoffStrategy
{
    /// <summary>
    /// Waits before the next reconnect attempt. <paramref name="consecutiveFailuresWithoutResponse"/> is the
    /// number of connection attempts since the last successful response, and
    /// <paramref name="durationSinceLastSuccessfulResponseOrStart"/> is how long the
    /// <paramref name="resource"/> (topic path or MES developer name) has gone without one.
    /// </summary>
    Task BackoffAsync(long consecutiveFailuresWithoutResponse, TimeSpan durationSinceLastSuccessfulResponseOrStart, string resource, CancellationToken token = default);
}
