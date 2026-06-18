namespace Wolverine.SalesforcePubSub;

/// <summary>
/// <see cref="BackoffAsync"/> is awaited before re-establishing a connection to the Pub/Sub API.
/// Implementations decide how long to wait based on the number of consecutive failures.
/// </summary>
public interface IBackoffStrategy
{
    Task BackoffAsync(long consecutiveFailuresWithoutResponse, TimeSpan durationSinceLastSuccessfulResponseOrStart, string resource, CancellationToken token);
}
