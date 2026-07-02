using Microsoft.Extensions.Logging;

namespace Wolverine.SalesforcePubSub.Internals;

/// <summary>
/// Linear backoff: no delay on the first error, then increments by <c>BackoffMilliseconds</c>
/// for each subsequent consecutive error, capped at <c>MaxWaitMilliseconds</c>.
/// </summary>
internal sealed class DefaultBackoffStrategy : IBackoffStrategy
{
    private readonly ILogger<DefaultBackoffStrategy> _logger;
    private const int BackoffMilliseconds = 15000;  // 15 seconds
    private const int MaxWaitMilliseconds = 120000;  // 2 minutes

    public DefaultBackoffStrategy(ILogger<DefaultBackoffStrategy> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task BackoffAsync(long consecutiveFailuresWithoutResponse, TimeSpan durationSinceLastSuccessfulResponseOrStart, string resource, CancellationToken token)
    {
        var duration = TimeSpan.FromMilliseconds(Math.Min(BackoffMilliseconds * (consecutiveFailuresWithoutResponse - 1), MaxWaitMilliseconds));

        if (duration <= TimeSpan.Zero)
            return;

        _logger.LogDebug("{Resource}: Backing off for {Duration}", resource, duration);
        await Task.Delay(duration, token).ConfigureAwait(false);
        _logger.LogDebug("{Resource}: Completed backoff of {Duration}", resource, duration);
    }
}
