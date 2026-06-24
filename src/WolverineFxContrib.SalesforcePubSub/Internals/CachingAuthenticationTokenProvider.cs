namespace Wolverine.SalesforcePubSub.Internals;

/// <summary>
/// Owns Salesforce access-token caching for the transport. Wraps the consumer's
/// <see cref="IAuthenticationTokenHandler"/> (which is expected to fetch a fresh token and not cache),
/// holding a single token for <see cref="SubscriberComponentsSettings.TokenCacheDuration"/>.
///
/// Registered as a singleton so the subscribe stream and the schema unary calls share one token and
/// one cache. <see cref="Invalidate"/> is called by the listener on an authentication failure so the
/// next fetch obtains a new token — this is what recovers from a revoked-before-expiry token, which a
/// purely TTL-based cache cannot do.
/// </summary>
internal sealed class CachingAuthenticationTokenProvider
{
    private sealed record Entry(AuthenticationTokenResponse Token, DateTimeOffset ExpiresAt);

    private readonly IAuthenticationTokenHandler _handler;
    private readonly SubscriberComponentsSettings _settings;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private volatile Entry? _entry;

    public CachingAuthenticationTokenProvider(IAuthenticationTokenHandler handler, SubscriberComponentsSettings settings)
    {
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    }

    public async Task<AuthenticationTokenResponse> GetTokenAsync(CancellationToken ct)
    {
        // Single volatile read: the entry is swapped atomically, so the fast path needs no lock.
        var entry = _entry;
        if (entry is not null && DateTimeOffset.UtcNow < entry.ExpiresAt)
            return entry.Token;

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            entry = _entry;
            if (entry is not null && DateTimeOffset.UtcNow < entry.ExpiresAt)
                return entry.Token;

            var token = await _handler.GetAuthenticationTokenAsync().ConfigureAwait(false)
                        ?? throw new InvalidOperationException(
                            $"{nameof(IAuthenticationTokenHandler)}.{nameof(IAuthenticationTokenHandler.GetAuthenticationTokenAsync)} returned null.");

            _entry = new Entry(token, DateTimeOffset.UtcNow.Add(_settings.TokenCacheDuration));
            return token;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Drops the cached token so the next <see cref="GetTokenAsync"/> re-fetches a fresh one.</summary>
    public void Invalidate() => _entry = null;
}
