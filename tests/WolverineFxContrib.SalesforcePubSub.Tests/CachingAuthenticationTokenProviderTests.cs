using Wolverine.SalesforcePubSub;
using Wolverine.SalesforcePubSub.Internals;

namespace WolverineFxContrib.SalesforcePubSub.Tests;

/// <summary>
/// The transport-owned token cache: serves a cached token within the TTL, re-fetches when the TTL
/// lapses, and re-fetches after <see cref="CachingAuthenticationTokenProvider.Invalidate"/> — the
/// invalidate path being what recovers from a revoked-before-expiry token.
/// </summary>
public class CachingAuthenticationTokenProviderTests
{
    private sealed class FakeHandler : IAuthenticationTokenHandler
    {
        private int _calls;
        public int Calls => _calls;
        public Func<int, AuthenticationTokenResponse?>? OnGet { get; init; }

        public Task<AuthenticationTokenResponse> GetAuthenticationTokenAsync()
        {
            var n = Interlocked.Increment(ref _calls);
            // When OnGet is set, honor its result verbatim (including null, for the null-guard test).
            var token = OnGet is not null ? OnGet(n) : new AuthenticationTokenResponse($"token-{n}", "https://instance", "tenant");
            return Task.FromResult(token!);
        }
    }

    [Fact]
    public async Task Caches_the_token_within_the_ttl()
    {
        var ct = TestContext.Current.CancellationToken;
        var handler = new FakeHandler();
        var provider = new CachingAuthenticationTokenProvider(handler, new SubscriberComponentsSettings());

        var first = await provider.GetTokenAsync(ct);
        var second = await provider.GetTokenAsync(ct);

        Assert.Same(first, second);
        Assert.Equal(1, handler.Calls);
    }

    [Fact]
    public async Task Invalidate_forces_a_fresh_fetch()
    {
        var ct = TestContext.Current.CancellationToken;
        var handler = new FakeHandler();
        var provider = new CachingAuthenticationTokenProvider(handler, new SubscriberComponentsSettings());

        var first = await provider.GetTokenAsync(ct);
        provider.Invalidate();
        var second = await provider.GetTokenAsync(ct);

        Assert.NotSame(first, second);
        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task Re_fetches_once_the_ttl_has_lapsed()
    {
        var ct = TestContext.Current.CancellationToken;
        var handler = new FakeHandler();
        var provider = new CachingAuthenticationTokenProvider(
            handler, new SubscriberComponentsSettings { TokenCacheDuration = TimeSpan.Zero });

        await provider.GetTokenAsync(ct);
        await provider.GetTokenAsync(ct);

        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public async Task Throws_when_the_handler_returns_null()
    {
        var ct = TestContext.Current.CancellationToken;
        var handler = new FakeHandler { OnGet = _ => null };
        var provider = new CachingAuthenticationTokenProvider(handler, new SubscriberComponentsSettings());

        await Assert.ThrowsAsync<InvalidOperationException>(() => provider.GetTokenAsync(ct));
    }
}
