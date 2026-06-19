using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using WolverineFxContrib.SalesforcePubSub.TestHost.Settings;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Salesforce;

public interface ISalesforceTokenClient
{
    Task<SalesforceTokenResponse> GetTokenResponseAsync(bool refresh = false);
}

/// <summary>
/// Client-credentials token client with in-memory caching. Lifted from the internal client
/// (Polly retry omitted for the test host).
/// </summary>
internal sealed class SalesforceTokenClient : ISalesforceTokenClient
{
    private static readonly string CacheKey = $"{typeof(SalesforceTokenClient).FullName!.ToLowerInvariant()}.salesforcetokenresponse";
    private static readonly SemaphoreSlim Locker = new(1);

    private readonly HttpClient _client;
    private readonly IMemoryCache _memoryCache;
    private readonly SalesforceAuthenticationSettings _settings;

    public SalesforceTokenClient(HttpClient client, IOptions<SalesforceAuthenticationSettings> settings, IMemoryCache memoryCache)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _memoryCache = memoryCache ?? throw new ArgumentNullException(nameof(memoryCache));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
    }

    public async Task<SalesforceTokenResponse> GetTokenResponseAsync(bool refresh = false)
    {
        Lazy<Task<SalesforceTokenResponse>>? cacheItem;

        try
        {
            await Locker.WaitAsync().ConfigureAwait(false);

            if (refresh)
                _memoryCache.Remove(CacheKey);

            cacheItem = _memoryCache.GetOrCreate(CacheKey, entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(_settings.TokenCachingInMinutes);
                return new Lazy<Task<SalesforceTokenResponse>>(
                    () => Task.Factory.StartNew(FetchTokenAsync).Unwrap(),
                    LazyThreadSafetyMode.ExecutionAndPublication);
            });
        }
        finally
        {
            Locker.Release();
        }

        return await cacheItem!.Value.ConfigureAwait(false);
    }

    private async Task<SalesforceTokenResponse> FetchTokenAsync()
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, "services/oauth2/token")
        {
            Content = new FormUrlEncodedContent(new List<KeyValuePair<string, string>>
            {
                new("grant_type", "client_credentials"),
                new("client_id", _settings.ClientId),
                new("client_secret", _settings.ClientSecret),
            })
        };

        using var resp = await _client.SendAsync(req).ConfigureAwait(false);
        var raw = await resp.Content.ReadAsStringAsync().ConfigureAwait(false);

        return JsonSerializer.Deserialize<SalesforceTokenResponse>(raw)
               ?? throw new InvalidOperationException("Could not deserialize Salesforce authentication response");
    }
}
