using System.Text.Json;
using Microsoft.Extensions.Options;
using Wolverine.SalesforcePubSub;
using Salesforce.Models;
using TestHost.Settings;

namespace TestHost.Salesforce;

/// <summary>
/// The transport's <see cref="IAuthenticationTokenHandler"/> over the subscriber ECA: a direct
/// client-credentials fetch per call — fresh every time, no cache — because the transport owns token
/// caching and invalidation (a caching handler would defeat revoked-token recovery). The REST
/// publisher authenticates separately through the Salesforce lib and the publisher ECA.
/// </summary>
internal sealed class SalesforceAuthenticationTokenHandler : IAuthenticationTokenHandler
{
    internal const string HttpClientName = "salesforce-subscriber-token";

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly SubscriberAuthenticationSettings _settings;

    public SalesforceAuthenticationTokenHandler(IHttpClientFactory httpClientFactory, IOptions<SubscriberAuthenticationSettings> settings)
    {
        _httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
    }

    public async Task<AuthenticationTokenResponse> GetAuthenticationTokenAsync(CancellationToken cancellationToken = default)
    {
        var client = _httpClientFactory.CreateClient(HttpClientName);

        using var req = new HttpRequestMessage(HttpMethod.Post, new Uri(_settings.LoginUri, "services/oauth2/token"))
        {
            Content = new FormUrlEncodedContent(new List<KeyValuePair<string, string>>
            {
                new("grant_type", "client_credentials"),
                new("client_id", _settings.ClientId),
                new("client_secret", _settings.ClientSecret),
            })
        };

        using var resp = await client.SendAsync(req, cancellationToken).ConfigureAwait(false);
        var raw = await resp.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

        // Fail loud on a non-success response; the transport's provider validates the token contents
        // before caching (a null-token response must never poison its cache).
        if (!resp.IsSuccessStatusCode)
            throw new InvalidOperationException($"Salesforce subscriber token request failed ({(int)resp.StatusCode} {resp.StatusCode}): {raw}");

        var token = JsonSerializer.Deserialize<SalesforceTokenResponse>(raw)
                    ?? throw new InvalidOperationException("Could not deserialize Salesforce authentication response");

        return new AuthenticationTokenResponse(token.AccessToken!, token.InstanceUrl!, token.TenantId!);
    }
}
