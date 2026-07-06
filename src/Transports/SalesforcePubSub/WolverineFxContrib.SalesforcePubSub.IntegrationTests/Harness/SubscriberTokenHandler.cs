using System.Text.Json;
using Wolverine.SalesforcePubSub;
using Salesforce.Models;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

/// <summary>Client-credentials for the subscriber ECA (distinct from the publisher ECA — independent token lifecycles).</summary>
public sealed record SubscriberCredentials(string ClientId, string ClientSecret, Uri LoginUri);

/// <summary>
/// The transport's <see cref="IAuthenticationTokenHandler"/> over the subscriber ECA: a direct
/// client-credentials fetch per call — fresh every time, no cache — because the transport owns token
/// caching and invalidation. Mirrors the TestHost's handler; the REST publisher authenticates
/// separately through the Salesforce lib with the publisher ECA.
/// </summary>
public sealed class SubscriberTokenHandler : IAuthenticationTokenHandler
{
    private static readonly HttpClient Http = new();
    private readonly SubscriberCredentials _credentials;

    public SubscriberTokenHandler(SubscriberCredentials credentials)
        => _credentials = credentials ?? throw new ArgumentNullException(nameof(credentials));

    public async Task<AuthenticationTokenResponse> GetAuthenticationTokenAsync()
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, new Uri(_credentials.LoginUri, "services/oauth2/token"))
        {
            Content = new FormUrlEncodedContent(new List<KeyValuePair<string, string>>
            {
                new("grant_type", "client_credentials"),
                new("client_id", _credentials.ClientId),
                new("client_secret", _credentials.ClientSecret),
            })
        };

        using var resp = await Http.SendAsync(req).ConfigureAwait(false);
        var raw = await resp.Content.ReadAsStringAsync().ConfigureAwait(false);

        if (!resp.IsSuccessStatusCode)
            throw new InvalidOperationException($"Salesforce subscriber token request failed ({(int)resp.StatusCode} {resp.StatusCode}): {raw}");

        var token = JsonSerializer.Deserialize<SalesforceTokenResponse>(raw)
                    ?? throw new InvalidOperationException("Could not deserialize Salesforce authentication response");

        return new AuthenticationTokenResponse(token.AccessToken!, token.InstanceUrl!, token.TenantId!);
    }
}
