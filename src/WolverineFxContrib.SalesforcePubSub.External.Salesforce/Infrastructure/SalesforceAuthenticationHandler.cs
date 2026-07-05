using System.Net.Http.Headers;

namespace WolverineFxContrib.SalesforcePubSub.External.Salesforce.Infrastructure;

/// <summary>
/// DelegatingHandler that attaches the bearer token (from <see cref="ISalesforceTokenClient"/>) to
/// outgoing REST requests. Ported from an internal Salesforce client.
/// </summary>
internal sealed class SalesforceAuthenticationHandler : DelegatingHandler
{
    private readonly ISalesforceTokenClient _client;

    public SalesforceAuthenticationHandler(ISalesforceTokenClient client)
        => _client = client ?? throw new ArgumentNullException(nameof(client));

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var token = await _client.GetTokenResponseAsync().ConfigureAwait(false);

        if (token == null)
            throw new InvalidOperationException("Failed to get token");

        if (string.IsNullOrWhiteSpace(token.TokenType) || string.IsNullOrWhiteSpace(token.AccessToken))
            throw new InvalidOperationException("Token type or access token is empty");

        request.Headers.Authorization = new AuthenticationHeaderValue(token.TokenType, token.AccessToken);
        return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
    }
}
