using System.Net.Http.Headers;
using a deprecated shared auth package;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// DelegatingHandler that attaches a bearer token (from the shared <see cref="ISalesforceTokenClient"/>)
/// to outgoing REST requests. Lifted from the original runner.
/// </summary>
internal sealed class SalesforceHandler : DelegatingHandler
{
    private readonly ISalesforceTokenClient _client;

    public SalesforceHandler(ISalesforceTokenClient client)
        => _client = client ?? throw new ArgumentNullException(nameof(client));

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var token = await _client.GetTokenResponseAsync();

        if (token?.AccessToken is null)
            throw new InvalidOperationException("Unable to get Salesforce access token");

        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.AccessToken);
        return await base.SendAsync(request, cancellationToken);
    }
}
