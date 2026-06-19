using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Salesforce;

/// <summary>
/// Bridges the local <see cref="ISalesforceTokenClient"/> to the pub/sub transport's
/// <see cref="IAuthenticationTokenHandler"/>. Mirrors the internal client.SalesforceHandlers.
/// </summary>
internal sealed class SalesforceAuthenticationTokenHandler : IAuthenticationTokenHandler
{
    private readonly ISalesforceTokenClient _client;

    public SalesforceAuthenticationTokenHandler(ISalesforceTokenClient client)
        => _client = client ?? throw new ArgumentNullException(nameof(client));

    public async Task<AuthenticationTokenResponse> GetAuthenticationTokenAsync()
    {
        var sfToken = await _client.GetTokenResponseAsync();
        return new AuthenticationTokenResponse(sfToken.AccessToken!, sfToken.InstanceUrl!, sfToken.TenantId!);
    }
}
