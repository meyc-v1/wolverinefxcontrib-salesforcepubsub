using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Salesforce;

/// <summary>
/// Bridges the local <see cref="ISalesforceTokenClient"/> to the pub/sub transport's
/// <see cref="IAuthenticationTokenHandler"/>. Mirrors the internal client.SalesforceHandlers.
/// </summary>
/// <remarks>
/// The transport owns token caching/invalidation, so this fetches a fresh token (<c>refresh: true</c>)
/// rather than serving the token client's cached value — otherwise an invalidated transport cache would
/// just be re-handed the same (possibly revoked) token. The REST publisher keeps using the cached path.
/// </remarks>
internal sealed class SalesforceAuthenticationTokenHandler : IAuthenticationTokenHandler
{
    private readonly ISalesforceTokenClient _client;

    public SalesforceAuthenticationTokenHandler(ISalesforceTokenClient client)
        => _client = client ?? throw new ArgumentNullException(nameof(client));

    public async Task<AuthenticationTokenResponse> GetAuthenticationTokenAsync()
    {
        var sfToken = await _client.GetTokenResponseAsync(refresh: true);
        return new AuthenticationTokenResponse(sfToken.AccessToken!, sfToken.InstanceUrl!, sfToken.TenantId!);
    }
}
