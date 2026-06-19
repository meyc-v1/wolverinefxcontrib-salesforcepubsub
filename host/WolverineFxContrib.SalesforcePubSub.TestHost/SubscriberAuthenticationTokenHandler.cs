using a deprecated shared auth package;
using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// Bridges the shared <see cref="ISalesforceTokenClient"/> (a deprecated shared auth package) to the
/// pub/sub transport's <see cref="IAuthenticationTokenHandler"/>. Lifted from the original runner.
/// </summary>
internal sealed class SubscriberAuthenticationTokenHandler : IAuthenticationTokenHandler
{
    private readonly ISalesforceTokenClient _client;

    public SubscriberAuthenticationTokenHandler(ISalesforceTokenClient client)
        => _client = client ?? throw new ArgumentNullException(nameof(client));

    public async Task<AuthenticationTokenResponse> GetAuthenticationTokenAsync()
    {
        var sfToken = await _client.GetTokenResponseAsync();
        return new AuthenticationTokenResponse(sfToken.AccessToken!, sfToken.InstanceUrl!, sfToken.TenantId!);
    }
}
