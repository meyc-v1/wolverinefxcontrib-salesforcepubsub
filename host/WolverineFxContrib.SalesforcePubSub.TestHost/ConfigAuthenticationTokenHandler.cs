using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// Test-host auth handler that returns a token straight from configuration. Fine for manual
/// verification; replace with a real Salesforce OAuth handler for anything sustained (tokens expire).
/// </summary>
public sealed class ConfigAuthenticationTokenHandler : IAuthenticationTokenHandler
{
    private readonly SalesforceTestHostOptions _options;

    public ConfigAuthenticationTokenHandler(SalesforceTestHostOptions options) => _options = options;

    public Task<AuthenticationTokenResponse> GetAuthenticationTokenAsync()
        => Task.FromResult(new AuthenticationTokenResponse(_options.AccessToken, _options.InstanceUrl, _options.TenantId));
}
