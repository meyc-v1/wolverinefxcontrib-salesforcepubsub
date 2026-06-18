namespace Wolverine.SalesforcePubSub;

/// <summary>
/// Supplies the access token and instance/tenant metadata required to authenticate against
/// the Salesforce Pub/Sub API. Implementations generate or retrieve (and cache/refresh) tokens.
/// </summary>
public interface IAuthenticationTokenHandler
{
    Task<AuthenticationTokenResponse> GetAuthenticationTokenAsync();
}

public sealed class AuthenticationTokenResponse
{
    public string AccessToken { get; }
    public string InstanceUri { get; }
    public string TenantId { get; }

    public AuthenticationTokenResponse(string accessToken, string instanceUri, string tenantId)
    {
        AccessToken = accessToken;
        InstanceUri = instanceUri;
        TenantId = tenantId;
    }
}
