namespace Wolverine.SalesforcePubSub;

/// <summary>
/// Supplies the access token and instance/tenant metadata required to authenticate against
/// the Salesforce Pub/Sub API.
/// <para>
/// Implementations should return a <b>freshly retrieved</b> token and must <b>not</b> cache it.
/// The transport owns caching (see <c>SubscriberComponentsSettings.TokenCacheDuration</c>) and
/// invalidates its cache on an authentication failure, calling this method again to obtain a new
/// token. A handler that caches internally would defeat that recovery and re-hand a revoked token.
/// </para>
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
