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
    /// <summary>
    /// Fetches a fresh token. The <paramref name="cancellationToken"/> is the requesting gRPC call's —
    /// honor it in the HTTP token request so a cancelled subscribe attempt doesn't leave a fetch running.
    /// </summary>
    Task<AuthenticationTokenResponse> GetAuthenticationTokenAsync(CancellationToken cancellationToken = default);
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
