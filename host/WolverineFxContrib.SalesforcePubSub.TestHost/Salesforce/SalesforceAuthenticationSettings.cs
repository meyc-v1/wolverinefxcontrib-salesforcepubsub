namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// Salesforce client-credentials auth, bound from the "salesforceAuthenticationSettings" section
/// (in user-secrets). Mirrors the internal client's settings.
/// </summary>
public sealed class SalesforceAuthenticationSettings
{
    public string ClientId { get; set; } = null!;
    public string ClientSecret { get; set; } = null!;
    public Uri LoginUri { get; set; } = null!;
    public int TokenCachingInMinutes { get; set; } = 60;
}
