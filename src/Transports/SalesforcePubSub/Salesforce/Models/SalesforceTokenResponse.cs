using System.Text.Json.Serialization;

namespace Salesforce.Models;

/// <summary>Salesforce OAuth token response.</summary>
public sealed class SalesforceTokenResponse
{
    [JsonPropertyName("access_token")] public string? AccessToken { get; set; }
    [JsonPropertyName("scope")] public string? Scope { get; set; }
    [JsonPropertyName("instance_url")] public string? InstanceUrl { get; set; }
    [JsonPropertyName("id")] public string? Id { get; set; }
    [JsonPropertyName("token_type")] public string? TokenType { get; set; }

    /// <summary>Org/tenant id parsed from the identity URL (https://login.../id/{orgId}/{userId}).</summary>
    public string? TenantId
    {
        get
        {
            if (Id == null)
                return null;

            return Uri.TryCreate(Id, UriKind.Absolute, out var uri)
                ? uri.Segments[2].TrimEnd('/')
                : null;
        }
    }
}
