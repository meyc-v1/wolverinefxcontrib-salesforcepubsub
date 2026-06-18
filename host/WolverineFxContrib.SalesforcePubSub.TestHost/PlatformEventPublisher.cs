using System.Net.Http.Headers;
using System.Net.Http.Json;
using Microsoft.Extensions.Logging;
using Wolverine.SalesforcePubSub;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// Publishes platform events by POSTing to the Salesforce sObjects REST endpoint — a platform event
/// behaves like an entity for creation. Uses the same <see cref="IAuthenticationTokenHandler"/> as
/// the transport, so one token configuration drives both publish and subscribe.
/// </summary>
public sealed class PlatformEventPublisher
{
    private readonly HttpClient _http;
    private readonly IAuthenticationTokenHandler _auth;
    private readonly SalesforceTestHostOptions _options;
    private readonly ILogger<PlatformEventPublisher> _logger;

    public PlatformEventPublisher(HttpClient http, IAuthenticationTokenHandler auth, SalesforceTestHostOptions options, ILogger<PlatformEventPublisher> logger)
    {
        _http = http;
        _auth = auth;
        _options = options;
        _logger = logger;
    }

    public Task PublishTestEventOneAsync(CancellationToken ct = default)
        => PublishAsync(_options.TestEventOneSObject, new { }, ct);

    public Task PublishTestEventTwoAsync(CancellationToken ct = default)
        => PublishAsync(_options.TestEventTwoSObject, new { }, ct);

    /// <summary>POSTs <paramref name="body"/> as the platform event payload to the sObjects REST endpoint.</summary>
    public async Task PublishAsync(string sObjectApiName, object body, CancellationToken ct = default)
    {
        var token = await _auth.GetAuthenticationTokenAsync().ConfigureAwait(false);
        var url = $"{token.InstanceUri.TrimEnd('/')}/services/data/{_options.ApiVersion}/sobjects/{sObjectApiName}";

        using var request = new HttpRequestMessage(HttpMethod.Post, url) { Content = JsonContent.Create(body) };
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.AccessToken);

        _logger.LogInformation("Publishing platform event to {Url}", url);

        using var response = await _http.SendAsync(request, ct).ConfigureAwait(false);
        var responseBody = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);

        if (response.IsSuccessStatusCode)
            _logger.LogInformation("Published {SObject}: {Status} {Body}", sObjectApiName, (int)response.StatusCode, responseBody);
        else
            _logger.LogError("Publish {SObject} failed: {Status} {Body}", sObjectApiName, (int)response.StatusCode, responseBody);
    }
}
