using System.Net.Http.Json;

namespace External.Salesforce.Infrastructure;

internal sealed class SalesforceClient : ISalesforceClient
{
    private readonly HttpClient _httpClient;

    public SalesforceClient(HttpClient httpClient)
        => _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

    public async Task SendPlatformEventAsync(string sObjectApiName, string message, CancellationToken token = default)
    {
        var payload = new { Message__c = message };
        using var resp = await _httpClient.PostAsJsonAsync($"sobjects/{sObjectApiName}", payload, token);
        resp.EnsureSuccessStatusCode();
    }
}
