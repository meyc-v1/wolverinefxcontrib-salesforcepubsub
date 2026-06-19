using System.Net.Http.Json;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Salesforce;

/// <summary>Publishes the test platform events via the Salesforce REST API. Lifted from the original runner.</summary>
public interface ISalesforceClient
{
    Task SendPlatformTestEventOneAsync(string message, CancellationToken token = default);
    Task SendPlatformTestEventTwoAsync(string message, CancellationToken token = default);
}

internal sealed class SalesforceClient : ISalesforceClient
{
    private readonly HttpClient _httpClient;

    public SalesforceClient(HttpClient httpClient)
        => _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));

    public async Task SendPlatformTestEventOneAsync(string message, CancellationToken token = default)
    {
        var payload = new { Message__c = message };
        using var resp = await _httpClient.PostAsJsonAsync("sobjects/CM_Test_Event_One__e", payload, token);
        resp.EnsureSuccessStatusCode();
    }

    public async Task SendPlatformTestEventTwoAsync(string message, CancellationToken token = default)
    {
        var payload = new { Message__c = message };
        using var resp = await _httpClient.PostAsJsonAsync("sobjects/CM_Test_Event_Two__e", payload, token);
        resp.EnsureSuccessStatusCode();
    }
}
