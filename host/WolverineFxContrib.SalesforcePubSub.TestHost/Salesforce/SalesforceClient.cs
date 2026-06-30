using System.Net.Http.Json;

namespace WolverineFxContrib.SalesforcePubSub.TestHost.Salesforce;

/// <summary>Publishes the test platform events via the Salesforce REST API. Lifted from the original runner.</summary>
public interface ISalesforceClient
{
    /// <summary>POST a platform event by its sObject API name (e.g. <c>CM_Test_Event_Two__e</c>) with a Message__c body.</summary>
    Task SendPlatformEventAsync(string sObjectApiName, string message, CancellationToken token = default);

    Task SendPlatformTestEventOneAsync(string message, CancellationToken token = default);
    Task SendPlatformTestEventTwoAsync(string message, CancellationToken token = default);
}

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

    public Task SendPlatformTestEventOneAsync(string message, CancellationToken token = default)
        => SendPlatformEventAsync("CM_Test_Event_One__e", message, token);

    public Task SendPlatformTestEventTwoAsync(string message, CancellationToken token = default)
        => SendPlatformEventAsync("CM_Test_Event_Two__e", message, token);
}
