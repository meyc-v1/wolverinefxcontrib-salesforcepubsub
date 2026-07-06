namespace External.Salesforce;

/// <summary>Publishes platform events via the Salesforce REST API (publisher-side only — the
/// pub/sub transport authenticates separately through its own <c>IAuthenticationTokenHandler</c>).</summary>
public interface ISalesforceClient
{
    /// <summary>POST a platform event by its sObject API name (e.g. <c>WIT_Event_A__e</c>) with a Message__c body.</summary>
    Task SendPlatformEventAsync(string sObjectApiName, string message, CancellationToken token = default);
}
