using External.Salesforce.Models;

namespace External.Salesforce;

public interface ISalesforceTokenClient
{
    Task<SalesforceTokenResponse> GetTokenResponseAsync(bool refresh = false);
}
