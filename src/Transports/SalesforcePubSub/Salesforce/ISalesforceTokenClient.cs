using Salesforce.Models;

namespace Salesforce;

public interface ISalesforceTokenClient
{
    Task<SalesforceTokenResponse> GetTokenResponseAsync(bool refresh = false);
}
