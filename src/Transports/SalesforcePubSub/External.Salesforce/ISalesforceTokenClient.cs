using WolverineFxContrib.SalesforcePubSub.External.Salesforce.Models;

namespace WolverineFxContrib.SalesforcePubSub.External.Salesforce;

public interface ISalesforceTokenClient
{
    Task<SalesforceTokenResponse> GetTokenResponseAsync(bool refresh = false);
}
