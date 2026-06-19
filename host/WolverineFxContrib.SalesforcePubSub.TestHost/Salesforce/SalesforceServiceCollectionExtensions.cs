using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace WolverineFxContrib.SalesforcePubSub.TestHost;

/// <summary>
/// Registration mirroring the internal Salesforce client lib: a token client (auth) plus a bearer-authed
/// REST client. Polly retry and FluentValidation validation are omitted for the test host.
/// </summary>
public static class SalesforceServiceCollectionExtensions
{
    /// <summary>Registers the client-credentials token client (cached) from <see cref="SalesforceAuthenticationSettings"/>.</summary>
    public static IServiceCollection AddSalesforceAuthentication(this IServiceCollection services, Action<SalesforceAuthenticationSettings> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        services.AddMemoryCache();
        services.AddHttpClient<ISalesforceTokenClient, SalesforceTokenClient>((provider, client) =>
        {
            client.BaseAddress = provider.GetRequiredService<IOptions<SalesforceAuthenticationSettings>>().Value.LoginUri;
        });

        return services;
    }

    /// <summary>Registers the bearer-authed REST client (<see cref="ISalesforceClient"/>) from <see cref="SalesforceSettings"/>.</summary>
    public static IServiceCollection AddSalesforce(this IServiceCollection services, Action<SalesforceSettings> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        services.AddTransient<SalesforceAuthenticationHandler>();
        services.AddHttpClient<ISalesforceClient, SalesforceClient>((provider, client) =>
        {
            client.BaseAddress = provider.GetRequiredService<IOptions<SalesforceSettings>>().Value.BaseUri;
        }).AddHttpMessageHandler<SalesforceAuthenticationHandler>();

        return services;
    }
}
