using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using WolverineFxContrib.SalesforcePubSub.External.Salesforce.Infrastructure;
using WolverineFxContrib.SalesforcePubSub.External.Salesforce.Settings;

namespace WolverineFxContrib.SalesforcePubSub.External.Salesforce;

/// <summary>
/// Registration mirroring the internal Salesforce client lib: a token client (auth) plus a bearer-authed
/// REST client for publishing platform events. Settings defaults + FluentValidation run via the
/// *Configurer options bindings; Polly retry is omitted.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>Registers the client-credentials token client (cached) from <see cref="SalesforceAuthenticationSettings"/>.</summary>
    public static IServiceCollection AddSalesforceAuthentication(this IServiceCollection services, Action<SalesforceAuthenticationSettings> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        services.ConfigureOptions<SalesforceAuthenticationSettingsConfigurer>();
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
        services.ConfigureOptions<SalesforceSettingsConfigurer>();
        services.AddTransient<SalesforceAuthenticationHandler>();
        services.AddHttpClient<ISalesforceClient, SalesforceClient>((provider, client) =>
        {
            client.BaseAddress = provider.GetRequiredService<IOptions<SalesforceSettings>>().Value.BaseUri;
        }).AddHttpMessageHandler<SalesforceAuthenticationHandler>();

        return services;
    }
}
