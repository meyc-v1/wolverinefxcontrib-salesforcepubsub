using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Salesforce;

namespace WolverineFxContrib.SalesforcePubSub.IntegrationTests.Harness;

/// <summary>
/// Assembly-wide fixture: loads the shared user-secrets store (id <c>wolverine.salesforcepubsub</c>,
/// same as the TestHost), fails fast with pointers when required config is missing, and provides the
/// REST publisher (publisher ECA, via the Salesforce lib) plus the subscriber credentials each
/// test host authenticates the transport with.
///
/// Org fixtures (WIT_ events / channel / MES) are permanent infra created per docs/org-setup/README.md —
/// this context verifies configuration, and a missing org fixture surfaces on the first subscribe with
/// the transport's own error.
/// </summary>
public sealed class SalesforceTestContext : IDisposable
{
    private readonly ServiceProvider _publisherServices;

    public SalesforceTestContext()
    {
        var config = new ConfigurationBuilder()
            .AddUserSecrets(typeof(SalesforceTestContext).Assembly, optional: true)
            .Build();

        SubscriberCredentials = new SubscriberCredentials(
            Require(config, "subscriberAuthenticationSettings:ClientId"),
            Require(config, "subscriberAuthenticationSettings:ClientSecret"),
            new Uri(Require(config, "subscriberAuthenticationSettings:LoginUri")));

        PubSubUri = config["salesforceSettings:pubSubUri"] is { Length: > 0 } uri
            ? new Uri(uri)
            : null; // transport default (the public Salesforce endpoint)

        DurabilityConnectionString = config["durabilitySettings:connectionString"] is { Length: > 0 } cs ? cs : null;

        // Publisher side: the Salesforce lib with the publisher ECA's credentials.
        var services = new ServiceCollection();
        services.AddSalesforceAuthentication(s => config.GetSection("publisherAuthenticationSettings").Bind(s));
        services.AddSalesforce(s => config.GetSection("salesforceSettings").Bind(s));
        _ = Require(config, "publisherAuthenticationSettings:ClientId");
        _ = Require(config, "salesforceSettings:baseUri");
        _publisherServices = services.BuildServiceProvider();
    }

    /// <summary>Credentials the test hosts hand to the transport's <c>SubscriberTokenHandler</c>.</summary>
    public SubscriberCredentials SubscriberCredentials { get; }

    /// <summary>Pub/Sub gRPC endpoint override, or null for the transport default.</summary>
    public Uri? PubSubUri { get; }

    /// <summary>
    /// SQL Server connection string for the Wolverine message store (Durable-mode tests). Optional —
    /// the Durable facts skip when it isn't configured (same "durabilitySettings:connectionString"
    /// secret the TestHost uses).
    /// </summary>
    public string? DurabilityConnectionString { get; }

    /// <summary>POSTs a platform event by API name with the given Message__c (the correlation id).</summary>
    public Task PublishAsync(string eventApiName, string message, CancellationToken ct = default)
        => _publisherServices.GetRequiredService<ISalesforceClient>().SendPlatformEventAsync(eventApiName, message, ct);

    private static string Require(IConfiguration config, string key)
        => config[key] is { Length: > 0 } value
            ? value
            : throw new InvalidOperationException(
                $"Missing required configuration '{key}' in user secrets (id 'wolverine.salesforcepubsub'). " +
                "The integration tests need the subscriber and publisher ECA credentials plus " +
                "salesforceSettings:baseUri — see docs/org-setup/README.md for the org fixtures and the TestHost " +
                "secrets layout for the sections.");

    public void Dispose() => _publisherServices.Dispose();
}
