using System.Reflection;
using Wolverine;
using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.TestHost;
using WolverineFxContrib.SalesforcePubSub.TestHost.Salesforce;
using WolverineFxContrib.SalesforcePubSub.TestHost.Settings;

var builder = Host.CreateApplicationBuilder(args);

var sf = builder.Configuration.GetSection("salesforceSettings").Get<SalesforceSettings>() ?? new SalesforceSettings();
// Apply the same defaults the options pipeline does, so the bootstrap read below matches IOptions consumers.
new SalesforceSettingsConfigurer().PostConfigure(Microsoft.Extensions.Options.Options.DefaultName, sf);

// Salesforce auth + REST client, mirroring the internal client (own token client; no deprecated package).
builder.Services.AddSalesforceAuthentication(s => builder.Configuration.GetSection("salesforceAuthenticationSettings").Bind(s));
builder.Services.AddSalesforce(s => builder.Configuration.GetSection("salesforceSettings").Bind(s));

builder.Services.AddHostedService<PublisherWorker>();



builder.UseWolverine(opts =>
{
    opts.UseSalesforcePubSub(s => s.PubSubUri = sf.PubSubUri)
        .UseAuthenticationHandler<SalesforceAuthenticationTokenHandler>();

    foreach (var sub in sf.Subscriptions)
    {
        var messageType = ResolveEventType(sub.MessageType)
            ?? throw new InvalidOperationException($"Could not resolve message type '{sub.MessageType}' for channel '{sub.Channel}'.");

        if (sub.Type == SalesforceResourceKind.ManagedSubscription)
            opts.ListenToManagedSubscription(sub.Channel, messageType);
        else
            opts.ListenToSalesforceTopic(sub.Channel, messageType);
    }
});

var host = builder.Build();
host.Run();

static Type? ResolveEventType(string name)
    => Type.GetType(name)
       ?? Assembly.GetExecutingAssembly().GetTypes().FirstOrDefault(t => t.Name == name || t.FullName == name);
