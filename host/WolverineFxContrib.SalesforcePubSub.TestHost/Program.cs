using System.Reflection;
using a deprecated shared auth package;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Wolverine;
using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.TestHost;

var builder = Host.CreateApplicationBuilder(args);

var sf = builder.Configuration.GetSection("salesforceSettings").Get<SalesforceSettings>() ?? new SalesforceSettings();
builder.Services.AddSingleton(sf);

// Salesforce auth (client-credentials) from the salesforceAuthenticationSettings section (in user-secrets).
builder.Services.AddSalesforceAuthentication(settings =>
    builder.Configuration.GetSection("salesforceAuthenticationSettings").Bind(settings));

// REST client for publishing platform events, bearer-authed via the shared token client.
builder.Services.AddTransient<SalesforceHandler>();
builder.Services.AddHttpClient<ISalesforceClient, SalesforceClient>(client => client.BaseAddress = sf.BaseUri)
    .AddHttpMessageHandler<SalesforceHandler>();

builder.Services.AddHostedService<PublisherWorker>();

builder.UseWolverine(opts =>
{
    opts.UseSalesforcePubSub(s => s.PubSubUri = sf.PubSubUri)
        .UseAuthenticationHandler<SubscriberAuthenticationTokenHandler>();

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
