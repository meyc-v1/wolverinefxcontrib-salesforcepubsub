using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Wolverine;
using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.TestHost;

var builder = Host.CreateApplicationBuilder(args);

var sf = builder.Configuration.GetSection("Salesforce").Get<SalesforceTestHostOptions>() ?? new SalesforceTestHostOptions();
builder.Services.AddSingleton(sf);

builder.Services.AddHttpClient<PlatformEventPublisher>();
builder.Services.AddHostedService<PublisherWorker>();

builder.UseWolverine(opts =>
{
    opts.UseSalesforcePubSub(s => s.PubSubUri = new Uri(sf.PubSubUri))
        .UseAuthenticationHandler<ConfigAuthenticationTokenHandler>();

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
