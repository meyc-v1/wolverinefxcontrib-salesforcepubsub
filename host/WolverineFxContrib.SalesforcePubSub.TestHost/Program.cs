using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Wolverine;
using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.TestHost;
using WolverineFxContrib.SalesforcePubSub.TestHost.Events;

var builder = Host.CreateApplicationBuilder(args);

var sf = builder.Configuration.GetSection("Salesforce").Get<SalesforceTestHostOptions>() ?? new SalesforceTestHostOptions();
builder.Services.AddSingleton(sf);

builder.Services.AddHttpClient<PlatformEventPublisher>();
builder.Services.AddHostedService<PublisherWorker>();

builder.UseWolverine(opts =>
{
    opts.UseSalesforcePubSub(s => s.PubSubUri = new Uri(sf.PubSubUri))
        .UseAuthenticationHandler<ConfigAuthenticationTokenHandler>();

    // Test Event One → managed event subscription (server-side replay)
    opts.ListenToManagedSubscription<TestEventOne>(sf.TestEventOneManagedSubscription);

    // Test Event Two → topic subscription (client-side replay)
    opts.ListenToSalesforceTopic<TestEventTwo>(sf.TestEventTwoChannel);
});

var host = builder.Build();
host.Run();
