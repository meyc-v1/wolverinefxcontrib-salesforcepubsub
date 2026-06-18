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

    // Test Event One → topic subscription (client-side replay)
    opts.ListenToSalesforceTopic<TestEventOne>(sf.TestEventOneChannel);

    // Test Event Two → managed event subscription (server-side replay)
    opts.ListenToManagedSubscription<TestEventTwo>(sf.TestEventTwoManagedSubscription);
});

var host = builder.Build();
host.Run();
