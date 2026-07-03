using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Wolverine;
using Wolverine.Configuration;
using Wolverine.SalesforcePubSub;
using Wolverine.SqlServer;
using Wolverine.Transports.SharedMemory;
using WolverineFxContrib.SalesforcePubSub.TestHost;
using WolverineFxContrib.SalesforcePubSub.TestHost.Events;
using WolverineFxContrib.SalesforcePubSub.TestHost.Replay;
using WolverineFxContrib.SalesforcePubSub.TestHost.Salesforce;
using WolverineFxContrib.SalesforcePubSub.TestHost.Settings;

var builder = Host.CreateApplicationBuilder(args);

// OTEL config first, then re-add appsettings.json last so it keeps precedence (mirrors the internal host).
builder.Configuration.AddJsonFile("appsettings.otel.json", optional: true, reloadOnChange: true);
builder.Configuration.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

var sf = builder.Configuration.GetSection("salesforceSettings").Get<SalesforceSettings>() ?? new SalesforceSettings();
// Apply the same defaults the options pipeline does, so the bootstrap read below matches IOptions consumers.
new SalesforceSettingsConfigurer().PostConfigure(Microsoft.Extensions.Options.Options.DefaultName, sf);

// Logging + OpenTelemetry, mirroring the internal host. Console formatter/log levels come from
// appsettings.otel.json; traces (HTTP token calls + the Pub/Sub gRPC stream) and logs export via OTLP.
if (builder.Environment.IsProduction())
    builder.Logging.ClearProviders();  // clear these out to improve perf in prod

builder.Services.ConfigureOpenTelemetryTracerProvider(providerBuilder =>
{
    providerBuilder
        .AddHttpClientInstrumentation(opts =>
        {
            opts.EnrichWithHttpRequestMessage = (activity, request) =>
            {
                activity.DisplayName = $"{request.Method} {request.RequestUri?.Host}";
            };
        })
        .AddGrpcClientInstrumentation(opts =>
        {
            opts.SuppressDownstreamInstrumentation = true;
        });
});

// See https://github.com/open-telemetry/opentelemetry-dotnet/blob/main/src/OpenTelemetry.Exporter.OpenTelemetryProtocol/README.md for env var info.
builder.Services.AddOpenTelemetry().UseOtlpExporter();

// Salesforce auth + REST client, mirroring the internal client (own token client; no deprecated package).
builder.Services.AddSalesforceAuthentication(s => builder.Configuration.GetSection("salesforceAuthenticationSettings").Bind(s));
builder.Services.AddSalesforce(s => builder.Configuration.GetSection("salesforceSettings").Bind(s));

// Durability-run harness: shared counters, periodic heartbeat snapshot, and the opt-in timed publisher.
builder.Services.AddSingleton<RunMetrics>();
// Heartbeat defaults on; set "heartbeat:enabled" false to silence the 60s snapshot (e.g. a long idle
// baseline where the per-tick line is just noise). RunMetrics stays registered — handlers depend on it.
if (builder.Configuration.GetValue("heartbeat:enabled", true))
    builder.Services.AddHostedService<HeartbeatService>();
builder.Services.Configure<PublisherSettings>(builder.Configuration.GetSection("publisherSettings"));
builder.Services.AddHostedService<PublisherWorker>();

// Replay store / fault injection. Registered before UseWolverine so the transport's TryAdd default
// (in-memory) is skipped. Precedence: bad-replay fault seam (isolated test) > persistent SQL store >
// lib in-memory default. Topic only — MES uses server-side replay.
builder.Services.Configure<ReplaySettings>(builder.Configuration.GetSection("salesforceReplaySettings"));
var replay = builder.Configuration.GetSection("salesforceReplaySettings").Get<ReplaySettings>() ?? new ReplaySettings();
if (replay.SeedBadReplayId is { } badReplayId)
{
    builder.Services.AddSingleton<IReplayIdRepository>(sp =>
        new FaultInjectingReplayIdRepository(badReplayId, sp.GetRequiredService<ILogger<FaultInjectingReplayIdRepository>>()));
}
else if (!string.IsNullOrWhiteSpace(replay.ConnectionString))
{
    SqlAadAuthentication.Register();
    builder.Services.AddSingleton<IReplayIdRepository, SqlReplayIdRepository>();
}

builder.UseWolverine(opts =>
{
    opts.UseSalesforcePubSub(sf.PubSubUri)
        .UseAuthenticationHandler<SalesforceAuthenticationTokenHandler>();

    // Durable-mode message store (inbox/DLQ), opt-in: only wired when a connection string is configured
    // (user secrets — "durabilitySettings:connectionString"). Endpoints opt in per-sub via "mode": "Durable".
    var durabilityConnectionString = builder.Configuration.GetValue<string>("durabilitySettings:connectionString");
    if (!string.IsNullOrWhiteSpace(durabilityConnectionString))
        opts.PersistMessagesWithSqlServer(durabilityConnectionString);

    foreach (var sub in sf.Subscriptions)
    {
        if (!sub.Enabled)
            continue;

        // Two kinds, split on who manages replay (DECISIONS #19); "Channel" is accepted as a config
        // alias for Topic (a custom channel is a topic to the Pub/Sub API).
        var listener = sub.Type == SalesforceSubscriptionType.ManagedSubscription
            ? opts.ListenToManagedSubscription(sub.Channel)
            : opts.ListenToSalesforceTopic(sub.Channel);

        foreach (var evt in sub.Events)
        {
            var eventType = ResolveEventType(evt.MessageType)
                ?? throw new InvalidOperationException($"Could not resolve message type '{evt.MessageType}' for channel '{sub.Channel}'.");

            // Name from config when given; otherwise the type's [SalesforcePlatformEvent] attribute.
            if (string.IsNullOrWhiteSpace(evt.EventApiName))
                listener.MapEvent(eventType);
            else
                listener.MapEvent(eventType, evt.EventApiName);
        }

        switch (sub.Mode)
        {
            case EndpointMode.Durable:
                if (string.IsNullOrWhiteSpace(durabilityConnectionString))
                    throw new InvalidOperationException(
                        $"Subscription '{sub.Channel}' is configured with mode Durable, but no message store is wired — set the 'durabilitySettings:connectionString' user secret (or change the mode). Without a store, Durable would silently run without persistence.");
                listener.UseDurableInbox();
                break;
            case EndpointMode.BufferedInMemory:
                listener.BufferedInMemory();
                break;
        }

        // Per-endpoint resiliency knobs (e.g. a short FetchTimeout to force the timeout-loop test).
        if (sub.FetchTimeout is { } timeout)
            listener.FetchTimeout(timeout);
        if (sub.StartFromEarliest is { } earliest)
            listener.StartFromEarliest(earliest);
        if (sub.HeartbeatInterval is { } heartbeat)
            listener.Heartbeat.Interval(heartbeat);
        if (sub.WatchdogThreshold is { } watchdogThreshold)
            listener.Watchdog.Threshold(watchdogThreshold);
    }
});

var host = builder.Build();

// The replay-repository fallback chain is silent by design in the transport; in the harness, make the
// in-memory default loud — a replay position that doesn't survive a restart invalidates recovery tests.
if (replay.SeedBadReplayId is null && string.IsNullOrWhiteSpace(replay.ConnectionString))
{
    host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Program").LogWarning(
        "No 'salesforceReplaySettings:connectionString' configured; topic/channel replay positions are IN-MEMORY ONLY and will not survive a restart.");
}

host.Run();

static Type? ResolveEventType(string name)
    => Type.GetType(name)
       ?? Assembly.GetExecutingAssembly().GetTypes().FirstOrDefault(t => t.Name == name || t.FullName == name);

static void ScratchTestConfig(IHostBuilder bulder)
{
    bulder.UseWolverine(options =>
    {
        options.UseSalesforcePubSub()
            .UseAuthenticationHandler<SalesforceAuthenticationTokenHandler>();
            //probably should have a .UseBackoff, to register a custom backoff impl
            //probably should have a .UseReplay for being specific about the ReplayRepository impl, in the existing setup it is registered singleton against the service provider
            //I think it is better to think about replay from the scope of the regsitration of the transport
            //Can we do the Heartbeat like an intermediate so .Heartbeat.Disable(), or .Heartbeat.SetInterval()
            //Same for Watchdog, .Watchdog.Disable(), or .WatchDog.SetInterval() and .WatchDog.SetThreshold?  Cleans you the amount of "stuff" you see an groups properties better


            //Is the "SalesforcePubSubConfiguration" that is returned by UseSalesforcePubSub standard to Woverine?  I want to make sure this isn't like the endpoint.  There's a type that 
            //it implements(from Wolverine) that it extends and returns.  I want to make sure it shouldn't be the same for the registration of the Transport
            var test1 = options.UseSalesforcePubSub();

            //MES registration things we need
            //mes subscription name
            //api platform event name if using MapEvent

            var testEventOneApiName = "CM_Test_Event_One__e";
            var testEventTwoApiName = "CM_Test_Event_Two__e";
            
            //single event MES registration.  The two below should be about equivalent
            var singleEventMesName = "CM_Test_Event_One";  //I should have named this different so its name didn't correspond so closely to the Event.  That is coincidental
            var multiEventMesName = "CM_Test_Channel_Sub";
            var singleEventChannelName = "/event/CM_Test_Event_Two__e";
            var multiEventChannelName = "/event/CM_Test_Channel__chn";

            //use this as the way to register MES, be explicit about what its name is and what PEs it contains, is consistent for 1:1 MES:Event or 1:N MES:Events
            
            //MES
            options.ListenToManagedSubscription(singleEventMesName)
                .MapEvent<TestEventOne>(testEventOneApiName);

            options.ListenToManagedSubscription(multiEventMesName)
                .MapEvent<TestEventOne>(testEventOneApiName)
                .MapEvent<TestEventTwo>(testEventTwoApiName);
            
            //Channel or Topic?
            options.ListenToSalesforceTopic(singleEventChannelName)
                .MapEvent<TestEventTwo>(testEventTwoApiName);

            options.ListenToSalesforceTopic(multiEventChannelName)
                .MapEvent<TestEventOne>(testEventOneApiName)
                .MapEvent<TestEventTwo>(testEventTwoApiName);

            //I think the same .Heartbeat, and .Watchdog guidance applies here if possible







    });
}
