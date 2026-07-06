using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Wolverine;
using Wolverine.Configuration;
using Wolverine.SalesforcePubSub;
using Wolverine.SqlServer;
using Wolverine.Transports.SharedMemory;
using External.Salesforce;
using SqlReplay;
using TestHost;
using TestHost.Events;
using TestHost.Replay;
using TestHost.Salesforce;
using TestHost.Settings;

var builder = Host.CreateApplicationBuilder(args);

// OTEL config first, then re-add appsettings.json last so it keeps precedence.
builder.Configuration.AddJsonFile("appsettings.otel.json", optional: true, reloadOnChange: true);
builder.Configuration.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

var sf = builder.Configuration.GetSection("salesforceSettings").Get<SalesforcePubSubSettings>() ?? new SalesforcePubSubSettings();
// Apply the same defaults the options pipeline does, then validate eagerly — nothing resolves
// IOptions<SalesforcePubSubSettings> (this bootstrap read drives the wiring), so this is where
// subscription config errors surface. Previously they hid until the publisher resolved the shared
// settings class.
var sfConfigurer = new SalesforcePubSubSettingsConfigurer();
sfConfigurer.PostConfigure(Microsoft.Extensions.Options.Options.DefaultName, sf);
var sfValidation = sfConfigurer.Validate(Microsoft.Extensions.Options.Options.DefaultName, sf);
if (sfValidation.Failed)
    throw new InvalidOperationException($"salesforceSettings validation failed: {sfValidation.FailureMessage}");

// Logging + OpenTelemetry. Console formatter/log levels come from
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

// Two ECAs, two token lifecycles: the External.Salesforce lib (REST publisher) authenticates with
// the publisher ECA; the transport authenticates with the subscriber ECA via the direct-fetch
// SalesforceAuthenticationTokenHandler (no cache — the transport owns caching/invalidation).
builder.Services.AddSalesforceAuthentication(s => builder.Configuration.GetSection("publisherAuthenticationSettings").Bind(s));
builder.Services.AddSalesforce(s => builder.Configuration.GetSection("salesforceSettings").Bind(s));

builder.Services.Configure<SubscriberAuthenticationSettings>(builder.Configuration.GetSection("subscriberAuthenticationSettings"));
builder.Services.ConfigureOptions<SubscriberAuthenticationSettingsConfigurer>();
builder.Services.AddHttpClient(SalesforceAuthenticationTokenHandler.HttpClientName);

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
// The SqlReplay lib binds its SQL detail (application/instance/schema/table) from the same section.
builder.Services.Configure<SqlReplaySettings>(builder.Configuration.GetSection("salesforceReplaySettings"));
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
            ? opts.ListenToManagedSubscription(sub.Resource)
            : opts.ListenToSalesforceTopic(sub.Resource);

        foreach (var evt in sub.Events)
        {
            var eventType = ResolveEventType(evt.MessageType)
                ?? throw new InvalidOperationException($"Could not resolve message type '{evt.MessageType}' for resource '{sub.Resource}'.");

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
                        $"Subscription '{sub.Resource}' is configured with mode Durable, but no message store is wired — set the 'durabilitySettings:connectionString' user secret (or change the mode). Without a store, Durable would silently run without persistence.");
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
