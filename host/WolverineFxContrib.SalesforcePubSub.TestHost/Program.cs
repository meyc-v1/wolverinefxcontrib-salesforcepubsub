using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Wolverine;
using Wolverine.SalesforcePubSub;
using WolverineFxContrib.SalesforcePubSub.TestHost;
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

    foreach (var sub in sf.Subscriptions)
    {
        if (!sub.Enabled)
            continue;

        var messageType = ResolveEventType(sub.MessageType)
            ?? throw new InvalidOperationException($"Could not resolve message type '{sub.MessageType}' for channel '{sub.Channel}'.");

        var listener = sub.Type == SalesforceResourceKind.ManagedSubscription
            ? opts.ListenToManagedSubscription(sub.Channel, messageType)
            : opts.ListenToSalesforceTopic(sub.Channel, messageType);

        // Per-endpoint resiliency knobs (e.g. a short FetchTimeout to force the timeout-loop test).
        if (sub.FetchTimeout is { } timeout)
            listener.FetchTimeout(timeout);
        if (sub.StartFromEarliest is { } earliest)
            listener.StartFromEarliest(earliest);
        if (sub.HeartbeatInterval is { } heartbeat)
            listener.HeartbeatInterval(heartbeat);
        if (sub.StaleStreamThreshold is { } staleThreshold)
            listener.StaleStreamThreshold(staleThreshold);
    }
});

var host = builder.Build();
host.Run();

static Type? ResolveEventType(string name)
    => Type.GetType(name)
       ?? Assembly.GetExecutingAssembly().GetTypes().FirstOrDefault(t => t.Name == name || t.FullName == name);
