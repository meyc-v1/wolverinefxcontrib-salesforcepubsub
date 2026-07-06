# WolverineFxContrib.SalesforcePubSub

[![CI](https://github.com/meyc-v1/wolverinefxcontrib-salesforcepubsub/actions/workflows/ci.yml/badge.svg)](https://github.com/meyc-v1/wolverinefxcontrib-salesforcepubsub/actions/workflows/ci.yml)

> **Prerelease** — no NuGet package yet; the package id and versioning scheme are being confirmed
> with the WolverineFx maintainers. Until then, build from source.

A community **[Wolverine](https://wolverinefx.net/) transport for the Salesforce Pub/Sub gRPC API** —
Salesforce platform events arrive as ordinary Wolverine messages, handled by ordinary Wolverine handlers,
with Wolverine owning dispatch, retries, and error handling.

**Listen-only by design**: publishing a platform event is a plain REST POST and lives outside this
transport. Change Data Capture is currently out of scope (platform events only).

```csharp
builder.UseWolverine(opts =>
{
    opts.UseSalesforcePubSub()
        .UseAuthenticationHandler<MyTokenHandler>();

    opts.ListenToSalesforceTopic("/event/Order_Shipped__e")
        .MapEvent<OrderShipped>("Order_Shipped__e");
});
```

```csharp
public class OrderShipped : PlatformEvent
{
    public string? OrderNumber__c { get; set; }
}

public class OrderShippedHandler
{
    public void Handle(OrderShipped message, ILogger<OrderShippedHandler> logger)
        => logger.LogInformation("Order {Order} shipped (replay {ReplayId})",
            message.OrderNumber__c, message.ReplayId);
}
```

Requires **.NET 10** and **WolverineFx 6.12+**. Design decisions and their rationale live in
[DECISIONS.md](docs/DECISIONS.md).

## What you implement

Three small interfaces connect the transport to your infrastructure:

| Interface | Registered via | Default |
|---|---|---|
| `IAuthenticationTokenHandler` — fetches a Salesforce access token (`AccessToken`, `InstanceUri`, `TenantId`) | `UseAuthenticationHandler<T>()` | **required** — none |
| `IReplayIdRepository` — durable store for topic replay ids (per-resource watermark) | `UseReplayIdRepository<T>()` | in-memory (fine for dev; use a durable store in prod) |
| `IBackoffStrategy` — delay between reconnect attempts | `UseBackoffStrategy<T>()` | linear: +15s per consecutive error, capped at 2 min |

**Your token handler must fetch a fresh token every call and must not cache.** The transport owns
caching (default 60 min, `TokenCacheDuration(...)`) and — critically — owns *invalidation*: when
Salesforce rejects a token (revoked before expiry, a recurring production reality), the transport drops
the cached token and re-fetches on the next attempt. A handler that caches would defeat that recovery.

The topic replay repository is your at-least-once anchor: the transport commits the lowest
fully-handled replay id to it (throttled), and a cold start resumes from what it returns. Implement it
over a table keyed by (application, instance, topic).

## Subscription kinds

There are exactly two, split by **who manages the replay position** — and every subscription declares
each event it carries with `MapEvent<T>("Api_Name__e")`, keyed by the **event API name** (the Avro
record name of the event's schema). A single-event subscription is simply the one-entry case.

The name can also live on the type itself — it rarely changes, so tuck it away once:

```csharp
[SalesforcePlatformEvent("Order_Shipped__e")]
public class OrderShipped : PlatformEvent { … }

opts.ListenToSalesforceTopic("/event/Order_Shipped__e").MapEvent<OrderShipped>();
```

An explicit name at the registration site always wins over the attribute.

### Topic — client-managed replay (the recommended default)

The path may be a plain platform-event topic (`/event/X__e`, exactly one event type) or a custom channel
(`/event/X__chn`, one or more) — both are topics to the Pub/Sub API. Replay is tracked client-side via
your `IReplayIdRepository`:

```csharp
opts.ListenToSalesforceTopic("/event/Order_Shipped__e")
    .MapEvent<OrderShipped>("Order_Shipped__e");

opts.ListenToSalesforceTopic("/event/Order_Events__chn")
    .MapEvent<OrderShipped>("Order_Shipped__e")
    .MapEvent<OrderCancelled>("Order_Cancelled__e");
```

The listener resolves each event's type from its schema and Wolverine routes it to the matching
handler. An event arriving with **no mapping** is logged once (warning) and handed to Wolverine's
missing-handler policy — dead-lettered by default — and the replay position still advances. Putting
`[MessageIdentity("Some_Event__e")]` on a handled type is a zero-config way to pick up an unmapped
event. A `__e` topic validates at startup that its mapped name matches the path (a mismatch would
dead-letter everything). Because every entry is named, every endpoint eagerly pre-warms its schemas at
startup.

The same event type may be mapped on several endpoints (topic + channel + MES); handlers are routed by
type, and `Envelope.TopicName` identifies which subscription delivered a given message.

### Managed event subscription (MES) — Salesforce-managed replay

Salesforce tracks the replay position for you (`CommitReplayIdRequest` on the stream); no
`IReplayIdRepository` involved. The MES's server-side channel may carry one event type or several:

```csharp
opts.ListenToManagedSubscription("My_Managed_Sub")
    .MapEvent<InvoicePaid>("Invoice_Paid__e");

opts.ListenToManagedSubscription("My_Channel_Sub")
    .MapEvent<OrderShipped>("Order_Shipped__e")
    .MapEvent<OrderCancelled>("Order_Cancelled__e");
```

**Prefer topics, especially in production.** A MES slot is exclusive per client and has no
force-release: after an unclean disconnect (network partition, half-open socket) Salesforce holds the
slot for ~15 minutes, during which every reconnect fails with `ALREADY_EXISTS`. Clean stops and local
process crashes release it in seconds — but a true network partition costs the full window. Topics
reconnect instantly in every failure mode. Use MES only where that recovery window is tolerable and
Salesforce-managed checkpointing is specifically wanted. (DECISIONS #13.)

## Delivery guarantees

Per endpoint, via the standard Wolverine listener configuration:

| Mode | Guarantee | Crash / force-kill + restart (observed) | Poison message (observed) |
|---|---|---|---|
| `ProcessInline()` *(default)* | at-least-once | resumes from the durable replay position; the handled-but-uncommitted tail redelivers (duplicates bounded by the commit throttle; no loss) | retried per your error policies, then discarded with no store / a DLQ row with one |
| `BufferedInMemory()` | at-most-once — **and not cleanly so across restarts** | acked at receipt, before handling. Two windows, depending on where the kill lands relative to the throttled replay commit: events *received-committed-but-unhandled* are **lost**, and events *handled-but-uncommitted* **redeliver as duplicates**. Use it only where both loss *and* duplication are acceptable | the loss window applies; a handler failure otherwise behaves like Inline |
| `UseDurableInbox()` | at-least-once **with parallelism** + a real DLQ | persisted before processing; a crash mid-handle is recovered from the inbox on restart with full fidelity | preserved in the store's dead-letter table, replayable |

Every cell above is live-verified against a sandbox org (resiliency campaign + kill-window tests; evidence
logs in `docs/test-results/`).

Replay tracking is a per-envelope **watermark**: events are tracked on receive, and the committed
position advances only through fully-resolved envelopes — never past one still in flight. Handler
failures follow your Wolverine error policies (`OnException…`, `MoveToErrorQueue`, …); with no durable
store a dead-lettered message is discarded (no-op store), so poison-message *preservation* requires
Durable mode.

### Durable mode

Add a message store (e.g. `WolverineFx.SqlServer` — and note `Microsoft.Data.SqlClient` 7.x needs the
`Microsoft.Data.SqlClient.Extensions.Azure` package for Entra ID authentication) and opt endpoints in:

```csharp
opts.PersistMessagesWithSqlServer(connectionString);
opts.ListenToSalesforceTopic("/event/Order_Events__chn")
    .MapEvent<OrderShipped>("Order_Shipped__e")
    .MapEvent<OrderCancelled>("Order_Cancelled__e")
    .UseDurableInbox();
```

What you get, all verified live: incoming events are persisted before processing; a crash mid-handle is
recovered on restart (the envelope replays from the inbox — the serializer re-fetches the Avro schema by
its persisted id if the cache is cold, with token-invalidation-and-retry auth handling); poison messages
land in the store's dead-letter table, replayable; and duplicate deliveries dedup by a **deterministic
envelope id** derived from the Salesforce event UUID.

**Fan-out semantics are a deliberate dial.** The transport derives one message identity per Salesforce
event — so under Wolverine's default `MessageIdentity.IdOnly`, the *same event* fanned out to two Durable
endpoints (e.g. its topic and a channel) is processed **once**; the second copy is rejected as a
duplicate. If you *want* each endpoint to process its own copy independently, that's Wolverine's native
setting for exactly this scenario:

```csharp
opts.Durability.MessageIdentity = MessageIdentity.IdAndDestination;
```

(App-global; it widens the inbox primary key to id + endpoint.) Inline endpoints have no store and always
fan out.

## Event types

Events derive from the public base types in `Wolverine.SalesforcePubSub.Events`:

- `PubSubEvent` — `ReplayId` (stamped by the transport from the stream position)
- `PlatformEvent : PubSubEvent` — `CreatedById`, `CreatedDate` (Unix ms; also stamped into
  `Envelope.SentAt`)

Properties map to Avro schema fields by name — declare your custom fields exactly as Salesforce names
them (`OrderNumber__c`), nullable where the field is optional.

## Resilience & observability

The listener owns its own connect/backoff/reconnect loop and never faults out; a token rejected by
Salesforce is invalidated and re-fetched; schemas are pre-fetched in the consume loop (and eagerly
warmed at startup from the topic / `MapEvent` manifest) so auth failures surface to the reconnect path.
In-process reconnects resume from the in-memory handled watermark (no redelivery of handled events);
restarts resume from the durable store (at-least-once).

Every log line leads with the resource (`/event/X__e: …`) so interleaved endpoints read cleanly. Two
periodic signals per listener:

- **Heartbeat** — a counters line (uptime, responses, events, errors, reconnects, last success/error)
  every 15 min at `Information` by default.
- **Stale-stream watchdog** — if *nothing* (not even a keep-alive) has arrived for 15 min, logs
  `has not received a response in {Duration}` at `Error` **every minute until recovery**, and
  reconnect-failure logs escalate to the same level. This is the alertable "connected but silently cold"
  signal; healthy idle streams keep-alive roughly every 2 min and never trip it.

## Configuration reference

Transport-level, on the `SalesforcePubSubTransportExpression` returned by `UseSalesforcePubSub(...)`:
`UseAuthenticationHandler<T>()`, `UseReplayIdRepository<T>()` (replace the in-memory default with your
durable store), `UseBackoffStrategy<T>()`, `TokenCacheDuration(ts)`, and the grouped observability
knobs — `Heartbeat.Interval(ts)` / `.Level(lvl)` / `.Disable()` and `Watchdog.Threshold(ts)` /
`.PollingInterval(ts)` / `.Level(lvl)` / `.Disable()`. The gRPC endpoint defaults to
`https://api.pubsub.salesforce.com:7443` (override via the `pubSubUri` argument).

Per-endpoint (on the listener configuration, overriding the transport defaults):
`MapEvent<T>("Api_Name__e")`, `FetchCount(n)` (default 10), `FetchTimeout(ts)` (idle reconnect ceiling,
default 270s), `StartFromEarliest()` (client-replay subscriptions, cold start only), the same grouped
`Heartbeat.…` / `Watchdog.…` overrides, plus everything Wolverine's standard listener surface provides
(`ProcessInline`, `BufferedInMemory`, `UseDurableInbox`, `Sequential`, `MaximumParallelMessages`, …).

## Limitations

- **Listen-only** — no sender; publishing to an `sfpubsub://` URI throws.
- **Platform events only** — CDC is out of scope for now.
- **One listener per endpoint** — `ListenerCount > 1` fails at startup (parallel listeners would open
  duplicate subscriptions); use Durable mode for parallel *processing*.

## Building & testing

Requires the .NET 10 SDK (pinned by `global.json`).

```bash
dotnet build WolverineFxContrib.SalesforcePubSub.slnx

# Unit suite — no external dependencies, runs anywhere (this is what CI runs):
dotnet test src/Transports/SalesforcePubSub/WolverineFxContrib.SalesforcePubSub.Tests

# Live integration suite — 16 facts against a real Salesforce org (~4.5 min, serial):
dotnet test src/Transports/SalesforcePubSub/WolverineFxContrib.SalesforcePubSub.IntegrationTests
```

The integration suite needs one-time setup: the org fixtures (platform events, channel, managed
subscriptions) and two External Client Apps, plus their credentials in the user-secrets store —
**[docs/org-setup/README.md](docs/org-setup/README.md)** walks through all of it. Facts that need
something unavailable skip rather than fail: the three Durable facts skip without a
`durabilitySettings:connectionString` SQL Server secret, and MES facts skip while another client
holds the subscription slot.

There is also a manual harness for interactive and long-running verification (the resiliency
campaign and overnight soaks behind the delivery-guarantee matrix) — it shares the same secrets
store:

```bash
dotnet run --project src/Transports/SalesforcePubSub/TestHost
```

## Repository layout

All projects sit as siblings under `src/Transports/SalesforcePubSub/`, mirroring Wolverine's own
`src/Transports/Kafka` layout. Only the transport and its test projects carry the package prefix;
plain-named siblings are repo-internal and never ship.

- `WolverineFxContrib.SalesforcePubSub` — the transport (public surface is deliberately minimal;
  everything else is `internal` under `Internals/`).
- `…Tests` — unit suite (xUnit v3). `…IntegrationTests` — the live suite: 16 facts against
  a real Salesforce org + SQL Server, mirroring the read-side coverage of Wolverine's own Kafka
  integration tests (receive per kind, multi-type decode, DLQ paths, retry, replay positions,
  restart-resume, fan-out identity, backpressure stop/rebuild). See `docs/org-setup/` for the one-time
  org fixtures and credentials it needs.
- `Salesforce` — repo-internal support lib: the REST publish helper the test projects use
  (not part of the package; the transport does not reference it).
- `MssqlReplay` — repo-internal support lib: a SQL Server `IReplayIdRepository` (Entra auth, DDL included)
  the TestHost uses for resume-across-restart verification.
- `TestHost` — a Worker harness used for manual live verification (resiliency campaign,
  overnight runs).
- `docs/DECISIONS.md` — the ADR-lite log: every design decision, divergence from Wolverine conventions,
  and the live-test evidence behind them. Evidence logs live in `docs/test-results/`.
