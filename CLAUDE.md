# CLAUDE.md — WolverineFxContrib.SalesforcePubSub

A community **Wolverine transport for the Salesforce Pub/Sub gRPC API**, listen-only: it surfaces
Salesforce topic subscriptions and managed event subscriptions (MES) as Wolverine listening
endpoints. Package id `WolverineFxContrib.SalesforcePubSub`; root namespace `Wolverine.SalesforcePubSub`.

## Structure
- `src/WolverineFxContrib.SalesforcePubSub` — the transport library (net10.0).
  - **Public surface:** the three consumer-implemented interfaces `IReplayIdRepository`,
    `IBackoffStrategy`, `IAuthenticationTokenHandler` (registered via `UseReplayIdRepository<T>` /
    `UseBackoffStrategy<T>` / `UseAuthenticationHandler<T>` on the transport expression); event base
    types `PubSubEvent` / `PlatformEvent` (in the `Wolverine.SalesforcePubSub.Events` namespace); and
    the Wolverine config surface — `UseSalesforcePubSub` → `SalesforcePubSubTransportExpression` (with
    grouped `Heartbeat`/`Watchdog` sub-expressions), `ListenToSalesforceTopic(path)` (a `/event/X__e`
    topic or `/event/X__chn` channel — both are topics to the Pub/Sub API) and
    `ListenToManagedSubscription(name)`, each declaring every event via `MapEvent<T>("Api_Name__e")`
    (multi-type-first, DECISIONS #19), `SalesforcePubSubTransport`, `SalesforceEndpoint`,
    `SalesforceListenerConfiguration`.
  - **Namespaces are folder-based:** root files → `Wolverine.SalesforcePubSub`, `Events/` →
    `Wolverine.SalesforcePubSub.Events`, `Internals/` and its subfolders →
    `Wolverine.SalesforcePubSub.Internals[.Authentication|.Backoff|.Replay|.Schema|.Transports]`.
  - **`Internals/`** (all `internal`, `sealed`): root holds the `SalesforceListener` (`IListener`) +
    `ListenerDiagnostics`; `Authentication/` (token cache + auth-rejection predicate); `Backoff/`
    (default strategy); `Replay/` (commit tracker, in-memory repo, `ReplayIds`); `Schema/` (schema
    repositories, `AvroRecordName`, `EventTypeResolver`, the Avro serializer); `Transports/`
    (`ISubscriptionTransport` + topic/MES impls, `ResponseMessageInfo`). The Salesforce gRPC proto is
    generated with `Access=Internal`.
- `tests/WolverineFxContrib.SalesforcePubSub.Tests` — unit (xUnit v3).
- `tests/WolverineFxContrib.SalesforcePubSub.IntegrationTests` — live Salesforce / SQL.
- `host/WolverineFxContrib.SalesforcePubSub.TestHost` — Worker app for manual verification.

## Build / test / run
- Build: `dotnet build C:/src/wolverine-salesforce-pubsub/WolverineFxContrib.SalesforcePubSub.slnx`
- Test: `dotnet test C:/src/wolverine-salesforce-pubsub/WolverineFxContrib.SalesforcePubSub.slnx`
- Run host: `dotnet run --project C:/src/wolverine-salesforce-pubsub/host/WolverineFxContrib.SalesforcePubSub.TestHost`

## Key facts
- **net10.0 only** — WolverineFx 6.x dropped net8, so the lib does too.
- **Listen-only** — there is no sender. Publishing a platform event is a REST POST and lives outside
  this transport. CDC is intentionally out of scope (Platform Events only for now).
- **Delivery guarantee is per-endpoint** via Inline (at-least-once), BufferedInMemory (at-most-once), or
  **Durable** (inbox-backed at-least-once with parallelism + a real DLQ; needs a consumer-side Wolverine
  message store, e.g. `WolverineFx.SqlServer` — DECISIONS #17. Restart-recovery decodes via the
  auth-hardened async serializer fetch-on-miss; the replay id and schema id ride persisted headers).
  Replay is a per-envelope **watermark** (`ReplayCommitTracker`): `Track` on receive, `CompleteAsync`
  advances the safe position (lowest in-flight − 1), keep-alives advance during idle, and commits are
  throttled + serialized. `DeferAsync` re-injects for an in-memory retry (Kafka-style). In-process
  reconnects resume from the handled watermark; cold start / restart reads the durable store. Topic commits
  client-side (`IReplayIdRepository`); MES commits server-side (`CommitReplayIdRequest` on the stream). See
  DECISIONS #2/#8/#10/#11. Without a durable store, a poison message is dead-lettered to a no-op and
  discarded (DECISIONS #10); under Durable it is preserved in the store's dead-letter table (#17).
- The listener owns its own reconnect loop (ported from the original `SubscriptionOrchestrator`); it
  must never throw out, because Wolverine does not auto-restart a faulted listener.
- Keep the public surface minimal (the three interfaces + event types + Wolverine config classes);
  everything else stays `internal`.

## TestHost
- Auth mirrors `the internal Salesforce client lib` (its own `SalesforceTokenClient`; **no** dependency on
  the deprecated `a deprecated shared auth package` package). Config split:
  - `salesforceAuthenticationSettings` (user-secrets, id `wolverine.salesforcepubsub`): `ClientId`,
    `ClientSecret`, `LoginUri`.
  - `salesforceSettings` (appsettings; `baseUri` lives in user secrets): `pubSubUri` (gRPC) and
    `subscriptions[]` — each `{ type: Topic|ManagedSubscription (Channel = Topic alias), resource
    (topic path or MES developer name), events[] {messageType, eventApiName? (attribute fallback)},
    mode: Inline|BufferedInMemory|Durable, enabled, … }`.
  - `durabilitySettings:connectionString` (user secrets) opts into the Wolverine SQL Server message store
    for Durable-mode endpoints.
- Wired against **the sandbox org**: Test Event One → MES `CM_Test_Event_One`; Test Event Two → topic
  `/event/CM_Test_Event_Two__e`; custom channel `CM_Test_Channel__chn` carries both, and MES
  `CM_Test_Channel_Sub` sits over that channel (all created via the sf CLI Tooling API). Timed
  `PublisherWorker` (opt-in `publisherSettings`) POSTs the test PEs; handler seams: `Message__c` =
  "poison" (throws) / "slow" (30s delay) drive the DLQ / restart-recovery / kill-window tests
  (delivery-guarantee evidence lives in `test-results/`).

## Conventions
- **Do it the Wolverine way.** This is a community Wolverine transport and should look/behave like a
  native one. When in doubt about API shape, naming, or behavior, mirror how Wolverine's own transports
  do it rather than inventing. The full Wolverine source is cloned locally at `a local Wolverine clone` —
  trace it to confirm conventions before deciding (**Kafka** is the primary reference transport,
  **Azure Service Bus** secondary). Where the port diverges from Wolverine or under-implements what
  Wolverine expects, either implement it Wolverine's way or document why we can't — and record it in
  `DECISIONS.md`.
- Use absolute paths in commands (not `cd`); separate Bash calls (no `&&`/`||`/`;`).
- Prefer transient DI registrations unless there's a specific reason otherwise; internal types live in
  `Internals/`.
- Design decisions (what + why) live in `DECISIONS.md` at the repo root. Full step-by-step history lives
  in `git log` (tag `pre-wolverine-glue` → HEAD) and the Claude project memory — not duplicated here.
