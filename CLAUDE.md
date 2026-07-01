# CLAUDE.md — WolverineFxContrib.SalesforcePubSub

A community **Wolverine transport for the Salesforce Pub/Sub gRPC API**, listen-only: it surfaces
Salesforce topic subscriptions and managed event subscriptions (MES) as Wolverine listening
endpoints. Package id `WolverineFxContrib.SalesforcePubSub`; root namespace `Wolverine.SalesforcePubSub`.

## Structure
- `src/WolverineFxContrib.SalesforcePubSub` — the transport library (net10.0).
  - **Public surface:** the three consumer-implemented interfaces `IReplayIdRepository`,
    `IBackoffStrategy`, `IAuthenticationTokenHandler`; event base types `PubSubEvent` / `PlatformEvent`
    (in the `Wolverine.SalesforcePubSub.Events` namespace — consumers add a `using` for it); and the
    Wolverine config surface — `UseSalesforcePubSub`, `ListenToSalesforceTopic[<T>]`,
    `ListenToManagedSubscription[<T>]` (generic + `Type`-based overloads), `SalesforcePubSubTransport`,
    `SalesforceEndpoint`.
  - **Namespaces are folder-based:** root files → `Wolverine.SalesforcePubSub`, `Events/` →
    `Wolverine.SalesforcePubSub.Events`, `Internals/` → `Wolverine.SalesforcePubSub.Internals`.
  - **`Internals/`** (all `internal`, `sealed`): `ISubscriptionTransport` + `TopicTransport` /
    `ManagedEventSubscriptionTransport`, the `SalesforceListener` (`IListener`), schema repo,
    `PlatformEventDeserializer`, the in-memory replay + default backoff fallbacks, and the Salesforce
    gRPC proto (generated with `Access=Internal`).
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
- **Delivery guarantee is per-endpoint** via Inline (at-least-once) vs BufferedInMemory (at-most-once).
  Replay is a per-envelope **watermark** (`ReplayCommitTracker`): `Track` on receive, `CompleteAsync`
  advances the safe position (lowest in-flight − 1), keep-alives advance during idle, and commits are
  throttled + serialized. `DeferAsync` re-injects for an in-memory retry (Kafka-style). In-process
  reconnects resume from the handled watermark; cold start / restart reads the durable store. Topic commits
  client-side (`IReplayIdRepository`); MES commits server-side (`CommitReplayIdRequest` on the stream). See
  DECISIONS #2/#8/#10/#11. **Open work item: `EndpointMode.Durable`** (inbox) for at-least-once *with
  parallelism* + a real DLQ — without a durable store, a poison message is dead-lettered to a no-op and
  discarded (DECISIONS #10).
- The listener owns its own reconnect loop (ported from the original `SubscriptionOrchestrator`); it
  must never throw out, because Wolverine does not auto-restart a faulted listener.
- Keep the public surface minimal (the three interfaces + event types + Wolverine config classes);
  everything else stays `internal`.

## TestHost
- Auth mirrors `the internal Salesforce client lib` (its own `SalesforceTokenClient`; **no** dependency on
  the deprecated `a deprecated shared auth package` package). Config split:
  - `salesforceAuthenticationSettings` (user-secrets, id `wolverine.salesforcepubsub`): `ClientId`,
    `ClientSecret`, `LoginUri`.
  - `salesforceSettings` (appsettings): `baseUri` (REST data API), `pubSubUri` (gRPC), and
    `subscriptions[]` — each `{ type: Topic|ManagedSubscription, channel, messageType }`.
- Wired against **the sandbox org**: Test Event One → MES `CM_Test_Event_One`; Test Event Two → topic
  `/event/CM_Test_Event_Two__e`. Publisher: console keys `[1]`/`[2]` POST the test PEs via
  `ISalesforceClient`.

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
