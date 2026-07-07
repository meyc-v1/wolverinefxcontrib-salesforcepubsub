# CLAUDE.md — WolverineFxContrib.SalesforcePubSub

A community **Wolverine transport for the Salesforce Pub/Sub gRPC API**, listen-only: it surfaces
Salesforce topic subscriptions and managed event subscriptions (MES) as Wolverine listening
endpoints. Package id `WolverineFxContrib.SalesforcePubSub`; root namespace `Wolverine.SalesforcePubSub`.

## Structure
All projects live as siblings under `src/Transports/SalesforcePubSub/` (mirroring Wolverine's own
`src/Transports/Kafka` layout); non-code artifacts live under `docs/`. Only the transport library and
its two test projects carry the `WolverineFxContrib.` prefix — plain-named siblings never ship.
- `WolverineFxContrib.SalesforcePubSub` — the transport library (net10.0).
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
- `MssqlReplay` — repo-internal support lib (`IsPackable=false`): the SQL Server `IReplayIdRepository`
  (raw ADO.NET, Entra auth via `MssqlAadAuthentication`, `CreateReplayTable.sql` DDL) used by the TestHost
  for resume-across-restart runs; binds `MssqlReplaySettings` from the host's replay section.
- `Salesforce` — repo-internal support lib
  (`IsPackable=false`): the client-credentials
  `ISalesforceTokenClient` (cached) + bearer `DelegatingHandler` + `ISalesforceClient` for REST-POSTing
  platform events. **Publisher-side only — no reference to the transport.** The transport authenticates
  through consumer-implemented `IAuthenticationTokenHandler`s (direct fetch-fresh, no cache — the
  transport owns caching/invalidation), which live host-side in the TestHost and IntegrationTests.
- `WolverineFxContrib.SalesforcePubSub.Tests` — unit (xUnit v3, ~99 tests).
- `WolverineFxContrib.SalesforcePubSub.IntegrationTests` — **the live suite**: 16 facts against a
  real Salesforce org + SQL Server, modeled on Wolverine's own Kafka tests (their docker broker ≙ our
  sandbox org; no gRPC mocking). Covers the full Kafka-parity read-side matrix: receive per kind
  (topic/channel/MES/MES-over-channel), multi-type decode, envelope metadata, unmapped→missing-handler,
  Inline poison discard vs Durable poison→DLQ-table row, requeue retry, cold-start Latest vs
  StartFromEarliest, restart-resume, fan-out dedup `IdOnly` vs `IdAndDestination`, hot-tail broadcast,
  repo commit semantics, and backpressure stop→rebuild. Runs serially (~4.5 min); isolation is by
  correlation Guid in `Message__c` (never by recreating org fixtures). MES facts `Assert.Skip` when the
  slot is held; Durable facts skip without the SQL secret and use their own schemas (`witint` /
  `witint_iad` — the `MessageIdentity` switch migrates the inbox PK). Harness pieces:
  `SalesforceTestContext` (assembly fixture: secrets + REST publisher), `TestHosts` (per-test Wolverine
  host), `EventSink` (sink+poll receive assertions), `RecordingReplayIdRepository`, `LogSink` (captures
  host logs — the reconnect loop never throws, so logs are the only signal for slot-held / too-busy).
- `TestHost` — Worker app for manual verification (predates the
  integration suite; kept for interactive/overnight runs).
- `docs/org-setup/` — one-time Salesforce fixture setup for the tests: README (manual platform-event
  creation + the two ECAs + the user-secrets layout) and a Bruno collection (channel/members/MES via
  Tooling REST). **Read this first on a new machine or org.**

## Build / test / run
(paths relative to the repo root)
- Build: `dotnet build WolverineFxContrib.SalesforcePubSub.slnx`
- Test: `dotnet test WolverineFxContrib.SalesforcePubSub.slnx`
- Run host: `dotnet run --project src/Transports/SalesforcePubSub/TestHost`

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

## Salesforce environment & credentials
- **Org fixtures are the `WIT_` set** (WIT = Wolverine Integration Test), shared by the TestHost and the
  integration suite: platform events `WIT_Event_A__e` / `WIT_Event_B__e` (one nullable `Message__c`
  Text 255 each), custom channel `WIT_Channel__chn` carrying both, MES `WIT_Event_A_Sub` (over event A)
  and `WIT_Channel_Sub` (over the channel). Create-once permanent infra — see `docs/org-setup/README.md` for
  the manual PE walkthrough + Bruno collection (PE definitions are Metadata-API-only; we deliberately
  ship no metadata deploy). Any `CM_`-prefixed fixtures in the org are the maintainer's personal test
  infra, not referenced by this repo.
- **Two ECAs (External Client Apps), one per role** — independent token lifecycles and least-privilege:
  the **subscriber** ECA's run-as user has Read on the WIT events (feeds the transport's
  `IAuthenticationTokenHandler`); the **publisher** ECA's has Create (feeds the Salesforce lib REST
  client). ECA gotcha: under "Admin approved users are pre-authorized" the profile/permission-set grant
  is attached **on the app's Edit Policies page** (External Client App Manager → row actions → Edit
  Policies), not inside the Permission Set editor.
- **User secrets** (single store, id `wolverine.salesforcepubsub`, shared by TestHost and
  IntegrationTests — full skeleton in `docs/org-setup/README.md`): `subscriberAuthenticationSettings` +
  `publisherAuthenticationSettings` (each ClientId/ClientSecret/LoginUri), `salesforceSettings:baseUri`
  (REST data API base incl. version, trailing slash), optional `salesforceSettings:pubSubUri`,
  `durabilitySettings:connectionString` (Wolverine SQL message store — Durable endpoints and Durable
  facts), `salesforceReplaySettings:connectionString` (the MssqlReplay store, used by the TestHost).
- **Salesforce facts learned live** (also in DECISIONS): near-simultaneous REST publishes can receive
  bus positions opposite to POST order (order-sensitive tests space publishes); replay ids are global
  to the org's event bus (foreign events create gaps in observed ids — never assume contiguity);
  `PlatformEventChannelMember.EventChannel`/`SelectedEntity` Tooling queries return record **Ids**, not
  names; a MES slot is exclusive per client and an unclean disconnect holds it ~15 min (DECISIONS #13).

## TestHost
- Auth: the REST publisher comes from the Salesforce lib (publisher ECA,
  `publisherAuthenticationSettings`); the transport uses the host-side `SalesforceAuthenticationTokenHandler`
  (subscriber ECA, `subscriberAuthenticationSettings`, direct fetch — no cache).
- `salesforceSettings` (appsettings; `baseUri` in user secrets): `pubSubUri` (gRPC) and
  `subscriptions[]` — each `{ type: Topic|ManagedSubscription (Channel = Topic alias), resource
  (topic path or MES developer name), events[] {messageType, eventApiName? (attribute fallback)},
  mode: Inline|BufferedInMemory|Durable, enabled, … }`. Committed default: all five WIT subscriptions
  enabled (topics A + B, channel, both MES), all Inline, publisher off; working-tree edits to
  `appsettings.json` are per-run test config by convention.
- Timed `PublisherWorker` (opt-in `publisherSettings`) POSTs the test PEs; handler seams: `Message__c` =
  "poison" (throws) / "slow" (30s delay) drive manual DLQ / restart-recovery / kill-window tests
  (delivery-guarantee evidence lives in `docs/test-results/`).

## Current state & open work (2026-07-07)
Feature-complete and design-settled: all delivery modes live-verified (Inline: resiliency campaign +
13.6h volume soak; Durable: live pass + overnight; Buffered: kill-window characterization + 19h
steady-state — DECISIONS #20), the stop/dispose stale-commit gap is fixed with a deterministic listener
unit harness (DECISIONS #22), and the integration suite covers the full Kafka-parity read-side matrix
(16 facts, ~4.5 min — the agreed acceptance gate, run green with zero skips before the first package).
**SHIPPED: `1.0.0-preview.1` is live on nuget.org (2026-07-07)** — identity per DECISIONS #24, pre-1.0
API pass per #25 (`IReplayIdRepository` → Get/Store/Reset + `ReplayCommitKind`; auth + backoff
interfaces reviewed), published via the manually-dispatched `publish.yml` (NuGet Trusted Publishing /
OIDC, no stored key — the dispatch `version` input must match `Directory.Build.props`). Open:
1. Awaiting review: the maintainer-invited docs PR to JasperFx/wolverine (from discussion #3325),
   opened from the meyc-v1/wolverine fork.
2. Opportunistic: consumer adoption; a Postgres replay sibling to `MssqlReplay`; 1.0.0 final when
   the preview has soaked with real consumers.

## Conventions
- **Do it the Wolverine way.** This is a community Wolverine transport and should look/behave like a
  native one. When in doubt about API shape, naming, or behavior, mirror how Wolverine's own transports
  do it rather than inventing. Trace a local clone of the Wolverine source to confirm conventions before
  deciding (**Kafka** is the primary reference transport, **Azure Service Bus** secondary) — clone
  `JasperFx/wolverine` if absent (the path is machine-specific). A clone tracking `main` is AHEAD of the
  pinned **WolverineFx 6.12.0** — the repo has version tags, so check APIs against the pinned source
  with `git show V6.12.0:<path>` (or check out the tag) before relying on them. Where the
  port diverges from Wolverine or under-implements what Wolverine expects, either implement it
  Wolverine's way or document why we can't — and record it in `DECISIONS.md`.
- Use absolute paths in commands (not `cd`); separate Bash calls (no `&&`/`||`/`;`).
- Prefer transient DI registrations unless there's a specific reason otherwise; internal types live in
  `Internals/`.
- Design decisions (what + why) live in `docs/DECISIONS.md` — read it first; it is the
  source of truth for every decision, divergence, and the live-test evidence behind them. Full
  step-by-step history lives in `git log` (tag `pre-wolverine-glue` → HEAD) — not duplicated here.
