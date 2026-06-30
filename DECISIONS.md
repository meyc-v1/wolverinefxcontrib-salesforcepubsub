# Decisions

A running log of design decisions for **WolverineFxContrib.SalesforcePubSub** — what we chose and
*why*, so we can refer back later. Newest entries at the top. Each entry is lightweight: Status,
Context, Decision, Why, Consequences.

Status legend: **Accepted** (in effect) · **Deferred** (deliberately not now) · **Superseded** (replaced by a later entry).

---

## Guiding principle: do it the Wolverine way

This is a community Wolverine transport, so it should look and behave like a native one. The default for
any design question is **how does Wolverine itself do it** — the full Wolverine source is cloned locally
at `a local Wolverine clone`, so tracing the real implementation (Kafka primary, Azure Service Bus
secondary) is the basis for resolving indecision, not guesswork. **Caveat:** the clone tracks `main` and
can be ahead of the pinned package (currently **WolverineFx 6.12.0**) — confirm an API exists in the
pinned version before relying on it (e.g. `DescribeEndpoint` / `IReportReceiveLoopHealth` are main-only,
which blocked Phase 6). Where the port strays from Wolverine's
conventions or doesn't fully implement what Wolverine expects, the rule is: **implement it Wolverine's
way, or document why we aren't/can't** — and capture it under "Divergences & gaps" below.

This extends to **code shape, not just behavior**: prefer mirroring the reference transport's *structure*
(Kafka primary) — how listeners are constructed, where config lives, how pieces are wired — so the
implementation **reads** like a Wolverine transport, not merely behaves like one. Structural gaps that
aren't conformance issues go under "Cleanups / tech-debt".

---

## 7. Inherited `ListenerConfiguration` serializer/encryption knobs left inert (no guard)
- **Date:** 2026-06-30 · **Status:** Accepted
- **Context:** Deriving from `ListenerConfiguration<,>` (see #5) inherits `DefaultSerializer`,
  `Encrypted`, `RequireEncryption`, and `MessageBatchSize`, which look inapplicable to this transport.
- **Decision:** Leave them inherited but treat them as inert; do **not** add a `RequireEncryption`
  guard.
- **Why:** The transport owns Avro deserialization and sets `envelope.Message` directly, bypassing
  Wolverine's inbound serialization pipeline. Verified in Wolverine source that the encryption gate
  lives inside `TryDeserializeEnvelope`, behind its `if (envelope.Message != null) return` short-circuit
  (`HandlerPipeline.cs`) — so for our pre-deserialized envelopes it never runs. These knobs are no-ops,
  not hazards. A guard would also require reflecting into Wolverine's deliberately-`internal`
  `RequiredEncryptedListenerUris`, fighting its design to guard nothing.
- **Consequences:** Document the inert knobs in XML docs. (Corrects an earlier in-session assumption
  that `RequireEncryption` would dead-letter events — it does not, for this transport.)

## 6. Per-message dispatch into Wolverine for now; batching deferred
- **Date:** 2026-06-30 · **Status:** Deferred
- **Context:** Salesforce delivers events in batches (`FetchResponse.Events`, sized by `FetchCount`),
  and Wolverine's `IReceiver` exposes a batch overload (`ReceivedAsync(IListener, Envelope[])`).
- **Decision:** The listener decomposes each batch and calls the single-envelope `ReceivedAsync` in a
  loop, then acks the whole batch.
- **Why:** Simplest correct first cut. Batching is genuinely possible (both wire and Wolverine support
  it); it just wasn't wired up.
- **Consequences:** `MessageBatchSize` is inert today. Switching to the array overload is a future
  refinement that would also align dispatch with the batch-level replay ack.

## 5. `SalesforceListenerConfiguration` derives from Wolverine's `ListenerConfiguration<,>`
- **Date:** 2026-06-30 · **Status:** Accepted
- **Context:** The port shipped a hand-rolled sealed config class exposing only `ProcessInline()` /
  `BufferedInMemory()` — two methods that merely set one endpoint property.
- **Decision:** Derive from `ListenerConfiguration<SalesforceListenerConfiguration, SalesforceEndpoint>`
  and delete the hand-rolled methods (now inherited).
- **Why:** Match the Wolverine convention (Kafka/ASB do exactly this) so the surface "natively belongs"
  in Wolverine. The hand-rolled class re-implemented base methods by hand. Rejected the alternative of
  passing the mode into `ConfigureListener` as a parameter — Wolverine's idiom is fluent chaining, not
  parameters (`ListenToKafkaTopic` takes only the topic name).
- **Consequences:** Consumers get the standard listener surface for free. Unsupported modes are rejected
  by `supportsMode`; `ListenerCount` is guarded (see #4); serializer/encryption knobs are inert (see #7).

## 4. `ListenerCount` constrained to 1
- **Date:** 2026-06-30 · **Status:** Accepted
- **Context:** The inherited base exposes `ListenerCount(int)`. Wolverine's `ListeningAgent` builds that
  many parallel listeners.
- **Decision:** `SalesforceEndpoint.BuildListenerAsync` throws if `ListenerCount > 1`.
- **Why:** Each listener opens its own gRPC subscription to the same channel, so >1 means duplicate
  delivery and replay-id races. `supportsMode` doesn't cover it and `ListenerCount` isn't virtual, so a
  fail-fast guard at build time is the clean spot.
- **Consequences:** Misconfiguration fails at startup with a clear message instead of silently
  duplicating delivery.

## 3. Transport owns Salesforce token caching + invalidates on auth failure
- **Date:** 2026-06-24 · **Status:** Accepted · **Commit:** 52a4aa3
- **Context:** Revoked-before-expiry tokens were a recurring production pain; a TTL-only cache (or a
  caching consumer handler) keeps re-handing the dead token on reconnect.
- **Decision:** `CachingAuthenticationTokenProvider` (singleton) owns caching (default 60 min, override
  via `SubscriberComponentsSettings.TokenCacheDuration`); `AddCallCredentials` resolves it; the listener
  calls `Invalidate()` on `RpcException` Unauthenticated/PermissionDenied before reconnecting.
  `IAuthenticationTokenHandler` must fetch fresh and not cache.
- **Why:** Centralizing cache + invalidation in the library is the only way to reliably recover from
  revocation; it also removes the burden from consumers. Kept `AddCallCredentials` (rejected inline
  credential attachment, "Option C" — more surface, no correctness gain).
- **Consequences:** Consumer handlers must not cache. TestHost bridge fetches with `refresh: true`.

## 2. Delivery guarantee: ship effective at-least-once (Inline); formal seam deferred
- **Date:** 2026-06-24 (implemented 2026-06-30, Phase 2) · **Status:** **Resolved (Phase 2)** for Inline at-least-once — per-envelope `ReplayCommitTracker` watermark; `CompleteAsync` advances the position, `DeferAsync` holds it; Topic + MES (MES commit serialized through the request stream); keep-alive advance + commit throttle + flush-on-stop; commits route to the current transport. **Buffered remains at-most-once** by design; parallel at-least-once via the durable inbox is the aspirational entry (built on this).
- **Context:** Replay is committed after the batch is dispatched; `CompleteAsync`/`DeferAsync` are no-ops.
- **Decision:** Ship with the default **Inline** mode, where `ReceivedAsync` runs the handler before the
  ack → effective at-least-once for the crash case (crash mid-batch ⇒ re-delivery). Defer the robust seam.
- **Scope when the seam IS built — no partial:** it must ship covering **Topic and MES together**, never
  a transport-only partial. A half-wired `IListener` ack contract (real on Topic, no-op on MES) silently
  differs by endpoint kind and is worse than today's honest no-op-on-both. The unit of work is: a shared
  per-envelope replay **watermark** (Kafka `KafkaOffsetCommitter`-style — `Track` on receive, advance
  only through successfully-handled events on `CompleteAsync`, hold on `DeferAsync`), a commit throttle,
  and the **MES serialized writer** (commits share the gRPC `RequestStream` with fetch requests, and
  concurrent `WriteAsync` throws).
- **Buffered stays at-most-once — by design; the seam does NOT change it.** `BufferedReceiver` calls
  `CompleteAsync` at *receipt* time (posts the complete-block right after enqueue, before the handler
  runs — `BufferedReceiver.cs:242`), so replay would advance when an event is buffered, not when handled.
  At-least-once is therefore inherently an **Inline** property. Documenting this is part of the scope,
  not something to implement for Buffered.
- **Current gap the seam also closes:** today a handler *failure* under Inline is swallowed by
  `InlineReceiver` (it calls our no-op `DeferAsync`) and the batch ack still advances past the failed
  event → that event is lost. So current Inline is at-least-once for crashes but not for handler
  failures; the seam fixes this via per-envelope `Complete`-on-success / `Defer`-holds.
- **Why deferred:** significant cross-cutting work; current behavior is acceptable for now and matches
  the old library.

## 1. Test framework xUnit v3; integration tests deferred
- **Date:** 2026-06-24 · **Status:** Accepted / Deferred
- **Context:** Both test projects were empty stubs with no test framework.
- **Decision:** Wire both as xUnit v3; build a starter unit suite over dependency-light logic; leave
  IntegrationTests as a single **skipped** placeholder.
- **Why:** Repo standard is xUnit v3. Integration tests need live Salesforce (the sandbox org) + SQL and are
  premature until the at-least-once seam and a live verification pass exist.
- **Consequences:** `dotnet test` runs the unit suite; integration is intentionally a stub.

---

## Wolverine alignment — divergences & gaps

Places the port strays from Wolverine's conventions or under-implements what Wolverine expects. Each is
either resolved (implemented Wolverine's way / documented) or open. Add to this list whenever a new one
is observed.

_Conformance pass 2026-06-30: audited `IListener`, `Endpoint`, and `TransportBase` against the base
contracts + Kafka/ASB. The port is largely conformant; findings below._

- **Hand-rolled listener config** — the port wrote a bespoke fluent class instead of deriving from
  `ListenerConfiguration<,>`. → **Resolved** (now derives — #5).
- **Inherited serializer/encryption knobs are inert** — Wolverine assumes its serialization pipeline; we
  bypass it by deserializing Avro ourselves. → **Superseded (Phase 3)** — option b now routes inbound
  through Wolverine's pipeline, so those knobs are *live*: `DefaultSerializer` is moot (our content-type
  serializer is registered), and the encryption gate is dormant unless a consumer calls `RequireEncryption`
  on a SF listener — which would now dead-letter every event (don't). Was #7.
- **Config lives off-endpoint, not on it** — Wolverine reference endpoints carry per-endpoint tuning on the
  endpoint (`KafkaTopic.CommitMode`/`ConsumerConfig`/`NativeDeadLetterQueueEnabled`, …); ours puts all
  tuning on the global `SubscriberComponentsSettings` singleton, so `SalesforceEndpoint` carries only
  `Kind`/`Resource`/`MessageType` and there's no per-endpoint override (e.g. per-topic
  `StartFromEarliest`/`FetchCount`). → **Open** (reshape toward Kafka's per-endpoint config). **Stipulation
  (user):** the reshape must drop `SubscriberComponentsSettings` as a *public, Options-configured* type —
  move the defaults onto `SalesforceEndpoint` (and/or a non-public defaults type), not an exposed options class.
  → **Resolved (Phase 4)** — `SubscriberComponentsSettings` is now `internal` (effective per-listener
  settings); `UseSalesforcePubSub(Uri? pubSubUri)` + fluent `TokenCacheDuration` replace the `Action<>`
  config; per-endpoint `FetchCount`/`FetchTimeout`/`StartFromEarliest` overrides live on
  `SalesforceEndpoint` with fluent setters on `SalesforceListenerConfiguration`, merged into an effective
  instance in `BuildListenerAsync`.
- **Envelope is under-populated** — the listener sets only `Message`/`TopicName`/`Offset`. Most important:
  **`Id` is not set deterministically**, so Wolverine assigns a fresh `Guid` per receive and a *redelivered*
  Salesforce event can't be deduped — derive `envelope.Id` from the Salesforce event id
  (`consumerEvent.Event.Id`/EventUuid; confirm exact field). Pairs with #2 — at-least-once needs a stable
  id for inbox idempotency. Minor parity: set `SentAt` from the event `CreatedDate` (telemetry) and the
  `MessageType` string (Kafka sets it). → **Resolved (Phase 1)** — `envelope.Id` via
  `SalesforceListener.ResolveEnvelopeId` (`consumerEvent.Event.Id` guid passthrough, else a deterministic
  guid; resource+replayId fallback), `SentAt` from `PlatformEvent.CreatedDate`, `MessageType` via
  `ToMessageTypeName()`.
- **`CompleteAsync`/`DeferAsync` are no-ops** — Wolverine's `IListener` contract expects per-envelope
  ack/defer; we commit replay at the batch level in the consume loop instead. The most significant
  under-implementation of what Wolverine expects. → **Resolved (Phase 2)** — wired to the
  `ReplayCommitTracker` watermark (Complete advances, Defer holds); Inline at-least-once (#2).
- **`ReplyEndpoint()` returned a listener** — `TransportBase`'s default advertised our listen-only endpoint
  as a request/reply target, but we can't send. → **Resolved** (overridden to return `null`; a listen-only
  transport has no reply target — every sendable transport overrides this with its real one).
- **Batch receive overload unused** — Wolverine's `IReceiver` offers `ReceivedAsync(Envelope[])`; we feed
  one envelope at a time. → **Deferred** (#6) — and it matches Kafka, which also dispatches singly.
- **No `Durable` (inbox) mode → no at-least-once *with parallelism*** — `supportsMode` allows only
  Inline/BufferedInMemory. The Wolverine-idiomatic way to get at-least-once *and* throughput is the
  **durable inbox** (`EndpointMode.Durable`): "Complete" means *persisted to the inbox* (in order), and
  the durability agent provides recovery + dedup — exactly how Kafka's `ProcessConcurrentlyByKey` works.
  Mostly **free from Wolverine** once a message store is configured (SQL Server / Postgres-Marten / EF +
  the `WolverineFx.*` package; tables auto-provisioned) — that store is the consumer-side infra.
  **Depends on #2** (verified in `DurableReceiver`): it persists to the inbox (`StoreIncomingAsync`) and
  *then* calls `listener.CompleteAsync` (`:518`/`:663`); `ReceivedAsync` returns *before* the persist, so
  our current batch-level `AcknowledgeAsync`-after-dispatch is **premature** in Durable (it would commit
  replay before durability). The only correct commit signal is `CompleteAsync` (post-persist in Durable,
  post-handler in Inline) — i.e. the #2 seam. So Durable can't be done correctly until #2 lands; after it,
  the remaining transport work is small: allow `Durable` in `supportsMode` + the **deterministic envelope
  `Id`** (inbox dedups by it). NB: low-water-marking on **BufferedInMemory** is *not* a path — it acks at
  receipt. → **Open / aspirational** (future; **built on #2**, not independent of it).
- **No sender (listen-only)** — `SalesforceEndpoint.CreateSender` throws. Confirmed **safe**:
  `AutoStartSendingAgent()` is false for a pure listener (no `Subscriptions`), so Wolverine never calls
  `StartSending`/`CreateSender`; only reachable if a consumer publishes to an `sfpubsub://` URI (throws
  clearly). → **Intentional / verified**.
- **Owns its reconnect loop + per-attempt transport factory** — `SalesforceListener` runs the
  connect/process/backoff/reconnect loop and `_transportFactory()` yields a *fresh*
  `ISubscriptionTransport` per attempt (disposed via `using`), rather than Kafka's single long-lived
  auto-reconnecting consumer. Driven by single-use gRPC duplex streams. A single reusable transport would
  read more Kafka-like and shed a listener ctor param, **but** the lifted `TopicTransport`/MES transports
  are written single-use and already **well-tested that way**; reshaping them would change
  `ConnectAsync`/`Dispose` lifecycle semantics on validated code for a stylistic gain. → **Intentional —
  kept** (existing test coverage outweighs the Kafka-shape cleanup; do not refactor without re-validating).
- **`findEndpointByUri` throws on an unknown URI** (Kafka creates on demand) — fine for listen-only, where
  endpoints exist only from `ListenTo…` config. → **Intentional**.
- **No diagnostics surface** — `DescribeEndpoint()` (sanitized broker host) and a listener health snapshot
  (Kafka's `ReceiveLoopStatus`/`IReportReceiveLoopHealth`, GH-3236). → **Blocked (Phase 6 attempted, then
  reverted)** — both APIs are **post-6.12.0**: `Endpoint.DescribeEndpoint` (virtual) and
  `IReportReceiveLoopHealth`/`ReceiveLoopStatus` don't exist in WolverineFx **6.12.0** (the local clone is
  `main`, ahead of the pinned package). Deferred until a WolverineFx upgrade; not worth upgrading for
  optional diagnostics alone.

## Cleanups / tech-debt

Non-conformance, code-quality items — places to make the implementation **read** more like Wolverine's
reference transports (Kafka primary), not just behave like one. Tackle in an update pass.

- **`SalesforceListener` construction is service-locator-ish** — `SalesforceEndpoint.BuildListenerAsync`
  does five `runtime.Services.GetRequiredService<…>()` calls and threads the results into an 11-arg
  constructor. Manual `new` in `BuildListenerAsync` is itself fine and Wolverine-idiomatic (Kafka `new`s
  its listener too), but the shape is heavier than Kafka's. Make it more Kafka-like:
  `ActivatorUtilities.CreateInstance<SalesforceListener>(runtime.Services, <contextual args>)` so DI fills
  the service params (deserializer, settings, backoff, token provider, logger), and/or pass the
  `SalesforceEndpoint` in and read `Resource`/`MessageType`/`Uri` off it to shrink the param list.
  Internal-only; no behavior or public-surface change. (Observed during the runtime-implementation review.)
  → **Resolved (Phase 2)** via `ActivatorUtilities.CreateInstance` (DI fills the service params).
- **Deserialize through Wolverine's serializer pipeline, keeping Avro (option "b")** — today the listener
  Avro-decodes and sets `envelope.Message` directly (the in-process pattern), bypassing Wolverine's
  serializer (#7). Candidate: set `envelope.Data` = raw Avro bytes + a content-type + a schema-id header,
  and register a **sync** custom `IMessageSerializer` that reads the (already-cached) schema and
  Avro-decodes to the type — mirroring how Confluent Schema-Registry resolves-then-decodes. Reactivates
  the serializer/encryption pipeline and makes us read like a broker transport (`Data`-based) instead of
  the local/in-process shape. **Hard constraint:** the **async schema lookup must stay in the transport
  consume loop** (the listener pre-fetches/ensures-cached before handoff), NOT in the serializer — so a
  revoked/expired-token `GetSchema` failure still surfaces to the reconnect loop and triggers the
  token-invalidate. Moving the fetch into the (pipeline-run) serializer would break that auth recovery.
  → **Resolved (Phase 3)** — `SalesforceAvroSerializer` (sync, registered on the endpoint by content-type)
  decodes from the cached schema and stamps `ReplayId`/`SentAt`; the listener sets `Data` + content-type +
  `sfdc-schema-id` header and pre-fetches the schema in the loop (auth recovery preserved). The now-dead
  `PlatformEventDeserializer`/`EventMessage`/`IEventDeserializer` were removed.

## Inherited from the original port — pending ratification

These were decided by the earlier porting session, not by us. Listed here to be reviewed and either
ratified into a numbered entry above or revisited:

- **net10.0 only** — WolverineFx 6.x dropped net8 (forced, not really a choice).
- **Listen-only** — publishing is a REST POST, out of scope for this transport.
- **CDC out of scope** — Platform Events first; CDC deferred.
- **Auth mirrors `the internal Salesforce client lib`** — own `SalesforceTokenClient`, not the deprecated
  `a deprecated shared auth package` package.
- **Folder-based namespaces; public event base types in `Wolverine.SalesforcePubSub.Events`.**
- **`UseWolverine` over `AddWolverine`** — host-builder integration + reliable handler assembly scanning.
