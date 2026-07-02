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

## 12. MES re-affirms its replay commit on idle keep-alives (server 1800s no-commit deadline)
- **Date:** 2026-07-02 · **Status:** Accepted
- **Context:** The ~4.5h idle baseline (`test-results/z_long-run-1.txt`, topic + MES both on, publisher off)
  surfaced a recurring MES-only cycle every ~30 min: `RpcException DeadlineExceeded: "No CommitReplayRequests
  sent in the last 1800 seconds. Closing managed subscription."` → the immediate 0-backoff reconnect on the
  first error raced Salesforce's slot teardown → `AlreadyExists` → 15s backoff → reconnect → repeat. Root
  cause (confirmed exactly by the log): the last MES commit was 15:32:41, first `DeadlineExceeded` at 16:02:41
  — precisely 1800s later. Replay commits are **watermark-driven** (`ReplayCommitTracker` commits only when the
  safe position *advances* past `_lastCommitted`). With no traffic the position never advances, so **no
  `CommitReplayRequest` is ever sent during idle**, and Salesforce closes a managed subscription that receives
  none within 1800s. Topic is immune (client-side commit, no server deadline). Real production impact: any
  idle or below-`commitEvery` low-volume MES subscription would churn (tear down + rebuild) every 30 min.
- **Decision:** MES re-affirms the last committed position on **each idle keep-alive**. `ReplayCommitTracker`
  gained `commitKeepAliveWhenIdle` (MES `true`, topic `false`, wired in `SalesforceListener` as
  `!resumesFromWatermark`); when a keep-alive produces no advance, it re-sends `_lastCommitted` (once something
  has been committed) to reset the server's deadline. MES keep-alives arrive ~every 2 min, comfortably inside
  1800s.
- **Why:** The re-affirm is safe — it re-sends a position the server already acknowledged, so it never
  regresses and never advances past an in-flight event (verified by test: with an event in flight, the
  keep-alive re-affirms the floor, not the tip). It requires no clock (keep-alive arrival is the natural
  cadence) and is scoped off for topic (which would otherwise re-write the repository pointlessly). Chose this
  over a dedicated commit-keepalive timer in the MES transport, which would have to independently commit the
  tip and could race past in-flight events (unsafe) — the watermark tracker is the only component that knows
  the safe position.
- **Consequences:** Fixes the 30-min teardown; the paired `AlreadyExists` disappears with it. Unit tests
  46 (was 43). **Residual (not addressed, benign):** on any *other* server-initiated MES close, the first
  reconnect (0s backoff) can still race the slot teardown for a single self-healing `AlreadyExists` (same as
  DECISIONS-observed in MES #1) — acceptable, rides the backoff. **Not yet re-verified live** — the next idle
  baseline should show steady ~2-min MES commits and zero `DeadlineExceeded`/`AlreadyExists`.

## 11. MES graceful-shutdown commit: flush before cancel + serialize stream writes
- **Date:** 2026-07-01 · **Status:** Accepted
- **Context:** The MES throwing-handler test showed the graceful-shutdown flush failing with
  `ObjectDisposedException`. `StopAsync` cancelled `_cts` — which aborts the MES gRPC call (created with that
  token) and disposes the transport — **before** calling `FlushAsync`, so the final `CommitReplayIdRequest`
  wrote to a dead stream. Topic was immune (it commits out-of-band to the repository). Separately, MES fetch
  requests and commits share one `RequestStream` and concurrent `WriteAsync` throws, so the flush could race
  an in-flight `RequestMore` — the "MES serialized writer" #2 anticipated but that wasn't wired.
- **Decision:** `StopAsync` flushes the final committable position **before** cancelling (bounded to 5s so a
  dead stream can't hang shutdown); `ManagedEventSubscriptionTransport` serializes **all** request-stream
  writes (fetch + commit) through a `_writeLock`.
- **Why:** MES commits on the gRPC stream, which cancellation aborts — the flush must run while the stream is
  live. The write lock stops the flush racing a fetch. The timeout means a dead stream degrades to
  at-least-once (the tail re-delivers on restart) instead of hanging shutdown.
- **Consequences:** Verified — a graceful MES shutdown now logs `Finished CommitReplayId` + `Committed replay
  position X` **before** the stream cancels, no `ObjectDisposedException`; the server records the final
  position so a restart doesn't redeliver the uncommitted tail. Topic unchanged (out-of-band SQL commit).

## 10. `DeferAsync` re-injects for an in-memory retry (Kafka precedent), not hold-idle or discard
- **Date:** 2026-07-01 · **Status:** Accepted
- **Context:** `DeferAsync` was a no-op that left the envelope in flight, holding the replay watermark until a
  reconnect happened to re-deliver it. On a healthy long-running stream that reconnect may never come, so a
  deferred message was **never retried** and the watermark stalled — and a stalled watermark risks the
  committed replay id **aging out of Salesforce's 72h retention** (→ `InvalidArgument` → reset-to-Latest,
  losing far more than the one message) plus mass replay on the eventual reconnect. Investigated how
  Wolverine's own stream transports handle this: **Kafka's `DeferAsync` re-injects the envelope into the
  pipeline (`_receiver.ReceivedAsync(this, envelope)`) — an in-memory retry — without advancing the offset**
  (Pulsar: native redelivery or re-produce; queues re-send a copy — not applicable to a replay stream).
- **Decision:** Mirror Kafka — `DeferAsync` re-injects for an in-memory retry. The envelope was `Track`ed on
  first receive and is not re-tracked, so the watermark holds below it until it is finally resolved by
  `CompleteAsync`.
- **Why:** It honors "defer = try again", is the Wolverine-idiomatic behavior for an offset/replay stream,
  and — crucially — is **bounded** under Wolverine's default failure handling: an unconfigured handler
  exception requeues (Defer → re-inject) for `MaximumAttempts−1` (default **2**) attempts, then
  `MoveToErrorQueue` → `CompleteAsync`, so the watermark **always advances** and there is no aging/stall.
  The old no-op neither retried nor terminated. A stall now only happens under a deliberately *unbounded*
  requeue policy (a consumer misconfiguration Kafka shares).
- **Consequences / caveat:** In **Inline/BufferedInMemory with no durable store**, `MoveToErrorQueue` writes
  to the `NullMessageStore` (a **no-op**) and then `CompleteAsync`s anyway — so a poison message is retried,
  then **silently discarded** (lost) while the watermark advances. No exception, no loop, no stall, but the
  message is gone. **Preserving poison messages requires a durable store / real DLQ** — i.e. the Durable
  inbox work (aspirational, see the divergences list). The consumer's standard Wolverine error policies
  (`RetryTimes`, `MaximumAttempts`, `MoveToErrorQueue`, scheduled retry) govern the outcome, so no separate
  defer-policy interface is added (keeps the public surface minimal).

## 9. Never cache an incomplete / failed authentication token
- **Date:** 2026-07-01 · **Status:** Accepted
- **Context:** The Layer-A "OAuth disabled at startup" test wedged the listener **permanently** (even after
  OAuth was re-enabled). Root cause chain: the consumer token handler returned a response with a **null
  `AccessToken`** (the TestHost token client deserialized a `400 invalid_client` body instead of throwing);
  `CachingAuthenticationTokenProvider` only null-checked the *response object*, not its contents, so it
  **cached the null-token entry** for the full TTL; `AddCallCredentials` then threw `ArgumentNullException`
  on `metadata.Add(…, null)`, surfacing as `RpcException Internal` — **not** `Unauthenticated` — so the
  `Invalidate()` path never fired and the poisoned cache entry was never cleared.
- **Decision:** The provider **validates the token before caching** — if `AccessToken`/`InstanceUri`/
  `TenantId` is empty it throws and caches nothing. The TestHost token client also **fails loud** on a
  non-success token response instead of producing a null-token.
- **Why:** A failed/empty fetch must not poison the cache. Caching nothing means every reconnect re-fetches,
  so recovery is automatic once auth is restored — without relying on the invalidate path, which only
  covers a gRPC auth *rejection* of an already-cached token (#3). Complements #3: #3 clears a
  cached-but-revoked token; #9 ensures a never-valid token is never cached in the first place.
- **Consequences:** Verified — OAuth-disabled now yields a clear `token request failed (400 …)` error, a
  graceful backoff loop (no crash), and recovery on re-enable. Regression tests added (incomplete token
  throws and is not cached → recovers on the next valid fetch).

## 8. In-process reconnect resumes from the handled watermark, not the durable store
- **Date:** 2026-07-01 · **Status:** Accepted
- **Context:** A topic reconnect (`TopicTransport.WriteFetchRequestAsync`) originally always resolved its
  resume position from `IReplayIdRepository.GetLastReplayIdAsync` — the last *durably-committed* replay
  id. Because commits are throttled (`commitEvery`, or an idle keep-alive), that position lags the
  actually-handled position by up to `commitEvery` events. The Layer-A **WiFi-death** test surfaced the
  effect: a reconnect redelivered every event handled since the last commit (7 duplicates in that run,
  because no commit had occurred pre-outage). Safe (at-least-once), but more redelivery than necessary.
- **Decision:** On an **in-process reconnect**, resume from the listener's in-memory *handled* watermark
  (`ReplayCommitTracker.TryGetResumePosition()` = lowest in-flight − 1, else high-water). A true **cold
  start** (fresh process — tracker empty → `null`) still reads the durable store. The value is threaded
  `tracker → listener → transport factory (Func<long?, ISubscriptionTransport>) → TopicTransport` and
  applied to the **initial** fetch only; `RequestMoreAsync` (mid-stream flow control) is unchanged. MES
  ignores it (server-side replay).
- **Why:** In-process we *know* which events were handled (`CompleteAsync` drove the watermark), so
  redelivering them on reconnect is wasteful. Resuming from the handled watermark eliminates reconnect
  duplicates while preserving at-least-once on a real restart (which reads the durable store, behind =
  safe). The resume anchor is the fully-handled watermark, not the raw received high-water, so in-flight
  (received-but-not-completed) events are still redelivered — no loss.
- **Consequences:** Verified on the WiFi-death rerun: reconnect resumed at the handled id, zero
  redelivery, contiguous ledger, no gap. The durable store can legitimately trail the in-memory position
  during a DB outage; the next successful commit writes the latest (monotonic) position and self-heals.

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
  - **Store decision: `WolverineFx.SqlServer`** (matches the stack). Parked pending live testing.
  - **Open gap — schema availability on restart-recovery (revisit after testing):** the durable inbox
    persists the envelope's raw `Data` (Avro) + headers; on a fresh-process restart the durability agent
    re-runs persisted envelopes through the pipeline → our **sync** `SalesforceAvroSerializer` reads the
    schema from the **in-memory** cache, which is empty on restart (the listener pre-fetches at receive,
    but recovery bypasses the listener). So recovered events would **dead-letter** — Durable wouldn't
    survive a restart, defeating its purpose. The hot path is unaffected (listener pre-fetch → cache hit).
    Candidate fixes to weigh after testing: (A) make the serializer `IAsyncMessageSerializer` — cache-hit
    fast path (hot path keeps auth-in-loop) + async schema fetch on miss (recovery); (B) persist the
    schema JSON in an envelope header (self-contained, storage bloat per event); (C) a durable
    schema-by-id cache/table (compact, more infra). Leaning A. **Do not implement until live testing
    informs the choice** (per user, 2026-06-30).
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
