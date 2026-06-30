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
secondary) is the basis for resolving indecision, not guesswork. Where the port strays from Wolverine's
conventions or doesn't fully implement what Wolverine expects, the rule is: **implement it Wolverine's
way, or document why we aren't/can't** — and capture it under "Divergences & gaps" below.

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
- **Date:** 2026-06-24 · **Status:** Deferred
- **Context:** Replay is committed after the batch is dispatched; `CompleteAsync`/`DeferAsync` are no-ops.
- **Decision:** Ship with the default **Inline** mode, where `ReceivedAsync` runs the handler before the
  ack → effective at-least-once (crash mid-batch ⇒ re-delivery). Defer the robust seam (per-envelope
  commit, keepalive-advance, handler-failure policy, safe Buffered).
- **Why:** Matches the old library's behavior and is acceptable for now; the robust seam is significant
  work not currently needed.
- **Consequences:** Buffered mode is at-most-once until the seam is built. Revisit when durability needs
  harden.

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

- **Hand-rolled listener config** — the port wrote a bespoke fluent class instead of deriving from
  `ListenerConfiguration<,>`. → **Resolved** (now derives — #5).
- **Inherited serializer/encryption knobs are inert** — Wolverine assumes its serialization pipeline; we
  bypass it by deserializing Avro ourselves. → **Resolved / documented** (#7).
- **`CompleteAsync`/`DeferAsync` are no-ops** — Wolverine's `IListener` contract expects per-envelope
  ack/defer; we commit replay at the batch level in the consume loop instead. The most significant
  under-implementation of what Wolverine expects. → **Open / Deferred** (the at-least-once seam — #2).
- **Batch receive overload unused** — Wolverine's `IReceiver` offers `ReceivedAsync(Envelope[])`; we feed
  one envelope at a time. → **Deferred** (#6).
- **No sender** — `SalesforceEndpoint.CreateSender` throws `NotSupportedException`. Wolverine endpoints
  can send; ours is listen-only by design. → **Intentional / documented** (listen-only).

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
