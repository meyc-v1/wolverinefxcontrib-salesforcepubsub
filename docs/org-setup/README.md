# Org setup for the integration tests

The integration test suite (`tests/WolverineFxContrib.SalesforcePubSub.IntegrationTests`) runs
against a **live Salesforce org** — Salesforce is the broker, the same way Wolverine's own Kafka
tests run against a real Kafka. Before the suite can run, the org needs a small set of permanent
test fixtures, all prefixed **`WIT_`** (Wolverine Integration Test):

| Fixture | API name | Created by |
|---|---|---|
| Platform event | `WIT_Event_A__e` | **you, manually** (step 1) |
| Platform event | `WIT_Event_B__e` | **you, manually** (step 1) |
| Custom channel | `WIT_Channel__chn` (members: A + B) | Bruno collection (step 2) |
| Managed event subscription | `WIT_Event_A_Sub` (over `/event/WIT_Event_A__e`) | Bruno collection (step 2) |
| Managed event subscription | `WIT_Channel_Sub` (over `/event/WIT_Channel__chn`) | Bruno collection (step 2) |

These are **create-once, permanent** fixtures. Do not delete them between runs — the suite
isolates itself by correlation ids in the event payload, not by recreating org metadata. You only
repeat this setup after a sandbox refresh (or when pointing the suite at a new org).

> Why is step 1 manual? Platform event *definitions* are metadata that can only be created via the
> SOAP Metadata API — there is no REST call for it, and we deliberately do not ship a metadata
> deploy. Creating two objects by hand is ~5 minutes and cannot affect anything else in the org.
> Everything else is plain Tooling API REST and is scripted.

## Prerequisites

- A Salesforce user with **Customize Application** (and View Setup and Configuration) in the
  target org — needed for both the manual step and the Bruno collection.
- [Bruno](https://www.usebruno.com/) to run the collection in `bruno/`.
- An access token for that user. Any source works; if you have the `sf` CLI authed, the quickest
  is `sf org display --target-org <alias>` and copy the access token and instance URL.

The *test suite itself* does not need any of this — it runs with the subscriber (connected app)
credentials and never creates org metadata. Its startup preflight verifies these fixtures exist
and points back at this README if anything is missing.

## Step 1 — create the two platform events manually

In **Setup → Integrations → Platform Events → New Platform Event**, create both of these,
identically except for the name:

| Setting | Event 1 | Event 2 |
|---|---|---|
| Label | `WIT Event A` | `WIT Event B` |
| Plural Label | `WIT Event A` | `WIT Event B` |
| Object Name | `WIT_Event_A` | `WIT_Event_B` |
| Publish Behavior | **Publish After Commit** | **Publish After Commit** |

Then on each event, under **Custom Fields & Relationships → New**:

| Setting | Value |
|---|---|
| Type | **Text** |
| Field Label | `Message` |
| Length | `255` |
| Field Name | `Message` |

Resulting API names must be exactly `WIT_Event_A__e` / `WIT_Event_B__e`, each with a `Message__c`
field — the Bruno collection's first requests verify this and the rest of the collection will fail
if a name is off.

Finally, make sure the **integration user the test suite authenticates as can subscribe**: its
profile or a permission set must grant Read on both platform events.

## Step 2 — run the Bruno collection

Open `bruno/` as a collection in Bruno (or copy the requests into an existing Salesforce
collection). It expects three environment variables (see `environments/example.bru`):

| Variable | Meaning | Example |
|---|---|---|
| `_endpoint` | Org instance URL, no trailing slash | `https://your-org.sandbox.my.salesforce.com` |
| `version` | API version, without the `v` | `62.0` |
| `accessToken` | Access token for the Customize Application user | from `sf org display` |

Run the requests **in sequence** (they are numbered):

1. **01–03 verify** the two platform events and their `Message__c` fields exist (fail loud here
   if the manual step has a typo — nothing has been created yet).
2. **04–06 create** the channel, then its two members.
3. **07–08 create** the two managed event subscriptions.
4. **09–10 verify** everything, listing what now exists.

Each create returns `201` with `{"success": true}`. **Re-running a create against an org where the
fixture already exists fails with a duplicate-value error — that is expected and harmless**; use
the verification requests (01–03, 09–10) to check the org's state at any time.

## Step 3 — the two External Client Apps (ECAs)

The suite (and the TestHost) authenticate with **two ECAs, one per role**, so the token lifecycles are
independent (revoking one can never touch the other) and each run-as user carries only its minimal
permission:

| ECA | Used by | Run-as user needs |
|---|---|---|
| Subscriber | the transport (`IAuthenticationTokenHandler`) | **Read** on `WIT_Event_A__e` and `WIT_Event_B__e` |
| Publisher | the REST publish helper (External.Salesforce lib) | **Create** on both events |

Create both in **Setup → External Client App Manager** with OAuth + the client-credentials flow enabled
and a run-as user assigned. Then the part that is easy to miss: with **Permitted Users = "Admin
approved users are pre-authorized"** (the default), each app rejects its own run-as user
(`invalid_app_access: user is not admin approved to access this app`) until you attach the user's
profile or permission set **on the app's Edit Policies page** (External Client App Manager → the app's
row actions → **Edit Policies** → under OAuth policies) — the grant is *not* made from inside the
Permission Set editor.

## Machine setup — user secrets

Both the TestHost and the integration tests read one .NET user-secrets store, id
**`wolverine.salesforcepubsub`**. On a new machine, populate it with (values from your org/ECAs):

```json
{
  "subscriberAuthenticationSettings": {
    "ClientId": "<subscriber ECA consumer key>",
    "ClientSecret": "<subscriber ECA secret>",
    "LoginUri": "https://<your-org>.sandbox.my.salesforce.com/"
  },
  "publisherAuthenticationSettings": {
    "ClientId": "<publisher ECA consumer key>",
    "ClientSecret": "<publisher ECA secret>",
    "LoginUri": "https://<your-org>.sandbox.my.salesforce.com/"
  },
  "salesforceSettings": {
    "baseUri": "https://<your-org>.sandbox.my.salesforce.com/services/data/v64.0/"
  },
  "durabilitySettings": {
    "connectionString": "<SQL Server for the Wolverine message store — Durable endpoints/facts; AAD auth needs no password>"
  },
  "salesforceReplaySettings": {
    "connectionString": "<SQL Server for the TestHost replay checkpoint table — see the TestHost Replay/CreateReplayTable.sql>"
  }
}
```

Notes: `baseUri` needs the API version segment and the trailing slash. The two connection strings are
optional — the Durable integration facts and the TestHost's SQL replay store skip / fall back to
in-memory without them (everything else runs). `salesforceSettings:pubSubUri` may also be set to
override the default public gRPC endpoint. The suite fails fast with the missing key's name if a
required section is absent.

## Notes

- **Managed event subscriptions take a few minutes to provision.** After creating them, wait
  ~5 minutes before the first test run; a subscribe attempt before propagation completes can fail
  even though the create succeeded.
- **MES slots are exclusive per client.** Only one process can hold `WIT_Event_A_Sub` /
  `WIT_Channel_Sub` at a time; the suite's MES tests skip (not fail) when the slot is held.
- The `WIT_` fixtures are intentionally disjoint from any other subscriptions in the org — the
  test suite never touches channels, subscriptions, or replay state belonging to anything else.
- Two Salesforce behaviors worth knowing when writing new tests: near-simultaneous REST publishes can
  receive event-bus positions **opposite** to POST order (space order-sensitive publishes), and replay
  ids are positions on the org's shared bus, so other org activity creates gaps between your events —
  never assert contiguity.
