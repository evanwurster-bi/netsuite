# Integration Reliability & Consistency

This document tracks the work to harden the HubSpot → NetSuite integration against the
"1% of cases" failure modes (duplicates, lost events, race conditions, unauthorized
requests). It maps each risk from the risk register to concrete code, records design
decisions, and tracks what is done vs. pending.

> **See also:** [docs/failure-scenarios/](docs/failure-scenarios/) — one document per concrete
> failure mode, each with before/after sequence diagrams showing how the old implementation
> would have failed and how the current one prevents it.

> Architecture recap:
> `HubSpot → WebhookFunction → SQS (HubSpotWebhookQueue) → ProcessorFunction → NetSuite/HubSpot`
> The natural idempotency key everywhere is **NetSuite `externalId` = HubSpot object id**
> (deal id for invoices, payment id for payments, venue id for locations).

---

## Risk register status

| # | Risk | Priority | Register status | Phase | Code status |
|---|------|----------|-----------------|-------|-------------|
| 2 | Lost events: webhook batch truncated to first event | High | In&nbsp;Progress | 1 | ✅ Done |
| 3 | Duplicate invoices from webhook retries (idempotency) | Medium | In&nbsp;Progress | 2 | ✅ Done |
| 4 | Shared NetSuite rate/concurrency budget (DLQ retry) | Medium | In&nbsp;Progress | 1 / 3 | ✅ Done |
| 5 | HubSpot webhook signature verification | Low | In&nbsp;Progress | 4 | ✅ Done (default OFF) |
| 6 | Guarantee integrity in NetSuite data changes (rollback) | Low | In&nbsp;Progress | 5 | ✅ Done |
| — | Parent/child ordering (line-item vs deal events) | — | Raised in review | 2 | ✅ Done |

---

## Key design principle: reconcile, don't apply deltas

Both invoice handlers re-read the **full current state** from HubSpot (the deal and **all**
its line items) and upsert the whole invoice. The webhook is only a *trigger*; the data
comes from a live read. Consequences we rely on:

- **Ordering is not required — mutual exclusion per deal is.** Whichever event runs *last*
  reads current state and writes it, so the invoice converges. We only need to stop two
  reconciliations of the same invoice from overlapping.
- **Retries are safe.** Re-processing an event re-reads state and re-upserts on the same
  `externalId`, so it cannot create a duplicate.

This is why the fix for parent/child consistency is a **per-deal lock**, not a global
ordering mechanism.

---

## Phased plan

| Phase | Goal | Risks | Status |
|-------|------|-------|--------|
| 0 | Infrastructure prerequisites | enables 3, 4, parent/child | ✅ Done |
| 1 | Stop active data loss | 2, 4 (partial) | ✅ Done |
| 2 | Parent/child consistency + idempotency | 3, parent/child | ✅ Done |
| 3 | NetSuite load & reliability | 4 (rest) | ✅ Done |
| 4 | Security | 5 | ✅ Done (default OFF) |
| 5 | Integrity / status semantics | 6 | ✅ Done |
| 6 | Code-quality hardening | — | ✅ Done |

---

## Phase 0 — Infrastructure prerequisites ✅

All in [`template.yaml`](template.yaml). No runtime behavior change on its own.

- **`SyncLockTable`** — DynamoDB table for the per-deal lock (PK `deal_id`, TTL attribute
  `expiresAt`, `PAY_PER_REQUEST`). Used by Phase 2.
- **`ReportBatchItemFailures`** added to the processor's SQS event source so the handler can
  report per-message failures (pairs with Phase 1).
- **`HubSpotWebhookDLQAlarm`** — CloudWatch alarm that fires when the DLQ is non-empty.
- Processor granted `DynamoDBCrudPolicy` on the table and a `SYNC_LOCK_TABLE` env var.

## Phase 1 — Stop active data loss ✅

### Risk 2 — every event in a batch is enqueued
[`lambda_functions/hubspot_webhook/lambda_function.py`](lambda_functions/hubspot_webhook/lambda_function.py)

- **Before:** `_parse_body` returned `payload[0]` for an array body — HubSpot batches
  multiple events per POST, so every event after the first was silently dropped.
- **After:** `_parse_events` returns *all* events; the handler validates each and enqueues
  them with `send_message_batch` (chunked at SQS's 10-per-call limit). One SQS message per
  event → independent retry / DLQ / idempotency per event.
- Partial enqueue failure → HTTP 500 so HubSpot resends the whole delivery; re-enqueue is
  safe because downstream syncs are idempotent on the HubSpot object id.

### Risk 4 (partial) — failures actually retry and reach the DLQ
[`lambda_functions/hubspot_processor/sqs_processor.py`](lambda_functions/hubspot_processor/sqs_processor.py)

- **Before:** `lambda_handler` caught per-record errors, incremented a counter, and still
  returned HTTP 200 → SQS deleted *all* messages, including failed ones. Nothing was ever
  retried and the DLQ stayed empty.
- **After:** returns `{"batchItemFailures": [...]}`. Only failed message ids are redriven;
  after `maxReceiveCount` (5) attempts they land in the DLQ and trip the alarm.

## Phase 2 — Parent/child consistency + idempotency ✅

### Per-deal serialization (the core fix)
New module [`lambda_functions/hubspot_processor/locks.py`](lambda_functions/hubspot_processor/locks.py)

- `deal_lock(key)` is a context manager backed by DynamoDB: a conditional `put_item`
  acquires the lock (`attribute_not_exists OR expiresAt < now`); if another worker holds it,
  it raises `LockNotAcquired` so the SQS message redrives and retries later.
- **TTL safety net:** `expiresAt` (default 960s, ≥ the SQS visibility timeout) releases a
  lock if a worker dies mid-process.
- **Fencing token:** each acquisition stores a unique `owner`; release is a conditional
  delete on that token, so a worker can never delete a lock that a *later* worker
  re-acquired after the first expired.

In [`sqs_processor.py`](lambda_functions/hubspot_processor/sqs_processor.py):

- `_resolve_lock_key()` maps every event to its **parent deal id** — deal events use their
  own id; line-item events resolve the parent via `get_line_item_deal_id` in
  [`hubspot.py`](lambda_functions/hubspot_processor/hubspot.py); venue events use a
  `venue:<id>` key (locations are shared, not per-deal). Payment events skip locking.
- `process_webhook_message()` acquires `deal_lock(key)` and then dispatches via
  `_dispatch_webhook_message()`. Concurrent deal + line-item events for the same invoice now
  process one at a time → no duplicate invoices, no NetSuite "Record has been changed".

### Idempotent reconcile + create-collision recovery (Risk 3)

- `process_deal_to_invoice` is split into a thin validator + `reconcile_deal_invoice(deal_id)`
  — a single, idempotent "build the invoice from current state and upsert" routine.
- `create_or_update_invoice` ([`netsuite_auth.py`](lambda_functions/hubspot_processor/netsuite_auth.py))
  now recovers from a duplicate-`externalId` POST: if the create fails and an invoice with
  that `externalId` already exists, it re-resolves and PATCHes instead of creating a second
  invoice. (The per-deal lock makes this collision rare; this is defense in depth.)

### Retryable vs. permanent failures

Three explicit outcomes, chosen so a failure is immediately visible in both the code and the
logs:
- **`True`** — successful sync or intentional skip → **acked**, no log noise.
- **`False`** — permanent business rejection (no billing contact, missing venue, bad data) →
  **acked immediately** (retrying cannot help) but logged loudly as `[rejected] ...` and
  recorded on the deal via `netsuite_invoice_status`. It is never silently swallowed and never
  churns the DLQ.
- **raises** — transient problem (lock contention, NetSuite 5xx, network) → reported in
  `batchItemFailures` so SQS redrives it, reaching the DLQ + alarm if it persists. The previous
  `except: return False` swallow-all blocks were changed to `raise` so transient errors are
  never misclassified as permanent.

### Parent-before-child without infinite retries

- A line-item event for a deal whose invoice doesn't exist yet **skips** (acked) instead of
  erroring. No data is lost: when the deal later syncs, `reconcile_deal_invoice` reads **all**
  current line items, including that change.

---

## Decisions log

- **DynamoDB lock over FIFO queue** — keeps the webhook fast and resolves the parent deal id
  in the processor (where those calls already happen), instead of forcing an association
  lookup into the inbound hot path. Given the reconcile-from-state model, mutual exclusion is
  sufficient for correctness; strict FIFO ordering is not needed.
- **Line-item handler kept as a lines-only update (not fully merged into the deal reconcile)**
  — full unification would change business behavior (e.g. apply the deal-stage / venue gates
  to line-item updates) and risk regressing existing flows. Both paths run under the same
  per-deal lock, which already removes the concurrency hazard. Revisit if a single reconcile
  path is desired.
- **Stale-event guard (skip writes older than the last sync) — deferred (LATER, not
  planned now).** It would require a NetSuite custom body field (e.g.
  `custbody_hs_last_sync`) to store the last-synced `hs_lastmodifieddate`, and writing to a
  field that doesn't exist would break invoice upserts. **Decision (confirmed): we are not
  adding that field for now**, so this guard stays a future item. The per-deal lock already
  covers the primary (concurrency) risk; this guard would only add protection against
  genuinely out-of-order HubSpot delivery. Revisit only if/when the custom field is created.

---

## Deploy & verify

```bash
# from the sandbox/ folder
sam validate --lint
sam build  --config-file "samconfig accounts/duvall.toml"
sam deploy --config-file "samconfig accounts/duvall.toml"
```

Replace `duvall` with `bestimpressions` or `rockytop`. See
[samconfig accounts/README.md](samconfig%20accounts/README.md) for shared vs per-account settings.

Roll out **Duvall → Best Impressions → Rocky Top**, watching the DLQ alarm after each.
When validation is complete, promote the same template to the primary sandbox stacks and retire
the `-reliability` stacks if they are no longer needed.

### What was tested locally
- `py_compile` on all changed modules.
- Template parsed with a CloudFormation-tag-aware loader; lock table, alarm, policy,
  `FunctionResponseTypes`, TTL, `ScalingConfig`, `Conditions`, and env vars all present.
- Webhook batch parser: 3-event array with one invalid → 2 valid; single object still works;
  23 events chunk to 10/10/3.
- Lock module: acquire/release round-trip, contention raises `LockNotAcquired`, and the
  fencing-token guard prevents lock-stealing after TTL expiry.
- NetSuite client: `Retry-After` parsing (numeric / HTTP-date ignored / clamped); token
  cache (2 requests → 1 token call; forced expiry → refresh).
- Signature verification: valid accepted; tampered body / wrong secret → mismatch; stale
  timestamp rejected; missing secret fails closed; missing headers rejected.
- `map_to_netsuite_format`: missing/malformed date omits `tranDate`; valid date set.
- Webhook misconfig guard: empty `SQS_QUEUE_URL` → 500.

> `sam validate`/`sam build` were not run here (SAM CLI not installed on the dev machine) —
> run them before deploying.

### Automated tests
A runnable unit suite lives in [tests/](tests/) — one group of tests per failure scenario,
mocking AWS/NetSuite/HubSpot so it needs no credentials:

```bash
pip install pytest -r lambda_layers/netsuite_dependencies/requirements.txt
pytest tests/ -q
```

See [tests/README.md](tests/README.md) for the scenario-to-test mapping and the
integration/end-to-end (sandbox) checks to run before merge.

---

## Phase 3 — NetSuite load & reliability (rest of Risk 4) ✅

[`netsuite_auth.py`](lambda_functions/hubspot_processor/netsuite_auth.py)

- **Token caching** — `get_access_token` now caches the token on the instance and reuses it
  until ~60s before expiry (uses the endpoint's `expires_in`). The processor keeps a
  module-level `NetSuiteAuth`, so the cache survives warm invocations. Previously a JWT +
  token call was made on **every** request; this roughly halves NetSuite calls and latency.
- **Honor `Retry-After`** — the 429 retry path uses `_parse_retry_after(...)` (numeric
  seconds, clamped to ≤60s; HTTP-date form falls back to exponential backoff).

[`template.yaml`](template.yaml)

- **`ProcessorMaxConcurrency`** (default **2**, min 2) wired to `ScalingConfig.MaximumConcurrency`
  on the SQS event source — caps how many processor Lambdas hit NetSuite at once so parallel
  invocations don't exceed the shared concurrency budget.
- **`ApiCallDelaySeconds`** (default `"0.5"`) → `API_CALL_DELAY_SECONDS` env var, read by
  `_sleep_between_api_calls`. Tunable, and can be set to `"0"` to disable the padding now
  that 429s are retried and concurrency is capped.

**Deliberately retained:** the `time.sleep(1)` calls *after* invoice/payment writes are
**read-after-write consistency waits**, not rate-limit padding — NetSuite's `externalId`
search is eventually consistent, so a just-created invoice may not be findable immediately.
These are left in place; only the configurable `_sleep_between_api_calls` padding changed.

### Known trade-off: lock contention vs. visibility timeout
The queue's `VisibilityTimeout` is 960s (it must be ≥ the 900s function timeout). When a
message loses the per-deal lock race, it is reported as a batch item failure and only
becomes visible again after that timeout — so a contended event can wait up to ~16 min
before its (then-successful) retry. With `ProcessorMaxConcurrency = 2` same-deal overlap is
rare, and the holder finishes well within one visibility cycle, so contention costs ~1 extra
receive (not the full `maxReceiveCount`) and does **not** cause false DLQ entries. If lower
retry latency is ever needed, options are a dedicated short-visibility retry queue or a FIFO
queue keyed by deal id.

## Phase 4 — Security: HubSpot signature verification (Risk 5) ✅ — default OFF

[`lambda_functions/hubspot_webhook/lambda_function.py`](lambda_functions/hubspot_webhook/lambda_function.py)

- `_verify_signature` validates the **HubSpot v3** signature: base string
  `method + requestUri + body + timestamp`, HMAC-SHA256 with the app client secret,
  base64-encoded, compared in **constant time** (`hmac.compare_digest`). Requests with a
  timestamp older than 5 minutes are rejected (replay guard). On any failure the webhook
  returns **401** before enqueuing.
- `_request_uri` rebuilds the signed URI from the API Gateway (HTTP API v2) event;
  `_raw_body` returns the exact body (base64-decoded if needed).
- **Fail-closed:** when verification is enabled but the secret is missing, requests are
  rejected (never silently allowed).

### Feature flag — OFF by default
- `HubSpotSignatureVerificationEnabled` (default **"false"**) → env var
  `HUBSPOT_SIGNATURE_VERIFICATION_ENABLED`. While off, the webhook behaves exactly as before
  (no verification), so the integration keeps working until the secret is provisioned.
- `HUBSPOT_CLIENT_SECRET` is resolved from Secrets Manager **only when the flag is on**
  (CloudFormation `Conditions` + `!If … AWS::NoValue`). This means deploys succeed *before*
  the `hubspot_client_secret` key exists — it is required only to turn verification on.

### To enable (per account, when ready)
1. Add the HubSpot app **client secret** to the account's Secrets Manager secret under key
   `hubspot_client_secret` (see the secret JSON shape in [README.md](README.md)).
2. Deploy that account with `HubSpotSignatureVerificationEnabled=true` (override the
   parameter in the account's `samconfig` or on the `sam deploy` command line).
3. Confirm: a valid HubSpot webhook still returns `204`; a forged/unsigned request returns
   `401`.

## Phase 5 — Integrity / status semantics (Risk 6) ✅

[`sqs_processor.py`](lambda_functions/hubspot_processor/sqs_processor.py)

- **Confirm before stamping.** `reconcile_deal_invoice` checks `create_or_update_invoice`'s
  result **before** writing `netsuite_invoice_status`; a non-success upsert **raises**
  (→ retry) instead of marking the deal "Invoice created" with no invoice behind it.
- **Take the new id from the write response, not a search.** `create_or_update_invoice` and
  `create_or_update_venue` return the record's internal id read from the create response's
  `Location` header (strongly consistent), and `get_invoice_number` then does a direct GET by
  id. We no longer re-search by `externalId` right after a write — that search is *eventually
  consistent*, so the old "verify by search then raise if missing" logic made a just-created
  record look missing, raised, and left the (already-processed) SQS message redriving forever
  — "processed but always in flight." Where a search read-back is still unavoidable
  (payments, a venue create without a `Location`), `_read_back` retries a few times and, if
  the record still isn't visible after a confirmed-success write, acks with a loud
  `[rejected]` log instead of looping.
- **Rollback strategy = idempotent reconcile.** NetSuite REST has no multi-record
  transaction, so we rely on ordering + idempotency instead: the invoice upsert is the last
  commit, and earlier steps (venue/location) are keyed on `externalId`, so a retry
  re-converges rather than duplicating. The `netsuite_invoice_status` field records
  business rejections as the audit trail.

## Phase 6 — Code-quality hardening ✅

- **Injection:** `get_customer_by_email` escapes `"` in the REST `q` filter (the SuiteQL
  helpers already escaped `'`).
- **No `None` return:** `get_or_create_venue` now always returns the location dict or raises,
  so callers can safely read `.get('id')` (previously it could fall through to `None` and
  crash, or silently create a second location).
- **`make_request`:** guards the possibly-unbound `response` (raises a clear error instead of
  returning an undefined value if every attempt failed without a response).
- **Null-safe dates:** `map_to_netsuite_format` guards a missing/malformed
  `event_start_date_and_time` — it logs and omits `tranDate` (NetSuite defaults to today)
  instead of crashing the whole deal sync.
- **Fail loud on misconfig:** removed the placeholder `SQS_QUEUE_URL` fallback; the webhook
  returns 500 if the queue URL is unset rather than publishing to a fake queue.
- **PII:** dropped full webhook-payload / venue / invoice / subsidiary object dumps from
  INFO to DEBUG.
