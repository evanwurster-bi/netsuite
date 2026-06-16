# 05 — NetSuite write succeeds, HubSpot writeback fails

**Register risk:** 6 — Integrity in NetSuite data changes (Low, depends on 3)
**Code:** [sqs_processor.py](../../lambda_functions/hubspot_processor/sqs_processor.py)

## The situation

Processing an invoice is multi-step: resolve venue/location → upsert the NetSuite invoice →
write `netsuite_invoice_number` / `netsuite_invoice_status` back to the HubSpot deal. What if
an API error hits **in the middle** — most importantly, the NetSuite invoice is created/updated
successfully but the **HubSpot writeback then fails**? NetSuite REST has no multi-record
transaction, so there is nothing to "roll back."

## Before — inconsistent state, no recovery

The upsert result was not checked, and any error was swallowed into a dropped event:

```python
netsuite.create_or_update_invoice(netsuite_invoice, netsuite_invoice_id)  # result ignored
time.sleep(1)
created_invoice_id = netsuite.get_invoice_by_deal_id(deal_id)
netsuite_invoice_number = netsuite.get_invoice_number(created_invoice_id)  # crashes if None
hubspot.update_deal_properties(deal_id, {...})   # if this throws → swallowed, no retry
```

```mermaid
sequenceDiagram
    participant Processor
    participant NetSuite
    participant HubSpot
    Processor->>NetSuite: create/update invoice ✓
    Processor->>HubSpot: update_deal_properties(invoice #, status)
    HubSpot-->>Processor: 502 (transient)
    Note over Processor: old handler swallows → returns 200
    Note over NetSuite,HubSpot: invoice exists, but deal never stamped<br/>no retry → permanently inconsistent ❌
```

### How it failed
The NetSuite invoice existed, but the HubSpot deal showed no invoice number/status, **forever**
— the event was acked and never retried. Worse, `get_invoice_number(None)` could itself throw
if the read-after-write lagged, masking the real state.

## After — confirm, then retry idempotently

The upsert result is checked **before** stamping the deal, and the new invoice id comes
straight from the write response — not a follow-up `externalId` search — so a genuine failure
raises (→ retry) while a just-created invoice is never mistaken for "missing". Because every
step is idempotent, the retry converges.

```python
# Returns the internal id from the write response (Location header / known id) — NOT an
# eventually-consistent externalId search. Falsy only on a real non-success -> raise.
created_invoice_id = netsuite.create_or_update_invoice(netsuite_invoice, netsuite_invoice_id)
if not created_invoice_id:
    raise RuntimeError(...)                       # real failure -> retry
netsuite_invoice_number = netsuite.get_invoice_number(created_invoice_id)  # direct GET, consistent
hubspot.update_deal_properties(deal_id, {...})    # raises on failure -> redrive
```

> **Why not re-search by `externalId` here?** That search is *eventually consistent*. An
> earlier version verified the write by searching and raised if it came back empty — but right
> after a create the search index often hasn't caught up, so it raised on a perfectly good
> invoice and left the (already-processed) SQS message redriving forever: **"processed but
> always in flight."** Reading the id from the `Location` header is strongly consistent and
> avoids that trap. Where a search read-back is unavoidable (payments, venue create without a
> `Location`), `_read_back` retries briefly and then acks with a loud `[rejected]` log rather
> than looping.

```mermaid
sequenceDiagram
    participant SQS
    participant Processor
    participant NetSuite
    participant HubSpot
    SQS->>Processor: deal event (attempt 1)
    Processor->>NetSuite: create/update invoice ✓ (externalId=deal)
    Processor->>HubSpot: update_deal_properties(...)
    HubSpot-->>Processor: 502 (transient)
    Processor-->>SQS: batchItemFailure → redrive
    Note over Processor: lock released; NO duplicate created
    SQS->>Processor: deal event (attempt 2)
    Processor->>NetSuite: get_invoice_by_deal_id → found → PATCH (idempotent) ✓
    Processor->>HubSpot: update_deal_properties(...) ✓
    Note over NetSuite,HubSpot: NetSuite + HubSpot consistent ✓
```

### How it's prevented (the "overpass")
- **No rollback needed — idempotent re-run instead.** The invoice upsert is the last NetSuite
  commit, keyed on `externalId`; earlier steps (venue/location) are also idempotent on
  `externalId`. Re-running converges rather than duplicating.
- **The writeback is re-attempted on every retry.** A single `update_deal_properties` PATCH
  writes all three properties atomically, so there is no partial property state; the recomputed
  status is stable across attempts.
- **Backstop:** if HubSpot is down past 5 attempts, the message hits the DLQ + alarm.
  Redriving it once HubSpot recovers safely completes the writeback (reconcile is idempotent).

### The same guarantee for payments and venues
- **Payments** (`process_payment`): the *write* result is checked (a real failure raises →
  retry); the existence verification afterwards uses `_read_back` (bounded retry for search
  lag) and, if the payment still isn't visible after a confirmed-success write, acks with a
  loud `[rejected]` log instead of looping.
- **Venues** (`process_venue`): the location id is taken from the write response; the
  HubSpot back-reference write **raises** on failure so it redrives (re-running is safe — the
  location upsert and the property write are both idempotent). If only the id can't be
  resolved after a successful write, it acks and defers the back-reference to a later event.

### Residual notes
There is a brief eventual-consistency window between the successful NetSuite write and a
successful writeback during which the HubSpot deal looks stale. It closes on the next retry;
it is a visibility lag, not data loss or duplication.
