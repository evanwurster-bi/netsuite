# Failure Scenarios — Before vs. After

This folder documents concrete ways the HubSpot → NetSuite integration could fail, **how the
previous implementation would have failed**, and **how the current implementation prevents
it**. Each scenario has two [Mermaid](https://mermaid.js.org/) sequence diagrams (rendered
inline on GitHub) — the "before" flow that breaks and the "after" flow that holds.

For the overall plan, phase breakdown, and risk register, see
[../../RELIABILITY.md](../../RELIABILITY.md).

## How to read these

- **Before** = the implementation prior to the reliability work (single-event webhook,
  `return 200` on failure, no lock, token minted per call, no signature check, etc.).
- **After** = the current implementation (batch enqueue, `ReportBatchItemFailures`, per-deal
  DynamoDB lock, idempotent reconcile, token cache, optional signature verification).
- ❌ marks where data is lost / corrupted / duplicated; ✓ marks where it is prevented.

## Participants used in the diagrams

| Name in diagrams | Component |
|---|---|
| HubSpot | HubSpot CRM (sends webhooks, exposes REST API) |
| Webhook | `WebhookFunction` — [lambda_functions/hubspot_webhook](../../lambda_functions/hubspot_webhook/lambda_function.py) |
| SQS | `HubSpotWebhookQueue` |
| DLQ | `HubSpotWebhookDLQ` |
| Processor | `ProcessorFunction` — [lambda_functions/hubspot_processor](../../lambda_functions/hubspot_processor/sqs_processor.py) |
| Lock | DynamoDB `SyncLockTable` — [locks.py](../../lambda_functions/hubspot_processor/locks.py) |
| NetSuite | NetSuite REST API |

## Index

| # | Scenario | Register risk |
|---|----------|---------------|
| [01](01-batched-webhook-truncation.md) | Batched webhook truncated to the first event | 2 — Lost events |
| [02](02-failures-silently-dropped.md) | Processing failures deleted, never retried | 4 — DLQ retry |
| [03](03-duplicate-invoice-concurrent-create.md) | Duplicate / dropped invoice creation | 3 — Idempotency |
| [04](04-parent-child-write-race.md) | Line-item + deal events race on one invoice | (parent/child) |
| [05](05-partial-write-hubspot-writeback-fails.md) | NetSuite write succeeds, HubSpot writeback fails | 6 — Integrity |
| [06](06-netsuite-rate-limit-and-token.md) | NetSuite rate-limit / token exhaustion | 4 — Shared budget |
| [07](07-unauthorized-webhook.md) | Forged / unauthorized webhook | 5 — Signature |
| [08](08-worker-crash-stuck-lock.md) | Worker crashes holding the lock | (lock design) |
| [09](09-malformed-data-poison-message.md) | Malformed data poisons a message | 6 / hardening |

## A note on the design principle

Most "after" flows rely on the same two properties, so they are stated once here:

1. **Reconcile, don't apply deltas.** Handlers re-read the full current state from HubSpot
   and upsert the whole record, keyed on `externalId = HubSpot object id`. Re-running an
   event therefore converges instead of duplicating.
2. **Serialize per parent deal.** A DynamoDB lock keyed on the parent deal id makes events
   that touch the same invoice mutually exclusive.

Together these mean the recovery strategy for almost every failure is **"raise → redrive →
re-run safely,"** with the DLQ + CloudWatch alarm as the backstop when an upstream
dependency is down for an extended period.
