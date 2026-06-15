# Tests

Unit tests that simulate each risk scenario from
[../docs/failure-scenarios/](../docs/failure-scenarios/) and assert the integration handles
it correctly. They mock the AWS / NetSuite / HubSpot boundaries, so they need **no
credentials and hit no network** — safe to run anywhere, including CI.

## Run

```bash
pip install pytest cryptography pyjwt requests boto3 python-dotenv
# (or: pip install -r ../lambda_layers/netsuite_dependencies/requirements.txt ; pip install pytest)

pytest tests/ -q          # from the repo root
pytest tests/ -v          # verbose, one line per scenario
```

`tests/conftest.py` puts the Lambda source on `sys.path`, sets dummy env vars, and generates
a throwaway EC key so the modules import without real credentials.

## What each file covers

| File | Scenario(s) | Asserts |
|------|-------------|---------|
| `test_01_webhook_batch_and_signature.py` | 01, 07 | every batch event enqueued; partial-enqueue → 500; missing queue URL → 500; v3 signature valid/tampered/stale/missing/handler-401 |
| `test_02_lock_and_lock_key.py` | 03, 04, 08 | lock acquire/release, contention raises, TTL re-acquire, no lock-stealing; line-item/payment/deal resolve to the **same** parent-deal key; venue uses `venue:<id>` |
| `test_03_processor_retry_semantics.py` | 02 | exception → `batchItemFailures`; `False` → redrive; `True` → ack; only the failing record reported |
| `test_04_netsuite_auth.py` | 06, 03 | `Retry-After` parsing; token cached + refresh on expiry; duplicate-`externalId` POST recovers via PATCH |
| `test_05_mapping_null_date.py` | 09 | missing/malformed `event_start_date_and_time` omits `tranDate` (no crash); valid date set |
| `test_06_reconcile_partial_failure.py` | 05 | happy path; writeback failure raises (retryable) with invoice already committed; retry does not duplicate; failed upsert never stamps the deal |

## What these unit tests do *not* cover

They prove the **logic** of each scenario. They do **not** exercise the real AWS wiring —
actual SQS redelivery, the DynamoDB conditional writes under true concurrency, the DLQ
threshold, or the CloudFormation template. For that, use the layers below.

### Integration (LocalStack — optional)

Run SQS + DynamoDB locally and drive `lambda_handler` against them to verify the
`ReportBatchItemFailures` redrive path and the conditional lock writes end-to-end.

### End-to-end (sandbox stack — recommended before merge)

Deploy an **independent** sandbox stack and validate against real HubSpot/NetSuite sandboxes:

1. `sam validate --lint && sam build && sam deploy --config-file "samconfig accounts/samconfig.duvall.toml"`
   (ideally a *separate* stack name so you can compare old vs. new side by side).
2. **Batch (01):** POST a webhook body containing an array of several events → confirm one SQS
   message per event and all syncs applied.
3. **Retry/DLQ (02):** point at an invalid NetSuite cred briefly (or force a 5xx) → confirm the
   message redrives and lands in the DLQ, and that `HubSpotWebhookDLQAlarm` fires.
4. **Parent/child (03/04):** fire a `line_item.propertyChange` and a `deal.propertyChange` for
   the same deal at the same time → confirm a single invoice with correct lines and no
   `Record has been changed` error; check only one lock row appears in `SyncLockTable`.
5. **Partial failure (05):** revoke the HubSpot token mid-test → confirm the NetSuite invoice
   exists, the message redrives, and a retry completes the deal writeback (no duplicate).
6. **Signature (07):** set `HubSpotSignatureVerificationEnabled=true` with the secret →
   confirm a real HubSpot webhook returns `204` and a forged/unsigned POST returns `401`.
