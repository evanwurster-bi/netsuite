# HubSpot → NetSuite (Sandbox)

AWS SAM application that receives HubSpot webhooks, buffers them in SQS, and syncs deals, venues, and line items into NetSuite sandbox (`3763009-sb1`).

One codebase deploys per client account. Application source and non-secret configuration live in git. Credentials never enter the repository — local files for development, AWS Secrets Manager for deployed Lambdas.

**Scope:** this repository targets **sandbox** environments only. Production stacks under `../production/` are out of scope for current work. Deploy configs in `samconfig accounts/` point at **reliability** stacks used for E2E validation (`hs-netsuite-sandbox-{account}-reliability`).

## What it does

HubSpot sends webhooks when deals, line items, or venues change. The integration does **not** apply each webhook as a delta. It **re-reads the full current state** from HubSpot (deal + line items) and **upserts** the NetSuite invoice keyed by `externalId = HubSpot deal id`. The webhook is only a trigger; retries and duplicate deliveries converge on the same invoice.

| Trigger | NetSuite target |
|---------|-----------------|
| Deal (create / property change) | Invoice create or update |
| Line item (create / change / delete) | Reconcile parent deal invoice lines (when invoice exists) |
| Venue (create / change) | NetSuite `location` + write-back of `netsuite_id` on the venue |
| Payment webhooks | Acknowledged only (sync disabled) |

## Architecture

```text
HubSpot → API Gateway (WebhookFunction) → SQS → ProcessorFunction → NetSuite / HubSpot APIs
                                              ↓
                                         DLQ + CloudWatch alarm
                                              ↓
                                    SyncLockTable (DynamoDB)
```

| Component | Role |
|-----------|------|
| `WebhookFunction` | Validates (optional signature), parses batched POST bodies, enqueues **one SQS message per event** |
| `ProcessorFunction` | Business rules, per-deal lock, reconcile, HubSpot write-back (`netsuite_invoice_status`, etc.) |
| `HubSpotWebhookQueue` | Async buffer (`VisibilityTimeout` 250s, `maxReceiveCount` 5, partial batch failures) |
| `HubSpotWebhookDLQ` | Messages that exhaust retries (14-day retention); CloudWatch alarm when non-empty |
| `SyncLockTable` | Per-deal lock + `pending_resync` coalescing flag (TTL on `expiresAt`, default 960s) |
| `DependenciesLayer` | Shared Python dependencies |

Processor Lambda timeout: **200s**. SQS event source `MaximumConcurrency`: **2** (caps parallel NetSuite load).

Further reading: [RELIABILITY.md](RELIABILITY.md) (locks, retries, idempotency), [docs/failure-scenarios/](docs/failure-scenarios/) (per failure mode), [samconfig accounts/README.md](samconfig%20accounts/README.md) (deploy configs).

## Concurrency: lock + `pending_resync`

Events for the same parent deal (including line items) share one lock key. Only one processor run reconciles that deal at a time. Venue events use a separate `venue:{id}` key.

| Situation | Behaviour |
|-----------|-----------|
| Lock free | Acquire lock → reconcile → drain pending → release lock |
| Lock busy | Set `pending_resync = true` on the lock row → **ACK** message (no SQS retry) |
| After reconcile | Holder runs follow-up reconciles while `consume_pending_resync` finds pending activity |
| Lock release | Row is **deleted** from DynamoDB |

Log prefixes: `[coalesced]` (contention), `[resync]` (follow-up reconcile), `[rejected]` (permanent business skip), `[retry]` (transient error → SQS redrive), `[timing]` (reconcile duration breakdown), `[lines]` (line-item filter summary).

## SQS message outcomes

The processor returns `batchItemFailures` (`ReportBatchItemFailures`). Each message ends in one of:

| Outcome | Meaning | SQS |
|---------|---------|-----|
| Returns `True` | Synced, intentional skip, or coalesced | Deleted (ACK) |
| Returns `False` | Permanent business rejection (no contact, wrong stage, NetSuite 400 validation, …) | Deleted; logged `[rejected]`; reason on deal `netsuite_invoice_status` |
| Raises exception | Transient error (NetSuite 5xx, network, HubSpot after in-process retries) | Redriven; after 5 receives → **DLQ** |

**NetSuite 400** responses (e.g. invalid `salesrep`, bad field value) are treated as **permanent** rejections: the deal is stamped with `netsuite_invoice_status`, the message is ACKed, and it does **not** retry to the DLQ.

HubSpot venue search **400** → `DealInvoiceRejected` (same permanent path). Credential errors (e.g. HubSpot 401) still raise and follow the retry/DLQ path.

## Client accounts (reliability stacks)

| Account | Deploy config | Stack | Webhook (reliability) | AWS secret | Subsidiary |
|---------|---------------|-------|------------------------|------------|------------|
| Best Impressions | `samconfig accounts/bestimpressions.toml` | `hs-netsuite-sandbox-bestimpressions-reliability` | `https://9pofareyp2.execute-api.us-east-1.amazonaws.com/hubspot/webhook` | `hs-netsuite/sandbox/bestimpressions` | `2` |
| Duvall | `samconfig accounts/duvall.toml` | `hs-netsuite-sandbox-duvall-reliability` | `https://1zwhwpcpgh.execute-api.us-east-1.amazonaws.com/hubspot/webhook` | `hs-netsuite/sandbox/duvall` | `4` |
| Rocky Top | `samconfig accounts/rockytop.toml` | `hs-netsuite-sandbox-rockytop-reliability` | `https://7l5bdrcx96.execute-api.us-east-1.amazonaws.com/hubspot/webhook` | `hs-netsuite/sandbox/rockytop` | `5` |

Each reliability stack has its own API Gateway URL, SQS queue, DLQ, and lock table. Secrets Manager credentials are shared with the primary sandbox account secret for that portal.

Point HubSpot webhooks at the reliability URL only while validating. Switch back to the primary sandbox webhook when reliability testing is complete.

## Deploy parameters

Non-secret values are passed via `samconfig accounts/<account>.toml` → CloudFormation parameters → Lambda env vars.

| SAM parameter | Lambda env var | Purpose |
|---------------|----------------|---------|
| `SecretName` | `ACCOUNT_SECRET_NAME` | Secrets Manager id for HubSpot + NetSuite credentials |
| `HubSpotDealStageCreateId` | `HUBSPOT_DEAL_STAGE_CREATE_ID` | Deal stage(s) allowed to **create** a NetSuite invoice |
| `HubSpotDealStageUpdateId` | `HUBSPOT_DEAL_STAGE_UPDATE_ID` | Deal stage(s) allowed to **update** an existing invoice only |
| `HubSpotObjectTypeVenue` | `HUBSPOT_OBJECT_TYPE_VENUE` | HubSpot custom object type id for venues |
| `HubSpotObjectTypePayment` | `HUBSPOT_OBJECT_TYPE_PAYMENT` | Payment object type id (webhooks acked only) |
| `HubSpotObjectTypeLineItem` | `HUBSPOT_OBJECT_TYPE_LINE_ITEM` | Line item object type id |
| `DealVenueNameProperty` | `DEAL_VENUE_NAME_PROPERTY` | **Deal** property that holds the venue name text to look up |
| `HubSpotVenueNameSearchProperties` | `HUBSPOT_VENUE_NAME_SEARCH_PROPERTIES` | **Venue** property internal names used in CRM search filters (comma-separated) |
| `NetSuiteAccountId` | `NETSUITE_ACCOUNT_ID` | NetSuite account id |
| `NetSuiteSubsidiaryId` | `NETSUITE_SUBSIDIARY_ID` | Subsidiary for customers / invoices / locations |
| `ProcessorMaxConcurrency` | (SQS scaling) | Max concurrent processor Lambdas (default 2) |
| `ApiCallDelaySeconds` | `API_CALL_DELAY_SECONDS` | Optional sleep between API calls (default `"0"`) |
| `WebhookObjectIdFilterEnabled` | `WEBHOOK_OBJECT_ID_FILTER_ENABLED` | When true, only listed object ids are processed |
| `WebhookObjectIdFilterValue` | `WEBHOOK_OBJECT_ID_FILTER_VALUE` | Comma-separated allowlist of HubSpot `objectId` values |

Stage and venue parameters accept comma-separated lists where noted. All reliability deploys currently set `WebhookObjectIdFilterEnabled=false`.

### Venue configuration (two different objects)

Invoice sync needs the venue name from the **deal**, then finds the **venue custom object** in HubSpot:

1. **`DealVenueNameProperty`** — read venue name **from the deal** (e.g. `venu_name_sync` on Best Impressions).
2. **`HubSpotVenueNameSearchProperties`** — filter the **venue object** in CRM Search (e.g. `name`). HubSpot returns `400` if a filter references a property that does not exist on that portal's venue schema.

| Account | `HubSpotObjectTypeVenue` | `DealVenueNameProperty` (on deal) | `HubSpotVenueNameSearchProperties` (on venue) |
|---------|--------------------------|-----------------------------------|-----------------------------------------------|
| Duvall | `2-47163024` | `venue_name_` | `name` |
| Best Impressions | `2-47363405` | `venu_name_sync` | `name` |
| Rocky Top | `2-52267420` | `venue_name_` | `name` |

## Credentials

### Deployed (AWS)

Each account stores credentials in **AWS Secrets Manager**. The processor receives only the secret **name** as `ACCOUNT_SECRET_NAME`. At runtime, [`config.py`](lambda_functions/hubspot_processor/config.py) calls `GetSecretValue` and caches the JSON for the lifetime of a warm Lambda container.

| Secret JSON key | Used by |
|-----------------|---------|
| `hubspot_api_key` | `HubSpotClient` |
| `netsuite_client_id` | `NetSuiteAuth` (OAuth JWT) |
| `netsuite_cert_id` | `NetSuiteAuth` (OAuth JWT `kid`) |
| `netsuite_cert_string` | `NetSuiteAuth` (EC P-256 private key PEM) |
| `hubspot_client_secret` | Webhook signature verification (optional; see `HubSpotSignatureVerificationEnabled`) |

All three sandbox accounts share the same NetSuite OAuth integration; only the HubSpot token differs per secret.

**Secret JSON shape:**

```json
{
  "hubspot_api_key": "pat-na1-...",
  "netsuite_client_id": "...",
  "netsuite_cert_id": "...",
  "netsuite_cert_string": "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
}
```

**Update a secret** (example — Best Impressions):

```bash
aws secretsmanager put-secret-value \
  --secret-id "hs-netsuite/sandbox/bestimpressions" \
  --secret-string file://path/to/secret.json \
  --profile dev-cetdigit --region us-east-1
```

Credential rotation takes effect on the **next cold start**. A code redeploy is **not** required for secret-only changes.

### Local development

| Purpose | Path |
|---------|------|
| Active local config | `.env` (copy from `.env.<account>`) |
| Per-account reference | `.env.duvall`, `.env.bestimpressions`, `.env.rockytop` |
| NetSuite private key (P-256) | `certificates/private.pem` |
| Reference ids (not auto-loaded) | `secrets/netsuite-sandbox.json`, `secrets/hubspot-accounts.json` |

`generate_token.py` reads `.env` and auto-loads `certificates/private.pem` when `NETSUITE_CERT_STRING` is unset.

Do **not** set `ACCOUNT_SECRET_NAME` locally unless you intend to call Secrets Manager from your machine.

## Prerequisites

- AWS SAM CLI, AWS CLI (profile `dev-cetdigit` with deploy + `secretsmanager:GetSecretValue`)
- Python 3.14 (matches `template.yaml` runtime)
- HubSpot private app token (CRM scopes)
- NetSuite REST integration (OAuth 2.0 client credentials + EC P-256 certificate)

## Deploy

From the `sandbox/` directory:

```bash
sam validate --lint
sam build  --config-file "samconfig accounts/duvall.toml"
sam deploy --config-file "samconfig accounts/duvall.toml"
```

Replace `duvall` with `bestimpressions` or `rockytop`. Add `--no-confirm-changeset` to skip the CloudFormation prompt.

Deploy all three accounts after a shared code change:

```bash
sam build --no-cached
sam deploy --config-file "samconfig accounts/bestimpressions.toml" --no-confirm-changeset
sam deploy --config-file "samconfig accounts/duvall.toml" --no-confirm-changeset
sam deploy --config-file "samconfig accounts/rockytop.toml" --no-confirm-changeset
```

Each stack exposes a `WebhookUrl` output — point HubSpot webhooks at the URL for the stack you are testing.

`sam build` packages only `lambda_functions/` and `lambda_layers/`; credentials, tests, and local tooling are excluded via `.samignore`.

## Local verification

```bash
pip install -r lambda_layers/netsuite_dependencies/requirements.txt
python generate_token.py
```

Webhook smoke test (use `WebhookUrl` from stack outputs):

```powershell
Invoke-WebRequest -Uri "REPLACE_WITH_WebhookUrl" -Method POST -ContentType "application/json" `
  -Body '{"subscriptionType":"deal.propertyChange","objectId":"123","propertyName":"dealstage","propertyValue":"1233582150"}'
```

Expected response: `204`.

Unit tests (80 tests, no live AWS / API calls):

```bash
pip install pytest cryptography pyjwt requests boto3 python-dotenv
python -m pytest tests/ -q
```

## Business rules

### Deal → invoice

Invoice sync is gated by **deal stage** and the HubSpot property `prior_netsuite_invoice`. Both must pass before any NetSuite write runs.

#### Prerequisites (all stages)

Every enabled stage — create and update — requires:

- `prior_netsuite_invoice = no` (if `yes`, blank, or missing, sync is skipped and the reason is written to `netsuite_invoice_status` on the deal)

#### Stage modes

| Mode | Env var | Behaviour |
|------|---------|-----------|
| **Create** | `HUBSPOT_DEAL_STAGE_CREATE_ID` | May **create** a new invoice or **re-sync** an existing one for the deal |
| **Update** | `HUBSPOT_DEAL_STAGE_UPDATE_ID` | May **update** an invoice only when one already exists in NetSuite; never creates |

Evaluation order in the processor:

1. Deal stage is listed in create or update config
2. `prior_netsuite_invoice = no`
3. If update stage → NetSuite invoice must already exist for the deal
4. Remaining business rules (contact, customer, subsidiary, venue, line items)

Stages not listed in either variable are ignored.

#### Per-account stage configuration

Configured in each account's `samconfig accounts/<account>.toml`:

| Account | Create (`HubSpotDealStageCreateId`) | Update (`HubSpotDealStageUpdateId`) |
|---------|-------------------------------------|-------------------------------------|
| Best Impressions | `1059843169` | `1059843170`, `1354090846`, `1059843158`, `1083991396`, `1083978271`, `1083978272`, `1316012841` |
| Duvall | `1233582149` | `1233582150`, `1359365640`, `1233582151`, `1233582152`, `1233582153`, `1233582154`, `1359365641`, `1316739225` |
| Rocky Top | `1208741442` | `1343333815`, `1208741443`, `1359370264`, `1208741444`, `1208741445`, `1208741446`, `1208741447`, `1346864770` |

To add or change stages, edit the comma-separated values in the account's `parameter_overrides` and redeploy.

#### Invoice reconcile flow

When a deal passes the stage gate, `reconcile_deal_invoice` runs:

1. **Customer** — billing contact (association label `Billing`, else first associated contact) → NetSuite customer lookup by contact **email**
2. **Subsidiary** — customer must be assigned to deploy subsidiary (`NETSUITE_SUBSIDIARY_ID`)
3. **Sales rep** — HubSpot **deal owner** (`hubspot_owner_id`) → owner email → NetSuite employee → invoice `salesrep` (not the associated contact)
4. **Venue** — deal property `DEAL_VENUE_NAME_PROPERTY` → HubSpot venue search → NetSuite `location` (get or create)
5. **Line items** — fetch all deal lines, filter, batch SKU lookup, upsert invoice with one row per eligible line
6. **Write-back** — `netsuite_invoice_number`, `netsuite_invoice_status`, `netsuite_invoice_last_modified_date` on the deal

If NetSuite rejects the invoice with **400**, the error detail is written to `netsuite_invoice_status` and the message is not retried.

#### Observability (CloudWatch)

| Log prefix | When |
|------------|------|
| `[deal] - Reconcile started` | Reconcile begins |
| `[lines] - Fetching, analyzing and validating line items` | Before line-item fetch + filter |
| `[lines] - Filter summary` | After filters: `total`, `mapped`, `skipped`, `not_found`, `skip_reasons`, `missing_skus` |
| `[timing] reconcile` | End of reconcile: per-step seconds + total |
| `[deal] - Reconcile complete` | Success with invoice number and line count |
| `[rejected]` | Permanent skip (lambda_handler) |
| `[retry]` | Transient failure → SQS redrive |

### Line items

The processor loads **all** deal line items efficiently:

1. One HubSpot associations call (deal → line items)
2. One or more `batch/read` calls (100 ids per request)
3. In-memory eligibility filters
4. One NetSuite SuiteQL batch lookup (`itemid IN (...)`)
5. One invoice upsert with **one NetSuite row per eligible HubSpot line** (duplicate SKUs are kept as separate rows)

| Rule | Action |
|------|--------|
| `subcategory = Owned Equipment` and `amount <= 0` | Skipped |
| SKU missing or not starting with `3` | Skipped |
| SKU passes filters but not found in NetSuite | Omitted from invoice; counted in `not_found` / `missing_skus` in filter summary |
| SKU passes filters and found | One NetSuite invoice line per eligible HubSpot line |

Line-item webhooks resolve the parent deal via HubSpot **v4 associations** (`GET /crm/v4/objects/line_items/{id}/associations/deals`). If no parent deal is found, the event is skipped.

When a line-item webhook fires and a NetSuite invoice already exists, only invoice lines are updated (`PATCH` with `replace=item`).

### Payments

Payment sync is **disabled**. Payment webhooks are acknowledged immediately with `[payment] sync disabled` and are not serialized under a deal lock.

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| Deploy fails on secret resolve | Missing secret, wrong `SecretName`, or no `GetSecretValue` permission |
| `400 Bad Request` on NetSuite `/oauth2/v1/token` | Wrong or malformed `netsuite_cert_string` in secret; verify with `generate_token.py` locally |
| OAuth works locally but fails in Lambda | Secret out of date, or warm container still on old cached secret — wait for cold start after `put-secret-value` |
| `[coalesced] lock busy` in logs | Normal under burst traffic; holder will follow-up reconcile if needed |
| Messages in DLQ | Persistent exception after 5 receives — inspect message body and `[retry]` logs |
| `[rejected]` in logs | Permanent business rule failure — check `netsuite_invoice_status` on the deal |
| `Not created: Invalid Field Value ... salesrep` | Deal owner maps to a NetSuite employee that is not a valid sales rep for the subsidiary; fix owner or employee in HubSpot/NetSuite |
| `No parent deal resolved for objectId=...` | Line-item webhook with no deal association (v4 API returned none) |
| HubSpot venue search `400` | Wrong `HubSpotVenueNameSearchProperties` or `HubSpotObjectTypeVenue` for that portal |
| `Invalid Field Value ... location` | Subsidiary / location mismatch in NetSuite |
| Invoice sync skipped | Deal stage not in create/update config, `prior_netsuite_invoice != no`, or update stage with no existing NetSuite invoice |
| OAuth fails locally (`secp521r1`) | Private key must be P-256 (`secp256r1`) for ES256 |
| Invoice created with fewer lines than HubSpot | Some SKUs failed filters or were not found in NetSuite — check `[lines] Filter summary` |

## Security

- Do not commit `.env*`, `secrets/`, `certificates/`, or PEM files.
- Do not put secret values in `samconfig` or `template.yaml`.
- Rotate any credential that was ever exposed in a remote or shared channel.
- HubSpot webhook signature verification is off by default; enable via `HubSpotSignatureVerificationEnabled=true` when `hubspot_client_secret` is in the secret.

## Repository layout

```text
template.yaml              SAM infrastructure
lambda_functions/
  hubspot_webhook/         API Gateway → SQS enqueue
  hubspot_processor/       SQS consumer, reconcile, locks
lambda_layers/             Shared Python dependencies
samconfig accounts/        Per-account SAM deploy configs (see README there)
tests/                     Unit tests (80 tests; no live AWS / API calls)
docs/failure-scenarios/    Failure-mode design notes
RELIABILITY.md             Lock, retry, and idempotency register
```
