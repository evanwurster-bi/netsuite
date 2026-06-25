# HubSpot → NetSuite (Sandbox)

AWS SAM application that receives HubSpot webhooks, buffers them in SQS, and syncs deals, venues, and line items into NetSuite.

One codebase deploys per client account. Application source and non-secret configuration live in git. Credentials never enter the repository — local files for development, AWS Secrets Manager for deployed Lambdas.

## Architecture

```text
HubSpot → API Gateway (WebhookFunction) → SQS → ProcessorFunction → NetSuite / HubSpot APIs
                                              ↓
                                         DLQ + CloudWatch alarm
```

| Component | Role |
|-----------|------|
| `WebhookFunction` | Accepts webhooks, enqueues one SQS message per event |
| `ProcessorFunction` | Business rules, per-deal locking, NetSuite / HubSpot sync |
| `HubSpotWebhookQueue` | Async processing (`VisibilityTimeout` 960s, `maxReceiveCount` 5) |
| `HubSpotWebhookDLQ` | Holds messages that exhaust retries (14-day retention) |
| `SyncLockTable` | DynamoDB per-deal lock — serializes concurrent events for the same invoice |
| `DependenciesLayer` | Shared Python dependencies |

Reliability behaviour (locks, retries, idempotency) is documented in [RELIABILITY.md](RELIABILITY.md).

## Client accounts

| Account | Deploy config | AWS secret |
|---------|---------------|------------|
| Duvall | `samconfig accounts/sandbox/duvall.toml` | `hs-netsuite/sandbox/duvall` |
| Best Impressions | `samconfig accounts/sandbox/bestimpressions.toml` | `hs-netsuite/sandbox/bestimpressions` |
| Rocky Top | `samconfig accounts/sandbox/rockytop.toml` | `hs-netsuite/sandbox/rockytop` |

Parallel reliability / E2E stacks use the same template under `samconfig accounts/reliability/`. See [samconfig accounts/README.md](samconfig%20accounts/README.md).

## Credentials

### Deployed (AWS)

Each account stores credentials in **AWS Secrets Manager**. The processor receives only the secret **name** as an environment variable (`ACCOUNT_SECRET_NAME`). At runtime, [`config.py`](lambda_functions/hubspot_processor/config.py) calls `GetSecretValue` and caches the JSON for the lifetime of a warm Lambda container.

| Secret JSON key | Used by |
|-----------------|---------|
| `hubspot_api_key` | `HubSpotClient` |
| `netsuite_client_id` | `NetSuiteAuth` (OAuth JWT) |
| `netsuite_cert_id` | `NetSuiteAuth` (OAuth JWT `kid`) |
| `netsuite_cert_string` | `NetSuiteAuth` (EC P-256 private key PEM) |

Non-secret deploy parameters (NetSuite account id, subsidiary, HubSpot object type IDs, deal stage filters) are passed via `samconfig accounts/sandbox/*.toml` → CloudFormation parameters → Lambda env vars.

| SAM parameter | Lambda env var | Purpose |
|---------------|----------------|---------|
| `HubSpotDealStageCreateId` | `HUBSPOT_DEAL_STAGE_CREATE_ID` | Deal stage(s) allowed to **create** a NetSuite invoice |
| `HubSpotDealStageUpdateId` | `HUBSPOT_DEAL_STAGE_UPDATE_ID` | Deal stage(s) allowed to **update** an existing invoice only |

Both accept comma-separated HubSpot internal stage IDs.

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

Credential rotation takes effect on the **next cold start** (or new Lambda execution environment). A code redeploy is **not** required for secret-only changes.

### Local development

| Purpose | Path |
|---------|------|
| Active local config | `.env` (copy from `.env.<account>`) |
| NetSuite private key (P-256) | `certificates/private.pem` |
| Reference ids (not auto-loaded) | `secrets/netsuite-sandbox.json` |

`generate_token.py` reads `.env` and auto-loads `certificates/private.pem` when `NETSUITE_CERT_STRING` is unset. Set `HUBSPOT_API_KEY`, `NETSUITE_CLIENT_ID`, `NETSUITE_CERT_ID`, and `NETSUITE_ACCOUNT_ID` in `.env` for local runs.

Do **not** set `ACCOUNT_SECRET_NAME` locally unless you intend to call Secrets Manager from your machine.

## Prerequisites

- AWS SAM CLI, AWS CLI (profile with deploy + `secretsmanager:GetSecretValue`)
- Python 3.14 (matches `template.yaml` runtime)
- HubSpot private app token (CRM scopes)
- NetSuite REST integration (OAuth 2.0 client credentials + EC P-256 certificate)

## Deploy

From the `sandbox/` directory:

```bash
sam validate
sam validate --lint

sam build  --config-file "samconfig accounts/sandbox/duvall.toml"
sam deploy --config-file "samconfig accounts/sandbox/duvall.toml"
```

Replace `duvall` with `bestimpressions` or `rockytop` for other accounts. For reliability stacks, use `samconfig accounts/reliability/<account>.toml`.

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

Unit tests:

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

Configured in each account's `samconfig accounts/sandbox/<account>.toml` (and mirrored under `reliability/`):

| Account | Create (`HubSpotDealStageCreateId`) | Update (`HubSpotDealStageUpdateId`) |
|---------|-------------------------------------|-------------------------------------|
| Best Impressions | `1059843169` | `1059843170`, `1354090846`, `1059843158`, `1083991396`, `1083978271`, `1083978272`, `1316012841` |
| Duvall | `1233582149` | `1233582150`, `1359365640`, `1233582151`, `1233582152`, `1233582153`, `1233582154`, `1359365641`, `1316739225` |
| Rocky Top | `1208741442` | `1343333815`, `1208741443`, `1359370264`, `1208741444`, `1208741445`, `1208741446`, `1208741447`, `1346864770` |

To add or change stages, edit the comma-separated values in the account's `parameter_overrides` and redeploy. No code change is required.

#### Additional invoice rules

- Billing contact: HubSpot association label `Billing`, else first associated contact.
- Customer: NetSuite lookup by contact email; must belong to deploy subsidiary (`NETSUITE_SUBSIDIARY_ID`).
- Venue: resolved from `DEAL_VENUE_NAME_PROPERTY` → NetSuite `location`.

### Line items

Deal line items are loaded with **one associations call + one HubSpot batch read** (`get_line_item_details_batch`), **consolidated by SKU** (quantities and amounts summed, rate recalculated), then validated and resolved with **one SuiteQL batch lookup** per deal ([`process_deal_lineitems_change`](lambda_functions/hubspot_processor/sqs_processor.py)):

| Rule | Action |
|------|--------|
| `subcategory = Owned Equipment` and `amount <= 0` | Skipped |
| SKU missing or not starting with `3` | Skipped |
| SKU passes filters but not found in NetSuite | Logged as error, skipped for invoice |
| SKU passes filters and found | One consolidated invoice line per SKU |

CloudWatch logs include consolidation stats, a filter summary (`total`, `eligible`, `skipped`, `by_reason`), and per-line skip reasons under the `[lines]` prefix.

### Payments

Payment sync is **disabled**. Payment webhooks are acknowledged immediately with `[payment] sync disabled` and are not serialized under a deal lock.

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| Deploy fails on secret resolve | Missing secret, wrong `SecretName`, or no `GetSecretValue` permission |
| `400 Bad Request` on NetSuite `/oauth2/v1/token` | Wrong or malformed `netsuite_cert_string` in secret; verify with `generate_token.py` locally |
| OAuth works locally but fails in Lambda | Secret out of date, or warm container still on old cached secret — wait for cold start after `put-secret-value` |
| `LockNotAcquired` in logs | Expected under concurrency; SQS redrives the message |
| Messages in DLQ | Persistent failure after 5 receives — inspect message body and processor logs |
| `Invalid Field Value ... location` | Subsidiary / location mismatch in NetSuite |
| `Record has been changed` | Concurrent invoice update (lock should prevent this in normal operation) |
| Invoice sync skipped | Deal stage not in create/update config, `prior_netsuite_invoice != no`, or update stage with no existing NetSuite invoice |
| OAuth fails locally (`secp521r1`) | Private key must be P-256 (`secp256r1`) for ES256 |

## Security

- Do not commit `.env*`, `secrets/`, `certificates/`, or PEM files.
- Do not put secret values in `samconfig` or `template.yaml`.
- Rotate any credential that was ever exposed in a remote or shared channel.

## Repository layout

```text
template.yaml              SAM infrastructure
lambda_functions/          Webhook + processor handlers
lambda_layers/             Shared Python dependencies
samconfig accounts/        Per-account deploy parameters
tests/                     Unit tests (no live AWS / API calls)
docs/failure-scenarios/    Failure-mode design notes
RELIABILITY.md             Lock, retry, and idempotency register
```
