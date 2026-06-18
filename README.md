# HubSpot → NetSuite (Sandbox)

AWS SAM application that receives HubSpot webhooks, buffers them in SQS, and syncs deals, venues, payments, and line items into NetSuite.

One codebase is deployed per client account. **Application source and non-secret configuration are versioned in git.** All credentials live outside the repository (local files for development, AWS Secrets Manager for deployed Lambdas).

## Architecture

```text
HubSpot → API Gateway (WebhookFunction) → SQS → ProcessorFunction → NetSuite / HubSpot APIs
```

| Component | Role |
|-----------|------|
| `WebhookFunction` | Accepts webhooks, enqueues payloads |
| `ProcessorFunction` | Business rules and NetSuite sync |
| `HubSpotWebhookQueue` / `HubSpotWebhookDLQ` | Async processing and failure isolation |
| `DependenciesLayer` | Shared Python dependencies |

## What is in git

```text
template.yaml
README.md
RELIABILITY.md
.env.example
docs/
tests/
lambda_functions/
lambda_layers/
samconfig accounts/
```

Everything else is local or generated at build time and is excluded via `.gitignore` and `.samignore`.

## Client accounts

| Account | Deploy config | AWS secret |
|---------|---------------|------------|
| Duvall | `samconfig accounts/sandbox/duvall.toml` | `hs-netsuite/sandbox/duvall` |
| Best Impressions | `samconfig accounts/sandbox/bestimpressions.toml` | `hs-netsuite/sandbox/bestimpressions` |
| Rocky Top | `samconfig accounts/sandbox/rockytop.toml` | `hs-netsuite/sandbox/rockytop` |

Reliability / E2E stacks use the same template under `samconfig accounts/reliability/`. See [samconfig accounts/README.md](samconfig%20accounts/README.md).

## Credentials

### Deployed (AWS)

Each account has a secret in **AWS Secrets Manager**. `samconfig` passes only `SecretName`; `template.yaml` resolves values at deploy time:

| Secret key | Lambda env var |
|------------|----------------|
| `hubspot_api_key` | `HUBSPOT_API_KEY` |
| `netsuite_client_id` | `NETSUITE_CLIENT_ID` |
| `netsuite_cert_id` | `NETSUITE_CERT_ID` |
| `netsuite_cert_string` | `NETSUITE_CERT_STRING` |

All three sandbox accounts share the same NetSuite OAuth integration; only the HubSpot token differs per secret.

Update a secret (example — Duvall):

```bash
aws secretsmanager put-secret-value \
  --secret-id "hs-netsuite/sandbox/duvall" \
  --secret-string file://path/to/secret.json \
  --profile dev-cetdigit --region us-east-1
```

Secret JSON shape:

```json
{
  "hubspot_api_key": "pat-na1-...",
  "netsuite_client_id": "...",
  "netsuite_cert_id": "...",
  "netsuite_cert_string": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
}
```

After rotating credentials, redeploy or wait for new Lambda environments to pick up the updated secret.

### Local development

Credentials are read from fixed local paths (never committed):

| Purpose | Path |
|---------|------|
| NetSuite OAuth (shared) | `secrets/netsuite-sandbox.json` |
| NetSuite private key (P-256) | `certificates/private.pem` |
| HubSpot tokens (per account) | `secrets/hubspot-accounts.json` |
| HubSpot deploy settings | `.env.<account>` (object types, subsidiary, filters) |
| Default account for CLI | `.env` → `SANDBOX_ACCOUNT=duvall` |

`secrets/netsuite-sandbox.json`:

```json
{
  "account_id": "3763009-sb1",
  "client_id": "...",
  "cert_id": "..."
}
```

`secrets/hubspot-accounts.json`:

```json
{
  "duvall": { "hubspot_api_key": "pat-na1-..." },
  "bestimpressions": { "hubspot_api_key": "pat-na1-..." },
  "rockytop": { "hubspot_api_key": "pat-na1-..." }
}
```

Non-secret deploy parameters (subsidiary, HubSpot object type IDs, stage filters) live in `samconfig accounts/sandbox/*.toml`.

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

Use `bestimpressions` or `rockytop` in place of `duvall` for the other accounts.

`sam build` packages only `lambda_functions/` and `lambda_layers/`; credentials, tests, and local tooling are excluded via `.samignore`.

## Local verification

```bash
pip install -r lambda_layers/netsuite_dependencies/requirements.txt

python generate_token.py
python generate_token.py --account rockytop
```

Webhook smoke test (`WebhookUrl` stack output):

```powershell
Invoke-WebRequest -Uri "REPLACE_WITH_WebhookUrl" -Method POST -ContentType "application/json" `
  -Body '{"subscriptionType":"deal.propertyChange","objectId":"123","propertyName":"dealstage","propertyValue":"1233582150"}'
```

Expected: `204`.

## Business rules

- Invoice sync when `prior_netsuite_invoice = no` and deal stage is in `HUBSPOT_DEAL_STAGE_SYNC_ID`.
- Billing contact: HubSpot association label `Billing`, else first associated contact.
- Customer: NetSuite lookup by contact email.
- Subsidiary: always `NETSUITE_SUBSIDIARY_ID` from deploy config; customer must share that subsidiary.
- Venue: from `DEAL_VENUE_NAME_PROPERTY` → NetSuite `location`.
- Line items: `hs_sku` → NetSuite `itemid`; SKU must start with `3`; skip `Owned Equipment` with `amount <= 0`.

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| Deploy fails on secret resolve | Missing secret, wrong `SecretName`, or no `GetSecretValue` permission |
| `Invalid Field Value ... location` | Subsidiary / location mismatch in NetSuite |
| `Record has been changed` | Concurrent invoice update |
| `NetSuite customer not found for email` | No matching NetSuite customer |
| Customer not assigned to subsidiary | Subsidiary not on customer record |
| Invoice sync skipped | Empty `HUBSPOT_DEAL_STAGE_SYNC_ID` (by design) |
| OAuth fails locally (`secp521r1`) | Private key must be P-256 for ES256 |

## Security

- Do not commit `.env*`, `secrets/`, `certificates/`, or PEM files.
- Do not put secret values in `samconfig` or `template.yaml`.
- Rotate any credential that was ever exposed in a remote or shared channel.
