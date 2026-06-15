# HubSpot → NetSuite Integration (AWS SAM)

Serverless integration that receives HubSpot webhooks, queues them in SQS, and synchronizes deals, venues, payments, and line items into NetSuite.

The same codebase is deployed to multiple client accounts. Only application source and **non-secret** configuration are versioned. Credentials live in AWS Secrets Manager.

## Architecture

```text
HubSpot → API Gateway (WebhookFunction) → SQS (HubSpotWebhookQueue) → Processor Lambda (ProcessorFunction) → NetSuite + HubSpot APIs
```

| Resource | Responsibility |
|----------|----------------|
| `WebhookFunction` | Receives HubSpot webhooks and publishes to SQS |
| `ProcessorFunction` | Applies business rules and syncs to NetSuite |
| `HubSpotWebhookQueue` / `HubSpotWebhookDLQ` | Decoupling and failure isolation |
| `DependenciesLayer` | Shared Python dependencies |

## Repository structure

Only these paths are tracked in git:

```text
sandbox/
├── template.yaml              # SAM infrastructure + Lambda definitions
├── README.md
├── .env.example              # Template for local testing only
├── samconfig accounts/       # Per-account deploy config (non-secret)
│   ├── samconfig.duvall.toml
│   ├── samconfig.bestimpressions.toml
│   └── samconfig.rockytop.toml
├── lambda_functions/         # Webhook + processor source
│   ├── hubspot_webhook/
│   └── hubspot_processor/
└── lambda_layers/            # Shared dependency layer
    └── netsuite_dependencies/requirements.txt
```

Everything else (`.env*`, `.aws-sam/`, PEM/key files, local scripts) stays local and is ignored by git.

## Accounts

Each client maps to one SAM config and one secret:

| Account | SAM config | Secrets Manager secret |
|---------|------------|------------------------|
| Duvall | `samconfig.duvall.toml` | `hs-netsuite/sandbox/duvall` |
| Best Impressions | `samconfig.bestimpressions.toml` | `hs-netsuite/sandbox/bestimpressions` |
| Rocky Top | `samconfig.rockytop.toml` | `hs-netsuite/sandbox/rockytop` |

## Credentials

Credentials are **never** stored in the repository.

| Type | Stored in | In git? |
|------|-----------|---------|
| HubSpot token, NetSuite client id / cert id / private key | AWS Secrets Manager | No |
| Stack name, region, profile, object type ids, subsidiary, filters | `samconfig accounts/*.toml` | Yes (non-secret) |

### How it works

`samconfig` passes only a `SecretName` (plus non-secret config). `template.yaml` resolves the actual secret values from AWS Secrets Manager **at deploy time**:

```yaml
HUBSPOT_API_KEY:     !Sub "{{resolve:secretsmanager:${SecretName}:SecretString:hubspot_api_key}}"
NETSUITE_CLIENT_ID:  !Sub "{{resolve:secretsmanager:${SecretName}:SecretString:netsuite_client_id}}"
NETSUITE_CERT_ID:    !Sub "{{resolve:secretsmanager:${SecretName}:SecretString:netsuite_cert_id}}"
NETSUITE_CERT_STRING:!Sub "{{resolve:secretsmanager:${SecretName}:SecretString:netsuite_cert_string}}"
```

CloudFormation reads the secret using the deploying AWS profile, so the deploying user needs `secretsmanager:GetSecretValue`. No secret value ever touches git.

### Creating an account secret

Each secret is a JSON document with these keys:

```json
{
  "hubspot_api_key": "pat-na1-...",
  "netsuite_client_id": "...",
  "netsuite_cert_id": "...",
  "netsuite_cert_string": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
}
```

Create it once per account (example for Duvall):

```bash
aws secretsmanager create-secret \
  --name "hs-netsuite/sandbox/duvall" \
  --secret-string file://duvall-secret.json \
  --profile dev-cetdigit --region us-east-1
```

Update an existing secret:

```bash
aws secretsmanager put-secret-value \
  --secret-id "hs-netsuite/sandbox/duvall" \
  --secret-string file://duvall-secret.json \
  --profile dev-cetdigit --region us-east-1
```

> Keep the `*-secret.json` file local; it is ignored by git. Delete it after upload.

## Prerequisites

- AWS SAM CLI
- Python `3.14` (matches the template runtime)
- AWS CLI profile with deploy + `secretsmanager:GetSecretValue` permissions
- HubSpot private app token with required CRM scopes
- NetSuite REST integration (OAuth 2.0 client credentials)

## Deploy

Run from the `sandbox/` folder.

```bash
# Validate
sam validate
sam validate --lint

# Build and deploy per account
sam build  --config-file "samconfig accounts/samconfig.duvall.toml"
sam deploy --config-file "samconfig accounts/samconfig.duvall.toml"
```

Replace `duvall` with `bestimpressions` or `rockytop` for the other accounts.

## Local OAuth testing

```bash
pip install -r lambda_layers/netsuite_dependencies/requirements.txt
cp .env.example .env   # fill in values locally
python generate_token.py
```

Smoke test the webhook endpoint (`WebhookUrl` stack output):

```powershell
Invoke-WebRequest -Uri "REPLACE_WITH_WebhookUrl" -Method POST -ContentType "application/json" `
  -Body '{"subscriptionType":"deal.propertyChange","objectId":"123","propertyName":"dealstage","propertyValue":"1233582150"}'
```

Expected response: `204`.

## Business rules

- **Invoice sync** runs only when `prior_netsuite_invoice = no` and the deal stage is in `HUBSPOT_DEAL_STAGE_SYNC_ID`.
- **Billing contact**: HubSpot association label `Billing`, falling back to the first associated contact.
- **Customer**: resolved in NetSuite by contact email.
- **Subsidiary**: invoice subsidiary is always `NETSUITE_SUBSIDIARY_ID`; the customer's shared subsidiaries are read via SuiteQL (`customerSubsidiaryRelationship`). If the deploy subsidiary is not shared, the deal is rejected.
- **Venue**: name comes from `DEAL_VENUE_NAME_PROPERTY`; resolved/created as a NetSuite `location`.
- **Line items**: `hs_sku` → NetSuite `itemid`; SKU must start with `3`; `Owned Equipment` with `amount <= 0` is skipped.

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| `Invalid Field Value ... location` | Subsidiary/location mismatch in NetSuite |
| `Record has been changed` | Concurrent update on an existing invoice |
| `NetSuite customer not found for email` | HubSpot contact email has no matching NetSuite customer |
| `Customer ... is not assigned to subsidiary ...` | Deploy subsidiary not shared with the customer |
| Deploy fails resolving the secret | Secret missing, wrong `SecretName`, or missing `secretsmanager:GetSecretValue` |
| Invoice sync skipped | `HUBSPOT_DEAL_STAGE_SYNC_ID` empty (by design) |

## Security checklist

- [ ] No secrets in `samconfig accounts/*.toml` (only `SecretName` + non-secret config)
- [ ] No secret values in `template.yaml` (only `{{resolve:secretsmanager:...}}` references)
- [ ] `.env*`, PEM, and `*-secret.json` files never committed
- [ ] Rotate any credential that was ever pushed to a remote
