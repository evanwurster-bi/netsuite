# HubSpot to NetSuite Integration (AWS SAM)

Production-grade serverless integration that receives HubSpot webhooks, queues them in SQS, and synchronizes deals, venues, payments, and line items into NetSuite.

## What this project does

- Receives HubSpot webhook events through API Gateway.
- Validates and enqueues payloads in SQS.
- Processes events asynchronously in a Lambda consumer.
- Creates or updates NetSuite records via SuiteTalk REST.
- Pushes NetSuite venue identifiers back to HubSpot when needed.

## High-level architecture

HubSpot -> API Gateway (`WebhookFunction`) -> SQS (`HubSpotWebhookQueue`) -> Processor Lambda (`ProcessorFunction`) -> NetSuite + HubSpot APIs

Main resources:

- `WebhookFunction`: webhook intake and SQS publish.
- `ProcessorFunction`: business rules and sync logic.
- `HubSpotWebhookQueue` and `HubSpotWebhookDLQ`: decoupling and failure isolation.
- `DependenciesLayer`: shared Python dependencies.

## Business rules implemented

- Deal invoice sync only runs when:
  - `prior_netsuite_invoice = no` (if `yes`, blank, or missing, the invoice is not created)
  - deal stage is included in `HUBSPOT_DEAL_STAGE_SYNC_ID`
- Billing contact priority:
  - uses HubSpot association label `Billing`
  - falls back to first associated contact
- Customer resolution:
  - NetSuite customer is searched by contact email
- Subsidiary resolution:
  - the invoice subsidiary is always the deploy subsidiary (`NETSUITE_SUBSIDIARY_ID`)
  - the customer's shared subsidiaries are read from the `customerSubsidiaryRelationship` record via SuiteQL (Multi-Subsidiary Customer feature)
  - if `NETSUITE_SUBSIDIARY_ID` is not among the customer's shared subsidiaries, the deal is rejected (no invoice is created)
  - this keeps the invoice and the venue/location aligned on the same subsidiary
- Venue resolution:
  - deal venue name comes from `DEAL_VENUE_NAME_PROPERTY`
  - venue is resolved/created as NetSuite `location`
- Line items:
  - SKU lookup is done with `hs_sku` -> NetSuite `itemid`
  - SKU must start with `3`
  - `Owned Equipment` with `amount <= 0` is skipped
  - `Owned Equipment` with `amount > 0` is included

## Prerequisites

- AWS SAM CLI
- Python compatible with template runtime (`python3.14`)
- AWS profile with deployment permissions
- HubSpot private app token with required CRM scopes
- NetSuite REST integration with OAuth 2.0 client credentials

## Required configuration

Set these values in `.env` (local) or SAM `parameter_overrides` (AWS):

- `NETSUITE_ACCOUNT_ID`
- `NETSUITE_CLIENT_ID`
- `NETSUITE_CERT_ID`
- `NETSUITE_SUBSIDIARY_ID`
- `NETSUITE_CERT_STRING`
- `HUBSPOT_API_KEY`
- `HUBSPOT_DEAL_STAGE_SYNC_ID`
- `HUBSPOT_OBJECT_TYPE_VENUE`
- `HUBSPOT_OBJECT_TYPE_PAYMENT`
- `HUBSPOT_OBJECT_TYPE_LINE_ITEM`
- `DEAL_VENUE_NAME_PROPERTY`

Optional debug filter:

- `WEBHOOK_OBJECT_ID_FILTER_ENABLED`
- `WEBHOOK_OBJECT_ID_FILTER_VALUE`

## Build and deploy

Run from this folder (`sandbox/`).

Validation:

```bash
sam validate
sam validate --lint
```

Deploy by account:

```bash
sam build --config-file "samconfig accounts/samconfig.duvall.toml"
sam deploy --config-file "samconfig accounts/samconfig.duvall.toml"
```

```bash
sam build --config-file "samconfig accounts/samconfig.bestimpressions.toml"
sam deploy --config-file "samconfig accounts/samconfig.bestimpressions.toml"
```

```bash
sam build --config-file "samconfig accounts/samconfig.rockytop.toml"
sam deploy --config-file "samconfig accounts/samconfig.rockytop.toml"
```

## Local verification

1. Install dependencies:

```bash
pip install -r lambda_layers/netsuite_dependencies/requirements.txt
```

2. Validate NetSuite OAuth:

```bash
python generate_token.py
```

3. Smoke test webhook endpoint (`WebhookUrl` stack output):

```powershell
Invoke-WebRequest -Uri "REPLACE_WITH_WebhookUrl" -Method POST -ContentType "application/json" `
  -Body '{"subscriptionType":"deal.propertyChange","objectId":"123","propertyName":"dealstage","propertyValue":"1233582150"}'
```

Expected response: `204`.

## Common operational issues

- `Invalid Field Value ... location`
  - usually indicates subsidiary/location mismatch in NetSuite.
- `Record has been changed`
  - concurrent update conflict on an existing NetSuite invoice.
- `NetSuite customer not found for email`
  - HubSpot contact email has no matching NetSuite customer.
- `Customer ... is not assigned to subsidiary ... or lacks permissions`
  - the deploy subsidiary (`NETSUITE_SUBSIDIARY_ID`) is not shared with the customer; assign it on the customer's `customerSubsidiaryRelationship` records in NetSuite.
- Empty `HUBSPOT_DEAL_STAGE_SYNC_ID`
  - deal invoice sync is skipped by design.

## Security notes

- Never commit `.env`, private keys, tokens, or account credentials.
- Treat `samconfig` `parameter_overrides` as sensitive data.

## Repository map

- `template.yaml`: SAM template
- `samconfig accounts/`: per-account deploy config
- `lambda_functions/hubspot_webhook/`: webhook Lambda
- `lambda_functions/hubspot_processor/`: sync processor and integrations
- `lambda_layers/netsuite_dependencies/requirements.txt`: shared dependencies
- `scripts/encode_pem_for_sam.py`: helper for one-line PEM encoding
- `generate_token.py`: local OAuth token validation
