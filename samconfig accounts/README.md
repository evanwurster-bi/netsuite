# SAM deploy configs

Both folders use the **same** `template.yaml`. They differ only in **stack name** and **S3 prefix**, so you can run two parallel deployments per account:

| Folder | Purpose | Stack name pattern |
|--------|---------|-------------------|
| `sandbox/` | Day-to-day sandbox integration (existing stacks) | `hs-netsuite-sandbox-{account}` |
| `reliability/` | Temporary parallel stack for reliability / E2E validation (SQS, DLQ, locks) | `hs-netsuite-sandbox-{account}-reliability` |

Reliability stacks reuse the same Secrets Manager secret as their sandbox counterpart. Point HubSpot webhooks at the reliability `WebhookUrl` only while testing; switch back to the sandbox stack when done.

When validation is complete, deploy `sandbox/` with the updated template and delete the `-reliability` stacks.

## Deploy

From the `sandbox/` repo root:

```bash
# Regular sandbox stack
sam build  --config-file "samconfig accounts/sandbox/duvall.toml"
sam deploy --config-file "samconfig accounts/sandbox/duvall.toml"

# Parallel reliability test stack (same code, different stack name)
sam build  --config-file "samconfig accounts/reliability/duvall.toml"
sam deploy --config-file "samconfig accounts/reliability/duvall.toml"
```

Replace `duvall` with `bestimpressions` or `rockytop` for the other accounts.

## Deal stage parameters

Each account's `parameter_overrides` includes two stage lists (comma-separated HubSpot internal IDs):

| SAM parameter | Lambda env var | Purpose |
|---------------|----------------|---------|
| `HubSpotDealStageCreateId` | `HUBSPOT_DEAL_STAGE_CREATE_ID` | Stage(s) that may **create** a NetSuite invoice |
| `HubSpotDealStageUpdateId` | `HUBSPOT_DEAL_STAGE_UPDATE_ID` | Stage(s) that may **update** an existing invoice only |

Both require `prior_netsuite_invoice = no` on the deal. See the main [README](../README.md#deal--invoice) for per-account stage IDs.
