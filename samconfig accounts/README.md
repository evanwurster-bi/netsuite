# SAM deploy configs (sandbox)

One TOML file per client account. All configs share the same `template.yaml` and the same **build/deploy** settings; only stack naming and `parameter_overrides` differ per account.

**Scope:** these files target the **sandbox reliability** stacks (`hs-netsuite-sandbox-{account}-reliability`). Do not use them for production.

## Layout

| File | Stack | AWS secret |
|------|-------|------------|
| `bestimpressions.toml` | `hs-netsuite-sandbox-bestimpressions-reliability` | `hs-netsuite/sandbox/bestimpressions` |
| `duvall.toml` | `hs-netsuite-sandbox-duvall-reliability` | `hs-netsuite/sandbox/duvall` |
| `rockytop.toml` | `hs-netsuite-sandbox-rockytop-reliability` | `hs-netsuite/sandbox/rockytop` |

Each reliability stack has its own API Gateway webhook URL, SQS queue, DLQ, and DynamoDB lock table. Secrets Manager credentials are **shared** with the matching sandbox account secret (same HubSpot portal + NetSuite sandbox).

Point HubSpot webhooks at the reliability `WebhookUrl` only while validating; switch back to the primary sandbox stack webhook when done.

## Shared settings (identical in every TOML)

These must stay in sync across `bestimpressions.toml`, `duvall.toml`, and `rockytop.toml`:

| Section | Keys | Value |
|---------|------|-------|
| `[default.global.parameters]` | `region` | `us-east-1` |
| `[default.build.parameters]` | `cached` | `false` |
| `[default.build.parameters]` | `parallel` | `true` |
| `[default.deploy.parameters]` | `capabilities` | `CAPABILITY_IAM` |
| `[default.deploy.parameters]` | `confirm_changeset` | `true` |
| `[default.deploy.parameters]` | `resolve_s3` | `true` |
| `[default.deploy.parameters]` | `profile` | `dev-cetdigit` |
| `[default.deploy.parameters]` | `disable_rollback` | `true` |
| `[default.deploy.parameters]` | `image_repositories` | `[]` |

When changing build or deploy behavior (cache, profile, rollback, etc.), update **all three** files together.

## Per-account settings (only these differ)

| Key | Purpose |
|-----|---------|
| `stack_name` | CloudFormation stack id |
| `s3_prefix` | SAM artifact prefix (must match stack naming) |
| `parameter_overrides` | HubSpot object types, deal stages, venue property names, subsidiary, optional webhook filter |

See the main [README](../README.md#deploy-parameters) for the full parameter → env var mapping and per-account stage / venue values.

## Deploy

From the `sandbox/` repo root:

```bash
sam validate --lint
sam build  --config-file "samconfig accounts/duvall.toml"
sam deploy --config-file "samconfig accounts/duvall.toml"
```

Replace `duvall` with `bestimpressions` or `rockytop`. To skip the changeset prompt:

```bash
sam deploy --config-file "samconfig accounts/duvall.toml" --no-confirm-changeset
```

Deploy all three accounts after a shared code change:

```bash
sam build --no-cached
sam deploy --config-file "samconfig accounts/bestimpressions.toml" --no-confirm-changeset
sam deploy --config-file "samconfig accounts/duvall.toml" --no-confirm-changeset
sam deploy --config-file "samconfig accounts/rockytop.toml" --no-confirm-changeset
```

## Deal stage parameters

Each `parameter_overrides` string includes:

| SAM parameter | Lambda env var | Purpose |
|---------------|----------------|---------|
| `HubSpotDealStageCreateId` | `HUBSPOT_DEAL_STAGE_CREATE_ID` | Stage(s) that may **create** a NetSuite invoice |
| `HubSpotDealStageUpdateId` | `HUBSPOT_DEAL_STAGE_UPDATE_ID` | Stage(s) that may **update** an existing invoice only |

Both require `prior_netsuite_invoice = no` on the deal. See [README § Deal / invoice](../README.md#deal--invoice) for per-account stage IDs.
