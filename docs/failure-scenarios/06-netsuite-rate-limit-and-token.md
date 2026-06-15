# 06 — NetSuite rate-limit / token exhaustion

**Register risk:** 4 — Shared NetSuite rate/concurrency budget (Medium)
**Code:** [netsuite_auth.py](../../lambda_functions/hubspot_processor/netsuite_auth.py) · [template.yaml](../../template.yaml)

## The situation

All client accounts share NetSuite's per-account concurrency and rate budget. Under a burst of
webhooks, the integration can hammer NetSuite — both the **token endpoint** (a fresh OAuth
token per call) and the **REST API** (many parallel Lambdas) — and start getting throttled.

## Before — a token per request, no Retry-After, unbounded fan-out

```python
def make_request(self, ...):
    access_token = self.get_access_token()   # mints a JWT + token call EVERY request
    ...
    if response.status_code == 429:
        sleep_time = retry_delay * (2 ** (attempt - 1))   # ignores Retry-After
```
…and the SQS event source had **no concurrency cap**, so a spike fanned out to many
simultaneous processor invocations.

```mermaid
sequenceDiagram
    participant Processor
    participant TokenEndpoint as NetSuite token
    participant NetSuite
    loop every single API call
        Processor->>TokenEndpoint: mint new token
        TokenEndpoint-->>Processor: access_token
        Processor->>NetSuite: REST call
    end
    Note over Processor,NetSuite: many parallel workers, 2x calls each
    NetSuite-->>Processor: 429 Too Many Requests
    Note over Processor: backoff ignores Retry-After → repeated 429s ❌
```

### How it failed
- **Token endpoint pressure + latency**: minting a JWT and calling the token endpoint on every
  request doubled the call volume and slowed every operation.
- **Throttling storms**: ignoring `Retry-After` meant retrying too soon and re-triggering 429s.
- **No backpressure**: unbounded concurrency let a burst exceed the shared NetSuite budget,
  affecting *all* accounts.

## After — cached token, Retry-After, capped concurrency

```mermaid
sequenceDiagram
    participant SQS
    participant Processor
    participant TokenEndpoint as NetSuite token
    participant NetSuite
    Note over SQS,Processor: ScalingConfig.MaximumConcurrency caps parallel workers
    Processor->>TokenEndpoint: mint token (first call only)
    TokenEndpoint-->>Processor: access_token (cached until exp−60s)
    loop subsequent calls (warm)
        Processor->>NetSuite: REST call (reuses cached token)
    end
    alt 429 with Retry-After
        NetSuite-->>Processor: 429, Retry-After: N
        Note over Processor: sleep exactly N (clamped ≤60s), then retry ✓
    end
```

### How it's prevented
- **Token caching** — `get_access_token` caches the token on the (module-level, warm-reused)
  `NetSuiteAuth` instance until ~60s before expiry. Roughly halves NetSuite traffic and cuts
  latency.
- **Honor `Retry-After`** — `_parse_retry_after` reads the header (numeric seconds, clamped to
  ≤60s; HTTP-date falls back to exponential backoff), so retries wait exactly as long as
  NetSuite asks.
- **Concurrency cap** — `ProcessorMaxConcurrency` (default 2) wired to
  `ScalingConfig.MaximumConcurrency` limits how many processor Lambdas hit NetSuite at once,
  keeping the integration inside the shared budget.
- **Tunable padding** — `API_CALL_DELAY_SECONDS` controls the inter-call delay (set `0` to
  disable now that real backpressure exists).

### Residual notes
The `time.sleep(1)` calls after invoice/payment writes are **read-after-write consistency
waits** (NetSuite's `externalId` search is eventually consistent), not rate-limit padding —
they are intentionally retained. Capping concurrency at 2 also interacts with the per-deal
lock; see the contention note in [../../RELIABILITY.md](../../RELIABILITY.md).
