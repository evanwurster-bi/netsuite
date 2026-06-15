"""HTTP webhook handler: validates the payload and publishes to SQS.

Invoked by API Gateway HTTP API. ``SQS_QUEUE_URL`` is set by the SAM template. For local
execution, set ``SQS_QUEUE_URL`` to a valid queue URL.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Tuple

import boto3

logger = logging.getLogger()
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")

# SAM injects this. No placeholder fallback: a misconfigured deploy must fail loudly at
# request time (see lambda_handler) rather than silently publish to a non-existent queue.
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL", "")

# HubSpot v3 webhook signature verification. OFF by default so the integration keeps working
# until the app client secret is provisioned in Secrets Manager and verification is enabled.
HUBSPOT_SIGNATURE_VERIFICATION_ENABLED = (
    os.environ.get("HUBSPOT_SIGNATURE_VERIFICATION_ENABLED", "false").strip().lower() == "true"
)
HUBSPOT_CLIENT_SECRET = os.environ.get("HUBSPOT_CLIENT_SECRET", "")
SIGNATURE_MAX_AGE_MS = 5 * 60 * 1000  # reject requests older than 5 minutes (replay guard)


def _parse_events(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return every HubSpot event in the request body.

    HubSpot delivers webhooks as a JSON **array**: a single POST can batch many events.
    A lone JSON object is also accepted for manual tests / backwards compatibility. Each
    event is enqueued separately so it can be retried and de-duplicated on its own.
    """
    raw = event.get("body") or "[]"
    payload = raw if isinstance(raw, (list, dict)) else json.loads(raw)
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    raise ValueError("Webhook body must be a JSON object or array")


def _is_valid_event(body: Dict[str, Any]) -> bool:
    return bool(body.get("objectId") and body.get("subscriptionType"))


def _chunked(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _to_sqs_entry(index: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "Id": str(index),
        "MessageBody": json.dumps(body),
        "MessageAttributes": {
            "webhook_type": {
                "StringValue": str(body.get("subscriptionType", "unknown")),
                "DataType": "String",
            },
            "object_id": {
                "StringValue": str(body.get("objectId", "")),
                "DataType": "String",
            },
        },
    }


def _request_uri(event: Dict[str, Any]) -> str:
    """Reconstruct the full request URI that HubSpot signed: ``proto://host/path[?query]``."""
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    proto = headers.get("x-forwarded-proto", "https")
    host = headers.get("host", "")
    path = event.get("rawPath") or (
        (event.get("requestContext", {}).get("http") or {}).get("path", "")
    )
    query = event.get("rawQueryString", "")
    uri = f"{proto}://{host}{path}"
    if query:
        uri += f"?{query}"
    return uri


def _raw_body(event: Dict[str, Any]) -> str:
    """Exact request body string as received (used for the signature base string)."""
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    return body


def _verify_signature(event: Dict[str, Any]) -> Tuple[bool, str]:
    """Validate the HubSpot v3 webhook signature. Returns ``(ok, reason)``.

    v3 base string is ``method + requestUri + body + timestamp``, HMAC-SHA256 with the app
    client secret, base64-encoded, compared in constant time. Stale requests (timestamp
    older than ``SIGNATURE_MAX_AGE_MS``) are rejected to block replays.
    """
    if not HUBSPOT_CLIENT_SECRET:
        return False, "verification enabled but HUBSPOT_CLIENT_SECRET is unset"

    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    signature = headers.get("x-hubspot-signature-v3")
    timestamp = headers.get("x-hubspot-request-timestamp")
    if not signature or not timestamp:
        return False, "missing signature or timestamp header"

    try:
        sent_ms = int(timestamp)
    except (TypeError, ValueError):
        return False, "invalid timestamp header"
    if abs(int(time.time() * 1000) - sent_ms) > SIGNATURE_MAX_AGE_MS:
        return False, "timestamp outside the allowed window"

    method = ((event.get("requestContext", {}).get("http") or {}).get("method")) or "POST"
    base_string = method + _request_uri(event) + _raw_body(event) + timestamp
    digest = hmac.new(
        HUBSPOT_CLIENT_SECRET.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    if not hmac.compare_digest(expected, signature):
        return False, "signature mismatch"
    return True, ""


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Receive a HubSpot webhook POST and enqueue every event for async processing."""
    try:
        logger.debug("Webhook event keys: %s", list(event.keys()))

        if not SQS_QUEUE_URL:
            logger.error("SQS_QUEUE_URL is not configured")
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Server misconfigured"}),
            }

        if HUBSPOT_SIGNATURE_VERIFICATION_ENABLED:
            ok, reason = _verify_signature(event)
            if not ok:
                logger.warning("Rejected webhook (signature): %s", reason)
                return {"statusCode": 401, "body": json.dumps({"error": "Unauthorized"})}

        events = _parse_events(event)

        valid_events = [body for body in events if _is_valid_event(body)]
        discarded = len(events) - len(valid_events)
        if discarded:
            logger.warning("Discarded %s invalid webhook event(s)", discarded)
        if not valid_events:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "No valid webhook events"}),
            }

        # SendMessageBatch accepts at most 10 entries per call.
        queued = 0
        for batch in _chunked(valid_events, 10):
            entries = [_to_sqs_entry(i, body) for i, body in enumerate(batch)]
            response = sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
            failed = response.get("Failed", [])
            queued += len(response.get("Successful", []))
            if failed:
                # Return 5xx so HubSpot retries the whole delivery; reprocessing is safe
                # because downstream syncs are idempotent on the HubSpot object id.
                logger.error("SQS batch had %s failed entr(ies): %s", len(failed), failed)
                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": "Failed to enqueue some events"}),
                }

        logger.info("Queued %s webhook event(s)", queued)
        return {"statusCode": 204, "body": ""}

    except json.JSONDecodeError as e:
        logger.warning("Invalid JSON body: %s", e)
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON"}),
        }
    except Exception as e:
        logger.exception("Webhook handler failed: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error", "message": str(e)}),
        }
