"""HTTP webhook handler: validates the payload and publishes to SQS.

Invoked by API Gateway HTTP API. ``SQS_QUEUE_URL`` is set by the SAM template. For local
execution, set ``SQS_QUEUE_URL`` to a valid queue URL.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

import boto3

logger = logging.getLogger()
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

sqs = boto3.client("sqs")

# SAM injects this; fallback is only for local testing without env.
SQS_QUEUE_URL = os.environ.get(
    "SQS_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/YOUR_QUEUE_NAME",
)


def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    raw = event.get("body") or "{}"
    if isinstance(raw, dict):
        payload = raw
    else:
        payload = json.loads(raw)
    if isinstance(payload, list) and payload:
        return payload[0]
    if isinstance(payload, dict):
        return payload
    raise ValueError("Webhook body must be a JSON object or non-empty array")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Receive HubSpot webhook POST and enqueue the payload for async processing."""
    try:
        logger.debug("Webhook event keys: %s", list(event.keys()))
        body = _parse_body(event)

        if not body.get("objectId") or not body.get("subscriptionType"):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid webhook payload"}),
            }

        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(body),
            MessageAttributes={
                "webhook_type": {
                    "StringValue": body.get("subscriptionType", "unknown"),
                    "DataType": "String",
                },
                "object_id": {
                    "StringValue": str(body.get("objectId", "")),
                    "DataType": "String",
                },
            },
        )
        logger.info("Queued webhook message: %s", response["MessageId"])
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
