"""Per-deal processing lock backed by DynamoDB.

Serializes webhook processing so two events that touch the same NetSuite invoice never run
concurrently. The lock key is the parent deal id (venue events use a ``venue:<id>`` key).

When the lock is held, contending webhooks set ``pending_resync`` and ack (coalescing) instead
of failing SQS delivery. The lock holder drains ``pending_resync`` with follow-up reconciles
before release.

A TTL releases the lock if a worker dies mid-process; a per-acquisition owner token ensures
a worker only ever deletes *its own* lock — never one that a later worker re-acquired after
the first lock expired.
"""

from __future__ import annotations

import logging
import os
import time
import uuid

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

SYNC_LOCK_TABLE = os.environ.get("SYNC_LOCK_TABLE", "").strip()
LOCK_TTL_SECONDS = int(os.environ.get("SYNC_LOCK_TTL_SECONDS", "960"))

_table = boto3.resource("dynamodb").Table(SYNC_LOCK_TABLE) if SYNC_LOCK_TABLE else None


def try_acquire(key: str) -> tuple[bool, str]:
    """Try to acquire the lock. Returns ``(acquired, owner_token)``."""
    token = uuid.uuid4().hex
    if _table is None:
        logger.warning("SYNC_LOCK_TABLE not set; processing without a lock (key=%s)", key)
        return True, token
    if _acquire(key, token):
        return True, token
    return False, token


def mark_pending_resync(key: str) -> None:
    """Record that *key* needs another reconcile while another worker holds the lock."""
    if _table is None:
        return
    try:
        _table.update_item(
            Key={"deal_id": key},
            UpdateExpression="SET pending_resync = :true",
            ConditionExpression="attribute_exists(deal_id)",
            ExpressionAttributeValues={":true": True},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info(
                "Lock row missing when marking pending_resync key=%s; a later webhook will sync",
                key,
            )
            return
        raise


def consume_pending_resync(key: str, token: str) -> bool:
    """Atomically clear ``pending_resync`` when true and owned by *token*.

    Returns ``True`` when a pending resync was consumed (caller should reconcile again).
    """
    if _table is None:
        return False
    try:
        _table.update_item(
            Key={"deal_id": key},
            UpdateExpression="SET pending_resync = :false",
            ConditionExpression="#owner = :token AND pending_resync = :true",
            ExpressionAttributeNames={"#owner": "owner"},
            ExpressionAttributeValues={
                ":token": token,
                ":false": False,
                ":true": True,
            },
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def release_lock(key: str, token: str) -> None:
    """Release the lock when *token* is still the owner."""
    _release(key, token)


def _acquire(key: str, token: str) -> bool:
    if _table is None:
        return True
    now = int(time.time())
    try:
        _table.put_item(
            Item={
                "deal_id": key,
                "owner": token,
                "expiresAt": now + LOCK_TTL_SECONDS,
                "pending_resync": False,
            },
            ConditionExpression="attribute_not_exists(deal_id) OR expiresAt < :now",
            ExpressionAttributeValues={":now": now},
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def _release(key: str, token: str) -> None:
    if _table is None:
        return
    try:
        _table.delete_item(
            Key={"deal_id": key},
            ConditionExpression="#owner = :token",
            ExpressionAttributeNames={"#owner": "owner"},
            ExpressionAttributeValues={":token": token},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning("Sync lock key=%s no longer owned at release; skipping delete", key)
            return
        logger.warning("Failed to release sync lock key=%s (will expire via TTL)", key, exc_info=True)
