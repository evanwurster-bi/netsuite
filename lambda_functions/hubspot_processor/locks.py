"""Per-deal processing lock backed by DynamoDB.

Serializes webhook processing so two events that touch the same NetSuite invoice never run
concurrently. The lock key is the parent deal id (venue events use a ``venue:<id>`` key).

A TTL releases the lock if a worker dies mid-process; a per-acquisition owner token ensures
a worker only ever deletes *its own* lock — never one that a later worker re-acquired after
the first lock expired.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from contextlib import contextmanager
from typing import Iterator

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

SYNC_LOCK_TABLE = os.environ.get("SYNC_LOCK_TABLE", "").strip()
# Safety-net lifetime; keep >= the SQS visibility timeout (960s) so a held lock can never
# block redelivery of the same message forever.
LOCK_TTL_SECONDS = int(os.environ.get("SYNC_LOCK_TTL_SECONDS", "960"))

_table = boto3.resource("dynamodb").Table(SYNC_LOCK_TABLE) if SYNC_LOCK_TABLE else None


class LockNotAcquired(Exception):
    """Another worker currently holds the lock for this key; the caller should retry."""


def _acquire(key: str, token: str) -> bool:
    if _table is None:
        logger.warning("SYNC_LOCK_TABLE not set; processing without a lock (key=%s)", key)
        return True
    now = int(time.time())
    try:
        _table.put_item(
            Item={"deal_id": key, "owner": token, "expiresAt": now + LOCK_TTL_SECONDS},
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
            ConditionExpression="owner = :token",
            ExpressionAttributeValues={":token": token},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # Our lock already expired and was re-acquired by another worker; leave it.
            logger.warning("Sync lock key=%s no longer owned at release; skipping delete", key)
            return
        logger.warning("Failed to release sync lock key=%s (will expire via TTL)", key, exc_info=True)


@contextmanager
def deal_lock(key: str) -> Iterator[None]:
    """Hold the lock for *key* for the duration of the block.

    Raises :class:`LockNotAcquired` if another worker holds it, so the caller can let the
    SQS message redrive and try again once the in-flight event finishes.
    """
    token = uuid.uuid4().hex
    if not _acquire(key, token):
        logger.info("Lock busy key=%s; will retry via SQS redelivery", key)
        raise LockNotAcquired(key)
    try:
        yield
    finally:
        _release(key, token)
