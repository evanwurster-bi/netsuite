"""Shared test setup.

These are **unit** tests: they import the Lambda modules directly and mock the AWS / NetSuite
/ HubSpot boundaries, so they need no credentials and hit no network. This file runs before
any test module is collected, so it must put the Lambda source on the path and set the env
vars those modules read at import time.
"""

import os
import sys
import time
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lambda_functions" / "hubspot_processor"))
sys.path.insert(0, str(ROOT / "lambda_functions" / "hubspot_webhook"))

# Dummy env the modules read at import time. No real values / no network.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("NETSUITE_ACCOUNT_ID", "acct")
os.environ.setdefault("NETSUITE_CLIENT_ID", "client")
os.environ.setdefault("NETSUITE_CERT_ID", "cert")
os.environ.setdefault("NETSUITE_SUBSIDIARY_ID", "1")
os.environ.setdefault("HUBSPOT_API_KEY", "hs-test")
os.environ.setdefault("SQS_QUEUE_URL", "https://sqs.us-east-1.amazonaws.com/123/test-queue")
# SYNC_LOCK_TABLE is intentionally left unset -> locks._table is None unless a test injects a
# fake table via the fake_lock_table fixture.

# A throwaway EC key so NetSuiteAuth() can build without real credentials.
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

_key = ec.generate_private_key(ec.SECP256R1())
os.environ.setdefault(
    "NETSUITE_CERT_STRING",
    _key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode(),
)


class FakeLockTable:
    """In-memory stand-in for the DynamoDB lock table honoring the ConditionExpressions
    locks.py uses: acquire-if-free-or-expired, delete-only-if-owner, pending_resync updates."""

    def __init__(self):
        self.items = {}

    def put_item(self, Item, ConditionExpression=None, ExpressionAttributeValues=None):
        key = Item["deal_id"]
        now = ExpressionAttributeValues[":now"]
        cur = self.items.get(key)
        if cur is not None and cur["expiresAt"] >= now:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
        self.items[key] = dict(Item)

    def update_item(
        self,
        Key,
        UpdateExpression=None,
        ConditionExpression=None,
        ExpressionAttributeNames=None,
        ExpressionAttributeValues=None,
    ):
        key = Key["deal_id"]
        cur = self.items.get(key)
        values = ExpressionAttributeValues or {}
        names = ExpressionAttributeNames or {}

        if ConditionExpression:
            if "attribute_exists(deal_id)" in ConditionExpression and cur is None:
                raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem")
            owner_key = names.get("#owner", "owner")
            if "#owner = :token" in ConditionExpression:
                if cur is None or cur.get(owner_key) != values.get(":token"):
                    raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem")
            if "pending_resync = :true" in ConditionExpression:
                if cur is None or not cur.get("pending_resync"):
                    raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem")

        if cur is None:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem")

        if UpdateExpression == "SET pending_resync = :true":
            cur["pending_resync"] = values[":true"]
        elif UpdateExpression == "SET pending_resync = :false":
            cur["pending_resync"] = values[":false"]

    def delete_item(
        self,
        Key,
        ConditionExpression=None,
        ExpressionAttributeNames=None,
        ExpressionAttributeValues=None,
    ):
        key = Key["deal_id"]
        cur = self.items.get(key)
        if cur and ExpressionAttributeValues and cur["owner"] != ExpressionAttributeValues[":token"]:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "DeleteItem")
        self.items.pop(key, None)


@pytest.fixture
def fake_lock_table(monkeypatch):
    """Point locks._table at an in-memory fake so deal_lock works without DynamoDB."""
    import locks

    table = FakeLockTable()
    monkeypatch.setattr(locks, "_table", table)
    return table


@pytest.fixture
def no_sleep(monkeypatch):
    """Strip out the deliberate delays so tests run instantly."""
    import sqs_processor

    monkeypatch.setattr(sqs_processor, "_sleep_between_api_calls", lambda: None)
    monkeypatch.setattr(time, "sleep", lambda *_a, **_k: None)
