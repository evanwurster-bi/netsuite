"""
HubSpot settings and credential resolution.

Portal-specific object type IDs come from env. Deployed Lambdas read HubSpot and
NetSuite OAuth credentials from Secrets Manager via ``ACCOUNT_SECRET_NAME``; local
runs use ``.env`` instead.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Tuple

import boto3
from dotenv import load_dotenv

load_dotenv()

_secret_json_cache: Dict[str, Dict[str, Any]] = {}


def account_secret_name() -> str:
    return os.environ.get("ACCOUNT_SECRET_NAME", "").strip()


def load_account_secret(secret_name: str) -> Dict[str, Any]:
    """Fetch and cache the account secret JSON (once per warm container per secret)."""
    cached = _secret_json_cache.get(secret_name)
    if cached is not None:
        return cached

    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    payload = json.loads(response["SecretString"])
    if not isinstance(payload, dict):
        raise ValueError(f"Secret {secret_name!r} must be a JSON object")
    _secret_json_cache[secret_name] = payload
    return payload


def resolve_hubspot_api_key() -> str:
    secret_name = account_secret_name()
    if secret_name:
        secret = load_account_secret(secret_name)
        api_key = secret.get("hubspot_api_key")
        if not api_key:
            raise ValueError(f"Secret {secret_name!r} must include hubspot_api_key")
        return str(api_key)
    return os.environ["HUBSPOT_API_KEY"]


def resolve_netsuite_oauth_credentials() -> Tuple[str, str, str]:
    secret_name = account_secret_name()
    if secret_name:
        secret = load_account_secret(secret_name)
        client_id = secret.get("netsuite_client_id")
        cert_id = secret.get("netsuite_cert_id")
        cert_string = secret.get("netsuite_cert_string")
        if not client_id or not cert_id or not cert_string:
            raise ValueError(
                f"Secret {secret_name!r} must include netsuite_client_id, "
                "netsuite_cert_id, and netsuite_cert_string"
            )
        return str(client_id), str(cert_id), str(cert_string)

    return (
        os.environ["NETSUITE_CLIENT_ID"],
        os.environ["NETSUITE_CERT_ID"],
        os.environ.get("NETSUITE_CERT_STRING", "").strip(),
    )


def _env(key: str, default: str) -> str:
    v = os.environ.get(key)
    if v is None or not str(v).strip():
        return default
    return str(v).strip()


# HubSpot object type IDs (webhook objectTypeId)
HUBSPOT_OBJECT_TYPE_VENUE = _env("HUBSPOT_OBJECT_TYPE_VENUE", "2-47163024")
HUBSPOT_OBJECT_TYPE_PAYMENT = _env("HUBSPOT_OBJECT_TYPE_PAYMENT", "0-101")
# CRM line items: webhooks often use 0-8; some flows use 0-102 — both are accepted.
HUBSPOT_OBJECT_TYPE_LINE_ITEM = _env("HUBSPOT_OBJECT_TYPE_LINE_ITEM", "0-8")
HUBSPOT_LINE_ITEM_OBJECT_TYPE_IDS: frozenset[str] = frozenset(
    {HUBSPOT_OBJECT_TYPE_LINE_ITEM, "0-8", "0-102"}
)

# Deal property internal name used to resolve venue name for invoices.
DEAL_VENUE_NAME_PROPERTY = _env("DEAL_VENUE_NAME_PROPERTY", "venu_name_sync")

# Deal–contact association label used to pick billing contact
HUBSPOT_DEAL_BILLING_ASSOCIATION_LABEL = "Billing"

# Line items with this HubSpot sub_category are skipped when mapping to NetSuite
HUBSPOT_LINE_ITEM_SKIP_SUB_CATEGORY = "Owned Equipment"

# HubSpot venue custom object property internal name to store NetSuite location id after sync
HUBSPOT_VENUE_NETSUITE_ID_PROPERTY = "netsuite_id"

def _parse_comma_separated_ids(raw: str) -> tuple[str, ...]:
    return tuple(s.strip() for s in raw.split(",") if s.strip())


# HubSpot deal stage internal ID(s) that may CREATE a NetSuite invoice (comma-separated).
HUBSPOT_DEAL_STAGE_CREATE_IDS: tuple[str, ...] = _parse_comma_separated_ids(
    os.environ.get("HUBSPOT_DEAL_STAGE_CREATE_ID") or ""
)

# HubSpot deal stage internal ID(s) that may UPDATE an existing invoice only (comma-separated).
HUBSPOT_DEAL_STAGE_UPDATE_IDS: tuple[str, ...] = _parse_comma_separated_ids(
    os.environ.get("HUBSPOT_DEAL_STAGE_UPDATE_ID") or ""
)
