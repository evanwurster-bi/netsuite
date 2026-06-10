"""
HubSpot-side settings: portal-specific object type IDs from env; stable field names and deal-stage allowlist in code/env as documented.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


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

# HubSpot deal stage internal ID(s), comma-separated — same name as Lambda / SAM env var.
HUBSPOT_DEAL_STAGE_SYNC_ID: str = (os.environ.get("HUBSPOT_DEAL_STAGE_SYNC_ID") or "").strip()
HUBSPOT_DEAL_STAGE_SYNC_IDS: tuple[str, ...] = tuple(
    s.strip() for s in HUBSPOT_DEAL_STAGE_SYNC_ID.split(",") if s.strip()
)
