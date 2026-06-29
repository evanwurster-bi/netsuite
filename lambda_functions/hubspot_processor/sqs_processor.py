"""SQS consumer: HubSpot webhook payloads to NetSuite (deals, invoices, venues, line items)."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from config import (
    DEAL_VENUE_NAME_PROPERTY,
    HUBSPOT_DEAL_STAGE_CREATE_IDS,
    HUBSPOT_DEAL_STAGE_UPDATE_IDS,
    HUBSPOT_LINE_ITEM_OBJECT_TYPE_IDS,
    HUBSPOT_LINE_ITEM_SKIP_SUB_CATEGORY,
    HUBSPOT_OBJECT_TYPE_PAYMENT,
    HUBSPOT_OBJECT_TYPE_VENUE,
    HUBSPOT_VENUE_NETSUITE_ID_PROPERTY,
)
from cache import TTLCache
from hubspot import DealInvoiceRejected, HubSpotClient
from locks import (
    consume_pending_resync,
    mark_pending_resync,
    release_lock,
    try_acquire,
)
from netsuite_auth import NetSuiteAuth

logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger.setLevel(logging.INFO)

netsuite = NetSuiteAuth()
hubspot = HubSpotClient()

# Caches for lookups that rarely change, to skip repeat round-trips. They live on these
# module-level instances, so they survive across warm Lambda invocations.
_SUBSIDIARY_CACHE = TTLCache(float(os.getenv("SUBSIDIARY_CACHE_TTL_SECONDS", "300")))
_SALES_REP_CACHE = TTLCache(float(os.getenv("SALES_REP_CACHE_TTL_SECONDS", "300")))

NETSUITE_SUBSIDIARY_ID = os.environ["NETSUITE_SUBSIDIARY_ID"]
WEBHOOK_OBJECT_ID_FILTER_ENABLED = os.getenv(
    "WEBHOOK_OBJECT_ID_FILTER_ENABLED", "false"
).strip().lower() == "true"
WEBHOOK_OBJECT_ID_FILTER_VALUE = os.getenv("WEBHOOK_OBJECT_ID_FILTER_VALUE", "").strip()
WEBHOOK_OBJECT_ID_FILTER_VALUES = {
    value.strip()
    for value in WEBHOOK_OBJECT_ID_FILTER_VALUE.replace(";", ",").split(",")
    if value.strip()
}

VENUE_CATEGORY_TO_NETSUITE: Dict[str, str] = {
    "Off-premise": "1",
    "Owned": "4",
    "Managed": "5",
    "Exclusive": "3",
    "Controlled": "2",
}

_HUBSPOT_LINE_ITEM_PROPERTIES = (
    "name",
    "description",
    "quantity",
    "hs_sku",
    "price",
    "amount",
    "subcategory",
)


# Optional pause between HubSpot/NetSuite calls (see API_CALL_DELAY_SECONDS in template).
_API_CALL_DELAY_SECONDS = float(os.getenv("API_CALL_DELAY_SECONDS", "0"))


def _sleep_between_api_calls() -> None:
    """Optional delay between HubSpot/NetSuite calls (API_CALL_DELAY_SECONDS)."""
    if _API_CALL_DELAY_SECONDS > 0:
        time.sleep(_API_CALL_DELAY_SECONDS)


def _fetch_deal_line_item_details(deal_id: str) -> List[Dict[str, Any]]:
    """One associations call + one batch read for all line items on a deal."""
    association_rows = hubspot.get_deal_line_items(deal_id)
    line_item_ids = [str(row["id"]) for row in association_rows if row.get("id")]
    if not line_item_ids:
        return []
    _sleep_between_api_calls()
    return hubspot.get_line_item_details_batch(
        line_item_ids,
        properties=list(_HUBSPOT_LINE_ITEM_PROPERTIES),
    )


def _read_back(fetch, *, attempts: int = 5, delay: float = 1.0):
    """Retry an eventually-consistent NetSuite ``externalId`` search after a write.

    The search index can lag a freshly written record by a few seconds, so a single read
    right after a create may return nothing even though the record exists. Used only where we
    cannot get the id straight from the write response.
    """
    result = None
    for attempt in range(attempts):
        result = fetch()
        if result:
            return result
        if attempt < attempts - 1:
            time.sleep(delay)
    return result


def _resolve_sales_rep_id(hubspot_owner_id: str) -> Optional[str]:
    """Map a HubSpot owner id to a NetSuite employee (sales rep) id, or None.

    Two round-trips (HubSpot owner -> email -> NetSuite employee); the result is cached by
    owner id because the mapping rarely changes (see _SALES_REP_CACHE).
    """
    hubspot_owner = hubspot.get_owner_by_id(hubspot_owner_id)
    if not hubspot_owner:
        logger.warning("HubSpot owner %s not found", hubspot_owner_id)
        return None
    owner_email = hubspot_owner.get("email")
    if not owner_email:
        logger.warning("HubSpot owner %s has no email", hubspot_owner_id)
        return None
    netsuite_employee = netsuite.search_employee_by_email(owner_email)
    if not netsuite_employee:
        logger.warning("No NetSuite employee for owner email %s", owner_email)
        return None
    sales_rep_id = netsuite_employee.get("id")
    logger.info("Mapped HubSpot owner %s to NetSuite employee %s", owner_email, sales_rep_id)
    return sales_rep_id


def _now_hubspot_datetime_ms() -> str:
    """Current UTC time as a HubSpot datetime value (epoch milliseconds)."""
    return str(int(datetime.now(timezone.utc).timestamp() * 1000))


def _set_deal_invoice_status(deal_id: str, status: str) -> None:
    """Write netsuite_invoice_status and stamp netsuite_invoice_last_modified_date on the deal."""
    hubspot.update_deal_properties(
        deal_id,
        {
            "netsuite_invoice_status": status,
            "netsuite_invoice_last_modified_date": _now_hubspot_datetime_ms(),
        },
    )


def _should_process_object_id(webhook_data: Dict[str, Any]) -> bool:
    """
    Optionally process only webhook items matching configured objectId value(s).
    When the filter is disabled, all messages are processed.
    """
    if not WEBHOOK_OBJECT_ID_FILTER_ENABLED:
        return True

    if not WEBHOOK_OBJECT_ID_FILTER_VALUES:
        logger.warning(
            "WEBHOOK_OBJECT_ID_FILTER_ENABLED=true but WEBHOOK_OBJECT_ID_FILTER_VALUE is empty. Skipping message."
        )
        return False

    object_id = str(webhook_data.get("objectId", "")).strip()
    if object_id not in WEBHOOK_OBJECT_ID_FILTER_VALUES:
        logger.info(
            "Skipping webhook due to objectId filter. objectId=%s allowed=%s",
            object_id or "missing",
            ",".join(sorted(WEBHOOK_OBJECT_ID_FILTER_VALUES)),
        )
        return False

    return True


def _deal_stage_invoice_mode(stage: str) -> Optional[str]:
    """Return ``create``, ``update``, or ``None`` when the stage is not enabled."""
    normalized = stage.strip()
    if normalized in HUBSPOT_DEAL_STAGE_CREATE_IDS:
        return "create"
    if normalized in HUBSPOT_DEAL_STAGE_UPDATE_IDS:
        return "update"
    return None


def _deal_invoice_sync_properties() -> List[str]:
    """HubSpot deal fields used by the invoice gate and reconcile path."""
    return [
        "prior_netsuite_invoice",
        "dealstage",
        "dealname",
        "event_type",
        "amount",
        "venue",
        "event_start_date_and_time",
        DEAL_VENUE_NAME_PROPERTY,
        "venue_netsuite_id",
        "hubspot_owner_id",
        "production_fee",
        "netsuite_est_guest_count",
        "netsuite_invoice_number",
    ]


def _deal_invoice_gate(
    deal_id: str,
    hubspot_deal: Dict[str, Any],
    netsuite_invoice_id: Optional[str],
) -> bool:
    """Return True when the deal should sync to NetSuite (stage and prior-invoice gates)."""
    current_deal_stage = str((hubspot_deal.get("properties") or {}).get("dealstage", "")).strip()
    stage_mode = _deal_stage_invoice_mode(current_deal_stage)
    if stage_mode is None:
        if not HUBSPOT_DEAL_STAGE_CREATE_IDS and not HUBSPOT_DEAL_STAGE_UPDATE_IDS:
            logger.warning(
                "[deal] - HUBSPOT_DEAL_STAGE_CREATE_ID and HUBSPOT_DEAL_STAGE_UPDATE_ID are empty; "
                "skipping invoice sync for deal %s",
                deal_id,
            )
        else:
            logger.info(
                "[deal] - Skipping invoice sync for deal %s because dealstage=%s create=%s update=%s",
                deal_id,
                current_deal_stage or "missing",
                ",".join(HUBSPOT_DEAL_STAGE_CREATE_IDS),
                ",".join(HUBSPOT_DEAL_STAGE_UPDATE_IDS),
            )
        _set_deal_invoice_status(
            deal_id,
            f"Not created: deal stage {current_deal_stage or 'missing'} is not enabled for invoice sync",
        )
        return False

    prior_invoice_value = str(
        (hubspot_deal.get("properties") or {}).get("prior_netsuite_invoice", "")
    ).strip().lower()
    if prior_invoice_value != "no":
        logger.info(
            "[deal] - Skipping invoice sync for deal %s because prior_netsuite_invoice=%s",
            deal_id,
            prior_invoice_value or "missing",
        )
        _set_deal_invoice_status(
            deal_id,
            f"Not created: prior_netsuite_invoice={prior_invoice_value or 'missing'} (must be No)",
        )
        return False

    if stage_mode == "update":
        if not netsuite_invoice_id:
            logger.info(
                "[deal] - Skipping invoice sync for deal %s because dealstage=%s is update-only "
                "and no NetSuite invoice exists yet",
                deal_id,
                current_deal_stage,
            )
            _set_deal_invoice_status(
                deal_id,
                f"Not created: deal stage {current_deal_stage} is update-only and no NetSuite invoice exists yet",
            )
            return False
        logger.info(
            "[deal] - Update-only stage %s; processing invoice %s for deal %s",
            current_deal_stage,
            netsuite_invoice_id,
            deal_id,
        )
        return True

    if netsuite_invoice_id:
        logger.info(
            "[deal] - prior_netsuite_invoice=No and invoice %s exists for deal %s; processing re-sync",
            netsuite_invoice_id,
            deal_id,
        )

    return True


def _should_process_deal_invoice(webhook_data: Dict[str, Any]) -> bool:
    """
    Gate deal-to-invoice processing by deal stage (standalone fetch for tests/tools).

    The production path loads the deal once in ``process_deal_to_invoice`` and reuses it in
    ``reconcile_deal_invoice``; this helper remains for unit tests of the gate rules.
    """
    deal_id = str(webhook_data.get("objectId", "")).strip()
    if not deal_id:
        logger.warning(
            "[deal] - Missing objectId while evaluating deal stage; skipping invoice sync"
        )
        return False

    hubspot_deal = hubspot.get_deal(
        deal_id,
        properties=["prior_netsuite_invoice", "dealstage"],
    )
    if not hubspot_deal:
        logger.warning(
            "[deal] - Could not load deal %s to evaluate deal stage; skipping invoice sync",
            deal_id,
        )
        return False

    netsuite_invoice_id = netsuite.get_invoice_by_deal_id(deal_id)
    return _deal_invoice_gate(deal_id, hubspot_deal, netsuite_invoice_id)


def _is_venue_event(webhook_data: Dict[str, Any]) -> bool:
    """Return True when webhook belongs to HubSpot venue object events."""
    subscription_type = webhook_data.get("subscriptionType", "")
    object_type = webhook_data.get("objectTypeId")

    if subscription_type in ("venue.creation", "venue.propertyChange"):
        return True

    if subscription_type in ("object.creation", "object.propertyChange"):
        return object_type == HUBSPOT_OBJECT_TYPE_VENUE

    return False


def _is_payment_webhook(webhook_data: Dict[str, Any]) -> bool:
    if webhook_data.get("objectTypeId") == HUBSPOT_OBJECT_TYPE_PAYMENT:
        return True
    return webhook_data.get("subscriptionType", "") in (
        "payment.creation",
        "payment.propertyChange",
    )


def _parse_line_amount(raw: Any) -> float:
    try:
        return float(raw or 0)
    except (TypeError, ValueError):
        return 0.0


def _parse_line_quantity(raw: Any) -> float:
    try:
        return float(raw or 0)
    except (TypeError, ValueError):
        return 0.0


def _consolidate_line_items_by_sku(
    hubspot_line_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Group HubSpot lines by ``hs_sku`` and build one synthetic row per SKU."""
    groups: Dict[str, List[Dict[str, Any]]] = {}
    group_order: List[str] = []

    for hubspot_line_item in hubspot_line_items:
        props = hubspot_line_item.get("properties") or {}
        sku = str(props.get("hs_sku") or "").strip()
        if sku not in groups:
            groups[sku] = []
            group_order.append(sku)
        groups[sku].append(hubspot_line_item)

    consolidated: List[Dict[str, Any]] = []
    for sku in group_order:
        items = groups[sku]
        total_quantity = 0.0
        total_amount = 0.0
        name = ""
        subcategory = ""

        for hubspot_line_item in items:
            props = hubspot_line_item.get("properties") or {}
            if not name:
                name = str(props.get("name") or "").strip()
            if not subcategory:
                subcategory = str(props.get("subcategory") or "").strip()
            total_quantity += _parse_line_quantity(props.get("quantity"))
            total_amount += _parse_line_amount(props.get("amount"))

        rate = total_amount / total_quantity if total_quantity else 0.0
        logger.info(
            "[lines] - Consolidated sku=%s from %s HubSpot line(s) qty=%s amount=%s rate=%s",
            sku or "missing",
            len(items),
            total_quantity,
            total_amount,
            rate,
        )
        consolidated.append(
            {
                "properties": {
                    "hs_sku": sku,
                    "name": name,
                    "quantity": total_quantity,
                    "amount": total_amount,
                    "price": rate,
                    "subcategory": subcategory,
                }
            }
        )

    return consolidated


def _parse_hubspot_line_item(
    hubspot_line_item: Dict[str, Any],
    skip_sub_category: str,
) -> Tuple[str, str, float, Dict[str, Any], Optional[str]]:
    """Parse HubSpot line props; return a skip reason or None if eligible for NetSuite lookup."""
    props = hubspot_line_item.get("properties") or {}
    current_sub_category = str(props.get("subcategory") or "").strip().lower()
    hubspot_item_sku = str(props.get("hs_sku") or "").strip()
    hubspot_item_name = str(props.get("name") or "").strip()
    line_amount = _parse_line_amount(props.get("amount"))

    if current_sub_category == skip_sub_category and line_amount <= 0:
        return (
            hubspot_item_sku,
            hubspot_item_name,
            line_amount,
            props,
            f"subcategory={HUBSPOT_LINE_ITEM_SKIP_SUB_CATEGORY} amount<=0",
        )

    if not hubspot_item_sku.startswith("3"):
        return (
            hubspot_item_sku,
            hubspot_item_name,
            line_amount,
            props,
            "sku_missing_or_not_starting_with_3",
        )

    return hubspot_item_sku, hubspot_item_name, line_amount, props, None


def process_deal_lineitems_change(
    hubspot_line_items: List[Dict[str, Any]],
) -> Union[List[Dict[str, Any]], bool]:
    """Map HubSpot lines to NetSuite invoice rows; consolidate by SKU, then validate."""
    try:
        total = len(hubspot_line_items)
        logger.info("[lines] - Evaluating %s HubSpot line item(s)", total)
        consolidated_items = _consolidate_line_items_by_sku(hubspot_line_items)
        logger.info("[lines] - Consolidated to %s SKU group(s)", len(consolidated_items))
        skip_sub_category = str(HUBSPOT_LINE_ITEM_SKIP_SUB_CATEGORY or "").strip().lower()
        eligible: List[Tuple[str, str, float, Dict[str, Any]]] = []
        skipped: List[Tuple[str, str, str]] = []
        skip_counts: Dict[str, int] = {}

        for consolidated_line_item in consolidated_items:
            sku, name, amount, props, skip_reason = _parse_hubspot_line_item(
                consolidated_line_item,
                skip_sub_category,
            )
            if skip_reason:
                skipped.append((sku or "missing", name or "missing", skip_reason))
                skip_counts[skip_reason] = skip_counts.get(skip_reason, 0) + 1
                continue
            eligible.append((sku, name, amount, props))

        logger.info(
            "[lines] - Filter summary total=%s eligible=%s skipped=%s by_reason=%s",
            total,
            len(eligible),
            len(skipped),
            skip_counts,
        )
        for sku, name, skip_reason in skipped:
            logger.info(
                "[lines] - Skipped sku=%s name=%s reason=%s",
                sku,
                name,
                skip_reason,
            )

        final_line_items: List[Dict[str, Any]] = []
        not_found = 0
        unique_skus = list(dict.fromkeys(sku for sku, _, _, _ in eligible))
        sku_to_item = netsuite.search_items_by_sku_batch(unique_skus)
        _sleep_between_api_calls()

        for hubspot_item_sku, hubspot_item_name, line_amount, props in eligible:
            netsuite_item = sku_to_item.get(hubspot_item_sku)

            if not netsuite_item:
                not_found += 1
                logger.error(
                    "[lines] - NetSuite item not found for sku=%s name=%s",
                    hubspot_item_sku,
                    hubspot_item_name or "missing",
                )
                continue

            netsuite_item_id = netsuite_item.get("id")
            line: Dict[str, Any] = {
                "item": {"id": netsuite_item_id},
                "quantity": int(_parse_line_quantity(props.get("quantity"))),
                "amount": line_amount,
                "description": "",
                "rate": float(props.get("price") or 0),
            }
            final_line_items.append(line)

        if not_found:
            logger.info(
                "[lines] - NetSuite lookup summary eligible=%s mapped=%s not_found=%s",
                len(eligible),
                len(final_line_items),
                not_found,
            )

        return final_line_items

    except Exception:
        logger.exception("[lines] - Mapping failed")
        return False


def process_deal_to_invoice(webhook_data: Dict[str, Any]) -> bool:
    """Validate a deal webhook, apply stage gates, then reconcile its NetSuite invoice."""
    is_valid, error_message, deal_id = hubspot.validate_webhook_payload(webhook_data)
    if not is_valid:
        logger.error("Invalid webhook payload: %s", error_message)
        return False

    hubspot_deal = hubspot.get_deal(deal_id, properties=_deal_invoice_sync_properties())
    if not hubspot_deal:
        logger.warning(
            "[deal] - Could not load deal %s to evaluate deal stage; skipping invoice sync",
            deal_id,
        )
        return True

    netsuite_invoice_id = netsuite.get_invoice_by_deal_id(deal_id)
    if not _deal_invoice_gate(deal_id, hubspot_deal, netsuite_invoice_id):
        return True

    return reconcile_deal_invoice(
        deal_id,
        hubspot_deal=hubspot_deal,
        netsuite_invoice_id=netsuite_invoice_id,
    )


def _sync_deal_by_id(deal_id: str) -> bool:
    """Reconcile a Deal invoice from current HubSpot state (no webhook payload)."""
    hubspot_deal = hubspot.get_deal(deal_id, properties=_deal_invoice_sync_properties())
    if not hubspot_deal:
        logger.warning(
            "[deal] - Could not load deal %s for follow-up reconcile; skipping",
            deal_id,
        )
        return True

    netsuite_invoice_id = netsuite.get_invoice_by_deal_id(deal_id)
    if not _deal_invoice_gate(deal_id, hubspot_deal, netsuite_invoice_id):
        return True

    return reconcile_deal_invoice(
        deal_id,
        hubspot_deal=hubspot_deal,
        netsuite_invoice_id=netsuite_invoice_id,
    )


def _reconcile_lock_key(lock_key: str) -> bool:
    """Full state reconcile for the entity serialized under *lock_key*."""
    if lock_key.startswith("venue:"):
        venue_id = lock_key.split(":", 1)[1]
        return process_venue(
            {
                "objectId": venue_id,
                "subscriptionType": "venue.propertyChange",
            }
        )
    return _sync_deal_by_id(lock_key)


def _drain_pending_resync(lock_key: str, token: str) -> None:
    """Run follow-up reconciles while coalesced webhooks set pending_resync during reconcile."""
    while consume_pending_resync(lock_key, token):
        logger.info("[resync] follow-up reconcile lock_key=%s", lock_key)
        _reconcile_lock_key(lock_key)


def reconcile_deal_invoice(
    deal_id: str,
    *,
    hubspot_deal: Optional[Dict[str, Any]] = None,
    netsuite_invoice_id: Optional[str] = None,
) -> bool:
    """Build and upsert the NetSuite invoice for *deal_id* from current HubSpot state.

    Idempotent: it re-reads the deal and all of its line items, so any trigger (deal change,
    line-item change, or a retry) converges to the same invoice. Creates the invoice if it
    is absent and updates it if it exists.

    Returns ``True`` when handled (synced, or a recorded business rejection) and ``False``
    for a permanent rejection. Transient errors propagate so the caller can retry.
    """
    try:
        logger.info("reconcile_deal_invoice: deal_id=%s", deal_id)

        def _set_invoice_status(reason: str) -> None:
            _set_deal_invoice_status(deal_id, reason)

        hubspot_contact = hubspot.get_deal_billing_contact_or_default_contact(deal_id)
        if not hubspot_contact:
            logger.error("No billing/default contact for deal %s", deal_id)
            _set_invoice_status("Not created: no billing or default contact on deal")
            return False

        email = (hubspot_contact.get("properties") or {}).get("email", "")
        if not email:
            logger.error("Contact email missing for deal %s", deal_id)
            _set_invoice_status("Not created: contact email missing")
            return False

        netsuite_customer = netsuite.get_customer_by_email(email)
        _sleep_between_api_calls()
        if not netsuite_customer:
            logger.error("NetSuite customer not found for email %s", email)
            _set_invoice_status(f"Not created: NetSuite customer not found for email {email}")
            return False

        netsuite_customer_id = netsuite_customer.get("id")

        customer_subsidiaries = _SUBSIDIARY_CACHE.get_or_compute(
            str(netsuite_customer_id),
            lambda: netsuite.get_customer_subsidiaries(netsuite_customer_id),
        )
        _sleep_between_api_calls()
        allowed_subsidiary_ids = {
            str(row.get("subsidiary")) for row in customer_subsidiaries
        }
        logger.debug(
            "[subsidiary-debug] customer_id=%s deploy_subsidiary=%s allowed_subsidiaries=%s relationships=%s",
            netsuite_customer_id,
            NETSUITE_SUBSIDIARY_ID,
            sorted(allowed_subsidiary_ids),
            json.dumps(customer_subsidiaries, default=str),
        )

        if str(NETSUITE_SUBSIDIARY_ID) not in allowed_subsidiary_ids:
            logger.error(
                "[deal] - Customer %s is not assigned to subsidiary %s or lacks permissions. Allowed subsidiaries: %s",
                netsuite_customer_id,
                NETSUITE_SUBSIDIARY_ID,
                sorted(allowed_subsidiary_ids),
            )
            _set_invoice_status(
                f"Not created: customer {netsuite_customer_id} not assigned to subsidiary {NETSUITE_SUBSIDIARY_ID}"
            )
            return False

        netsuite_customer_subsidiary_id = NETSUITE_SUBSIDIARY_ID
        logger.info(
            "[subsidiary-debug] resolved netsuite_customer_subsidiary_id=%s",
            netsuite_customer_subsidiary_id,
        )

        if hubspot_deal is None:
            hubspot_deal = hubspot.get_deal(deal_id, properties=_deal_invoice_sync_properties())
        if not hubspot_deal:
            raise RuntimeError(f"Could not load HubSpot deal {deal_id} for invoice reconcile")

        existing_invoice_number = (hubspot_deal.get("properties") or {}).get(
            "netsuite_invoice_number", ""
        )

        hubspot_deal_line_items_details = _fetch_deal_line_item_details(deal_id)

        netsuite_sales_rep_id: Optional[str] = None
        hubspot_owner_id = (hubspot_deal.get("properties") or {}).get("hubspot_owner_id")
        if hubspot_owner_id:
            try:
                # Cached by owner id (owner -> employee mapping rarely changes), so a cache
                # hit skips both the HubSpot owner and NetSuite employee round-trips.
                netsuite_sales_rep_id = _SALES_REP_CACHE.get_or_compute(
                    str(hubspot_owner_id),
                    lambda: _resolve_sales_rep_id(str(hubspot_owner_id)),
                )
            except Exception:
                # Sales rep is best-effort — never fail the whole deal over it.
                logger.exception("Error resolving sales rep from deal owner")

        if netsuite_invoice_id is None:
            netsuite_invoice_id = netsuite.get_invoice_by_deal_id(deal_id)
        _sleep_between_api_calls()

        venue_name_prop = (hubspot_deal.get("properties") or {}).get(
            DEAL_VENUE_NAME_PROPERTY, ""
        )
        if not str(venue_name_prop).strip():
            logger.error(
                "[deal] - Missing venue name on deal (property %s is empty)",
                DEAL_VENUE_NAME_PROPERTY,
            )
            _set_invoice_status(
                f"Not created: missing venue name on deal (property {DEAL_VENUE_NAME_PROPERTY} is empty)"
            )
            return False

        hubspot_venue = hubspot.get_venue_by_name(
            venue_name_prop,
            properties=["name", "address", "city", "state", "zip_code"],
        )
        if not hubspot_venue:
            logger.error(
                "[deal] - No HubSpot venue found for name=%r (check custom object and property names)",
                venue_name_prop,
            )
            _set_invoice_status(
                f"Not created: no HubSpot venue found for name {venue_name_prop}"
            )
            return False

        vprops = hubspot_venue.get("properties") or {}
        hubspot_venue_name = str(vprops.get("name") or "").strip()
        if not hubspot_venue_name:
            logger.error(
                "[deal] - Venue %s has empty name field (name)",
                hubspot_venue.get("id"),
            )
            _set_invoice_status(
                f"Not created: venue {hubspot_venue.get('id')} has empty name field"
            )
            return False
        hubspot_venue_address = " ".join(
            filter(
                None,
                [
                    hubspot_venue_name,
                    vprops.get("address", ""),
                    vprops.get("city", ""),
                    vprops.get("state", ""),
                    vprops.get("zip_code", ""),
                ],
            )
        ).strip()
        
        logger.debug("hubspot_venue: %s", hubspot_venue)
        netsuite_venue = netsuite.get_or_create_venue(hubspot_venue_name, hubspot_venue.get("id"))
        netsuite_venue_id = netsuite_venue.get("id")

        netsuite_line_items = process_deal_lineitems_change(hubspot_deal_line_items_details)
        if netsuite_line_items is False:
            _set_invoice_status("Not created: line item mapping failed")
            return False
        if not netsuite_line_items:
            if netsuite_invoice_id:
                venue_only_payload: Dict[str, Any] = {
                    "location": {"id": netsuite_venue_id},
                    "shipAddress": hubspot_venue_address,
                }
                if netsuite_sales_rep_id:
                    venue_only_payload["salesrep"] = {"id": int(netsuite_sales_rep_id)}
                netsuite.make_request(
                    "PATCH",
                    f"record/v1/invoice/{netsuite_invoice_id}",
                    venue_only_payload,
                )
                logger.info(
                    "[deal] - Updated venue association for existing invoice %s (deal %s)",
                    netsuite_invoice_id,
                    deal_id,
                )
                _set_invoice_status("Invoice Modified")
                return True

            logger.info(
                "[deal] - No eligible line items for deal %s after filters; skipping NetSuite invoice upsert",
                deal_id,
            )
            _set_invoice_status("Not created: no eligible line items after filters")
            return True

        netsuite_invoice = hubspot.map_to_netsuite_format(
            hubspot_deal,
            netsuite_customer_id,
            netsuite_customer_subsidiary_id,
            netsuite_venue_id,
            netsuite_line_items,
        )

        logger.debug("netsuite_invoice: %s", netsuite_invoice)
        if netsuite_sales_rep_id:
            netsuite_invoice["salesrep"] = {"id": int(netsuite_sales_rep_id)}

        netsuite_invoice["shipAddress"] = hubspot_venue_address

        # The invoice upsert is the last NetSuite commit. We take the internal id straight
        # from the upsert response (Location header / known id) instead of re-searching by
        # externalId, because that search is eventually consistent and would otherwise make a
        # just-created invoice look "missing" and wrongly redrive the (already processed)
        # message. A genuine non-success returns a falsy id and raises (transient -> retry).
        created_invoice_id = netsuite.create_or_update_invoice(netsuite_invoice, netsuite_invoice_id)
        if not created_invoice_id:
            raise RuntimeError(f"Invoice upsert did not succeed for deal {deal_id}")
        # Direct GET by internal id is strongly consistent, so no read-back lag here.
        netsuite_invoice_number = netsuite.get_invoice_number(created_invoice_id)
        invoice_status = "Invoice Modified" if existing_invoice_number else "Invoice created"
        hubspot.update_deal_properties(
            deal_id,
            {
                "netsuite_invoice_number": netsuite_invoice_number,
                "netsuite_invoice_status": invoice_status,
                "netsuite_invoice_last_modified_date": _now_hubspot_datetime_ms(),
            },
        )
        logger.info(
            "[deal] - Set netsuite_invoice_number=%s netsuite_invoice_status=%s on deal %s",
            netsuite_invoice_number,
            invoice_status,
            deal_id,
        )
        return True

    except DealInvoiceRejected as exc:
        logger.warning("Deal invoice mapping rejected for deal %s: %s", deal_id, exc)
        _set_deal_invoice_status(deal_id, str(exc))
        return False

    except Exception:
        logger.exception("reconcile_deal_invoice failed for deal %s", deal_id)
        raise


def process_payment(webhook_data: Dict[str, Any]) -> bool:
    """Payment sync is disabled; acknowledge payment webhooks without processing."""
    logger.info(
        "[payment] - sync disabled; skipping objectId=%s subscriptionType=%s",
        webhook_data.get("objectId"),
        webhook_data.get("subscriptionType"),
    )
    return True


def process_venue(webhook_data: Dict[str, Any]) -> bool:
    """Sync a HubSpot venue to NetSuite location and write NetSuite id back to HubSpot."""
    try:
        is_valid, error_message, venue_id = hubspot.validate_webhook_payload(webhook_data)
        if not is_valid:
            logger.error("Invalid venue webhook: %s", error_message)
            return False
        _sleep_between_api_calls()

        hubspot_venue = hubspot.get_venue_by_id(
            venue_id, properties=["name", "address", "city", "state", "zip_code", "category"]
        )
        if not hubspot_venue:
            logger.error("Venue not found in HubSpot: %s", venue_id)
            return False

        venue_properties = hubspot_venue.get("properties") or {}
        venue_name = venue_properties.get("name", "")
        venue_address = venue_properties.get("address", "")
        venue_city = venue_properties.get("city", "")
        venue_state = str(venue_properties.get("state", "")).strip().upper()
        venue_zip_code = venue_properties.get("zip_code", "")
        venue_category = venue_properties.get("category", "")
        venue_type = VENUE_CATEGORY_TO_NETSUITE.get(venue_category)

        logger.info(
            "[venue-debug] HubSpot venue payload id=%s name=%r address=%r city=%r state=%r zip_code=%r category=%r",
            venue_id,
            venue_name,
            venue_address,
            venue_city,
            venue_state,
            venue_zip_code,
            venue_category,
        )

        if not venue_name:
            logger.error("Venue name missing for %s", venue_id)
            return False

        netsuite_venue = netsuite.get_venue_by_external_id(venue_id)
        _sleep_between_api_calls()

        netsuite_venue_data: Dict[str, Any] = {
            "name": venue_name,
            "externalId": venue_id,
            "subsidiary": {"items": [{"id": NETSUITE_SUBSIDIARY_ID}]},
        }
        if venue_type:
            netsuite_venue_data["custrecord_cci_venue_type"] = venue_type
        main_address: Dict[str, Any] = {}
        if venue_address:
            main_address["addr1"] = venue_address
        if venue_city:
            main_address["city"] = venue_city
        if venue_state:
            main_address["state"] = venue_state
        if venue_zip_code:
            main_address["zip"] = venue_zip_code
        if main_address:
            netsuite_venue_data["mainAddress"] = main_address

        logger.info(
            "[venue-debug] NetSuite venue payload id=%s mainAddress=%s",
            venue_id,
            netsuite_venue_data.get("mainAddress"),
        )

        if netsuite_venue:
            result = netsuite.create_or_update_venue(
                netsuite_venue_data, netsuite_venue.get("id")
            )
            netsuite_venue_id = netsuite_venue.get("id")
        else:
            result = netsuite.create_or_update_venue(netsuite_venue_data)
            # Prefer the id from the write response; fall back to the eventually-consistent
            # search only if the Location header was absent.
            netsuite_venue_id = result.get("id")
            if not netsuite_venue_id:
                created = _read_back(lambda: netsuite.get_venue_by_external_id(venue_id))
                netsuite_venue_id = created.get("id") if created else None

        if not result.get("success"):
            # The write itself failed -> transient; let it redrive.
            raise RuntimeError(
                f"Venue upsert did not succeed for HubSpot venue {venue_id}: {result.get('message')}"
            )

        if not netsuite_venue_id:
            # Venue is synced in NetSuite (write succeeded) but its id wasn't resolvable this
            # run. Ack with a loud log and let a later venue event set the back-reference,
            # rather than looping the message in flight.
            logger.warning(
                "[rejected] venue %s synced but id unresolved this run; back-reference deferred",
                venue_id,
            )
            return True

        # Write the NetSuite id back to HubSpot. A failure here raises so the message redrives;
        # re-running is safe (venue upsert and the property write are both idempotent).
        hubspot.update_venue_properties(
            venue_id,
            {HUBSPOT_VENUE_NETSUITE_ID_PROPERTY: str(netsuite_venue_id)},
        )
        logger.info("Updated HubSpot venue %s with NetSuite id %s", venue_id, netsuite_venue_id)

        logger.info("Venue sync finished: %s", result)
        return True

    except Exception:
        logger.exception("process_venue failed")
        raise


def process_line_item(webhook_data: Dict[str, Any]) -> bool:
    """Refresh NetSuite invoice lines when a HubSpot line item changes."""
    try:
        is_valid, error_message, line_item_id = hubspot.validate_webhook_payload(webhook_data)
        if not is_valid:
            logger.error("Invalid line item webhook: %s", error_message)
            return False
        _sleep_between_api_calls()

        deal_id = hubspot.get_line_item_deal_id(line_item_id)
        if not deal_id:
            logger.error("No deal for line item %s", line_item_id)
            return False

        netsuite_invoice_id = netsuite.get_invoice_by_deal_id(deal_id)
        if not netsuite_invoice_id:
            logger.info(
                "[line_item] - No NetSuite invoice yet for deal %s; lines will sync when the "
                "deal invoice is created",
                deal_id,
            )
            return True

        hubspot_line_items = _fetch_deal_line_item_details(deal_id)
        final_line_items = process_deal_lineitems_change(hubspot_line_items)
        if final_line_items is False:
            return False

        if final_line_items:
            netsuite.update_invoice_line_items(netsuite_invoice_id, final_line_items)
            logger.info("Updated NetSuite invoice %s line items", netsuite_invoice_id)
        else:
            logger.warning("No line items to update for invoice %s", netsuite_invoice_id)

        return True

    except Exception:
        logger.exception("process_line_item failed")
        raise


def _resolve_lock_key(webhook_data: Dict[str, Any]) -> Optional[str]:
    """Resolve the serialization key for an event: the parent deal id, or a venue key.

    Every event that touches the same NetSuite invoice must share a key so they never run
    concurrently. Line-item events resolve to their parent deal; venue events use a
    ``venue:<id>`` key (locations are shared in NetSuite, not per-deal).
    """
    subscription_type = webhook_data.get("subscriptionType", "")
    object_id = str(webhook_data.get("objectId", "")).strip()
    if not object_id:
        return None

    if _is_venue_event(webhook_data):
        return f"venue:{object_id}"

    if subscription_type in ("deal.creation", "deal.propertyChange"):
        return object_id

    if subscription_type in (
        "line_item.creation",
        "line_item.propertyChange",
        "line_item.deletion",
    ):
        return hubspot.get_line_item_deal_id(object_id)

    if subscription_type in ("object.creation", "object.propertyChange"):
        object_type = webhook_data.get("objectTypeId")
        if object_type == HUBSPOT_OBJECT_TYPE_PAYMENT:
            return None
        if object_type == HUBSPOT_OBJECT_TYPE_VENUE:
            return f"venue:{object_id}"
        if object_type in HUBSPOT_LINE_ITEM_OBJECT_TYPE_IDS:
            return hubspot.get_line_item_deal_id(object_id)

    return object_id


def process_webhook_message(webhook_data: Dict[str, Any]) -> bool:
    """Serialize per parent deal, then route the event to the right handler.

    Contending workers coalesce: they set ``pending_resync`` and ack without SQS retry.
    The lock holder drains pending flags with follow-up reconciles before release.
    """
    is_venue_event = _is_venue_event(webhook_data)
    if _is_payment_webhook(webhook_data):
        return process_payment(webhook_data)
    if not is_venue_event and not _should_process_object_id(webhook_data):
        return True

    lock_key = _resolve_lock_key(webhook_data)
    if not lock_key:
        logger.info(
            "No parent deal resolved for objectId=%s subscriptionType=%s; skipping",
            webhook_data.get("objectId"),
            webhook_data.get("subscriptionType"),
        )
        return True

    acquired, token = try_acquire(lock_key)
    if not acquired:
        mark_pending_resync(lock_key)
        logger.info("[coalesced] lock busy key=%s; pending_resync set", lock_key)
        return True

    try:
        result = _dispatch_webhook_message(webhook_data)
        _drain_pending_resync(lock_key, token)
        return result
    finally:
        release_lock(lock_key, token)


def _dispatch_webhook_message(webhook_data: Dict[str, Any]) -> bool:
    """Route a single HubSpot webhook payload to the right handler (lock already held)."""
    subscription_type = webhook_data.get("subscriptionType", "")

    if subscription_type in ("deal.propertyChange", "deal.creation"):
        return process_deal_to_invoice(webhook_data)

    if subscription_type in ("venue.creation", "venue.propertyChange"):
        return process_venue(webhook_data)

    if subscription_type in (
        "line_item.creation",
        "line_item.propertyChange",
        "line_item.deletion",
    ):
        return process_line_item(webhook_data)

    if subscription_type in ("object.creation", "object.propertyChange"):
        object_type = webhook_data.get("objectTypeId")
        if object_type == HUBSPOT_OBJECT_TYPE_VENUE:
            return process_venue(webhook_data)
        if object_type == HUBSPOT_OBJECT_TYPE_PAYMENT:
            return process_payment(webhook_data)
        if object_type in HUBSPOT_LINE_ITEM_OBJECT_TYPE_IDS:
            return process_line_item(webhook_data)
        logger.warning("Unsupported objectTypeId: %s", object_type)
        return False

    logger.warning("Unsupported subscriptionType: %s", subscription_type)
    return False


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """SQS trigger: process a batch and report per-message failures.

    Returns a ``batchItemFailures`` list (the ``ReportBatchItemFailures`` contract). Only
    the message ids reported here are redriven by SQS; everything else is deleted.

    Retry policy — three explicit outcomes, easy to spot in the code and in the logs:

    * handler returns ``True``  -> success or intentional skip -> acked, no log noise.
    * handler returns ``False`` -> permanent business rejection (e.g. no billing contact)
      -> acked (retrying cannot help) but logged loudly as ``[rejected] ...`` and recorded
      on the deal via ``netsuite_invoice_status``, so it is visible immediately.
    * processing raises         -> transient error (NetSuite 5xx, network, HubSpot after retries)
      -> reported in ``batchItemFailures`` so SQS redrives it, reaching the DLQ if it persists.
    * lock busy                 -> ``pending_resync`` set, message acked (not a failure).
    """
    records = event.get("Records", [])
    logger.info("SQS records: %s", len(records))
    batch_item_failures: List[Dict[str, str]] = []

    for record in records:
        message_id = record.get("messageId", "unknown")
        try:
            message_body = json.loads(record["body"])
            items = message_body if isinstance(message_body, list) else [message_body]
            for item in items:
                logger.debug(
                    "HubSpot webhook payload: %s",
                    json.dumps(item, separators=(",", ":"), default=str),
                )
                started = time.time()
                handled = process_webhook_message(item)
                elapsed = time.time() - started
                logger.info(
                    "[event] objectId=%s subscriptionType=%s -> %s in %.1fs",
                    item.get("objectId"),
                    item.get("subscriptionType"),
                    "ok" if handled else "rejected",
                    elapsed,
                )
                if not handled:
                    # Permanent business rejection (bad/missing data, no contact, …): ACK it —
                    # retrying cannot help — but log it loudly so it is immediately visible.
                    # The handler also records the reason on the deal (netsuite_invoice_status).
                    logger.warning(
                        "[rejected] acked without sync objectId=%s subscriptionType=%s",
                        item.get("objectId"),
                        item.get("subscriptionType"),
                    )
        except Exception:
            logger.exception("[retry] transient failure messageId=%s", message_id)
            batch_item_failures.append({"itemIdentifier": message_id})

    logger.info(
        "Batch done: records=%s failures=%s", len(records), len(batch_item_failures)
    )
    return {"batchItemFailures": batch_item_failures}
