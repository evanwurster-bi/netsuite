"""SQS consumer: HubSpot webhook payloads to NetSuite (deals, invoices, payments, venues, line items)."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from config import (
    DEAL_VENUE_NAME_PROPERTY,
    HUBSPOT_DEAL_STAGE_SYNC_IDS,
    HUBSPOT_LINE_ITEM_OBJECT_TYPE_IDS,
    HUBSPOT_LINE_ITEM_SKIP_SUB_CATEGORY,
    HUBSPOT_OBJECT_TYPE_PAYMENT,
    HUBSPOT_OBJECT_TYPE_VENUE,
    HUBSPOT_VENUE_NETSUITE_ID_PROPERTY,
)
from hubspot import HubSpotClient
from netsuite_auth import NetSuiteAuth

logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger.setLevel(logging.INFO)

netsuite = NetSuiteAuth()
hubspot = HubSpotClient()

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


def _sleep_between_api_calls() -> None:
    """Small delay to reduce HubSpot / NetSuite rate-limit pressure."""
    time.sleep(0.5)


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


def get_netsuite_customer_by_email(email: str) -> Optional[Dict[str, Any]]:
    return netsuite.get_customer_by_email(email)


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


def _should_process_deal_invoice(webhook_data: Dict[str, Any]) -> bool:
    """
    Process deal-to-invoice only when HubSpot property prior_netsuite_invoice is No.
    If it is Yes, blank, or missing, the invoice is not created.
    This is the first gate for deal invoice processing.
    This guard runs independently from WEBHOOK_OBJECT_ID_FILTER_ENABLED.
    """
    deal_id = str(webhook_data.get("objectId", "")).strip()
    if not deal_id:
        logger.warning(
            "[deal] - Missing objectId while evaluating prior_netsuite_invoice; skipping invoice sync"
        )
        return False

    hubspot_deal = hubspot.get_deal(
        deal_id,
        properties=["prior_netsuite_invoice", "dealstage"],
    )
    _sleep_between_api_calls()
    if not hubspot_deal:
        logger.warning(
            "[deal] - Could not load deal %s to evaluate prior_netsuite_invoice; skipping invoice sync",
            deal_id,
        )
        return False

    current_deal_stage = str((hubspot_deal.get("properties") or {}).get("dealstage", "")).strip()
    allowed_deal_stages = tuple(str(stage).strip() for stage in HUBSPOT_DEAL_STAGE_SYNC_IDS if str(stage).strip())
    if not allowed_deal_stages:
        logger.warning(
            "[deal] - HUBSPOT_DEAL_STAGE_SYNC_ID is empty; skipping invoice sync for deal %s",
            deal_id,
        )
        return False
    if current_deal_stage not in allowed_deal_stages:
        logger.info(
            "[deal] - Skipping invoice sync for deal %s because dealstage=%s allowed=%s",
            deal_id,
            current_deal_stage or "missing",
            ",".join(allowed_deal_stages),
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

    netsuite_invoice_id = netsuite.get_invoice_by_deal_id(deal_id)
    _sleep_between_api_calls()
    if netsuite_invoice_id:
        logger.info(
            "[deal] - prior_netsuite_invoice=No and invoice %s exists for deal %s; processing re-sync",
            netsuite_invoice_id,
            deal_id,
        )

    return True


def _is_venue_event(webhook_data: Dict[str, Any]) -> bool:
    """Return True when webhook belongs to HubSpot venue object events."""
    subscription_type = webhook_data.get("subscriptionType", "")
    object_type = webhook_data.get("objectTypeId")

    if subscription_type in ("venue.creation", "venue.propertyChange"):
        return True

    if subscription_type in ("object.creation", "object.propertyChange"):
        return object_type == HUBSPOT_OBJECT_TYPE_VENUE

    return False


def process_deal_lineitems_change(
    hubspot_line_items: List[Dict[str, Any]],
) -> Union[List[Dict[str, Any]], bool]:
    """
    Build NetSuite invoice line payloads from HubSpot line items.
    Returns a list of line dicts, or False on unexpected error.
    """
    try:
        logger.info("[lines] - Mapping %s HubSpot line item(s) to NetSuite", len(hubspot_line_items))
        final_line_items: List[Dict[str, Any]] = []
        skip_sub_category = str(HUBSPOT_LINE_ITEM_SKIP_SUB_CATEGORY or "").strip().lower()

        for hubspot_line_item in hubspot_line_items:
            props = hubspot_line_item.get("properties") or {}
            # HubSpot internal name is "subcategory" for line items.
            current_sub_category = str(props.get("subcategory") or "").strip().lower()
            hubspot_item_sku = str(props.get("hs_sku") or "").strip()
            hubspot_item_name = str(props.get("name") or "").strip()
            line_amount_raw = props.get("amount")
            try:
                line_amount = float(line_amount_raw or 0)
            except (TypeError, ValueError):
                line_amount = 0

            # Business rule: Owned Equipment with amount <= 0 should not be sent to NetSuite.
            if current_sub_category == skip_sub_category and line_amount <= 0:
                logger.info(
                    "[lines] - Skip sub_category=%s amount=%s",
                    HUBSPOT_LINE_ITEM_SKIP_SUB_CATEGORY,
                    line_amount,
                )
                continue

            if hubspot_item_sku and not hubspot_item_sku.startswith("3"):
                logger.info(
                    "[lines] - Skip line item sku=%s name=%s (SKU must start with 3)",
                    hubspot_item_sku,
                    hubspot_item_name or "missing",
                )
                continue

            netsuite_item = netsuite.search_item_by_sku(hubspot_item_sku)
            _sleep_between_api_calls()

            if not netsuite_item:
                logger.error(
                    "[lines] - NetSuite item not found for sku=%s name=%s",
                    hubspot_item_sku or "missing",
                    hubspot_item_name or "missing",
                )
                continue

            netsuite_item_id = netsuite_item.get("id")
            line: Dict[str, Any] = {
                "item": {"id": netsuite_item_id},
                "quantity": int(props.get("quantity") or 0),
                "amount": line_amount,
                "description": "",
                "rate": float(props.get("price") or 0),
            }
            final_line_items.append(line)

        return final_line_items

    except Exception:
        logger.exception("[lines] - Mapping failed")
        return False


def process_deal_to_invoice(webhook_data: Dict[str, Any]) -> bool:
    """Create or update a NetSuite invoice from a HubSpot deal webhook."""
    try:
        logger.info("process_deal_to_invoice: validating payload")
        is_valid, error_message, deal_id = hubspot.validate_webhook_payload(webhook_data)
        if not is_valid:
            logger.error("Invalid webhook payload: %s", error_message)
            return False
        _sleep_between_api_calls()

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

        netsuite_customer = get_netsuite_customer_by_email(email)
        _sleep_between_api_calls()
        if not netsuite_customer:
            logger.error("NetSuite customer not found for email %s", email)
            _set_invoice_status(f"Not created: NetSuite customer not found for email {email}")
            return False

        netsuite_customer_id = netsuite_customer.get("id")

        customer_subsidiaries = netsuite.get_customer_subsidiaries(netsuite_customer_id)
        _sleep_between_api_calls()
        allowed_subsidiary_ids = {
            str(row.get("subsidiary")) for row in customer_subsidiaries
        }
        logger.info(
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

        hubspot_deal = hubspot.get_deal(
            deal_id,
            properties=[
                "dealname",
                "dealstage",
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
            ],
        )

        existing_invoice_number = (hubspot_deal.get("properties") or {}).get(
            "netsuite_invoice_number", ""
        )

        hubspot_deal_line_items = hubspot.get_deal_line_items(deal_id)
        hubspot_deal_line_items_details: List[Dict[str, Any]] = []
        for line_item in hubspot_deal_line_items:
            _sleep_between_api_calls()
            detail = hubspot.get_line_item_detail(
                line_item.get("id"),
                properties=[
                    "name",
                    "description",
                    "quantity",
                    "hs_sku",
                    "price",
                    "amount",
                    "subcategory",
                ],
            )
            hubspot_deal_line_items_details.append(detail)

        netsuite_sales_rep_id: Optional[str] = None
        hubspot_owner_id = (hubspot_deal.get("properties") or {}).get("hubspot_owner_id")
        if hubspot_owner_id:
            try:
                hubspot_owner = hubspot.get_owner_by_id(hubspot_owner_id)
                _sleep_between_api_calls()
                if hubspot_owner:
                    owner_email = hubspot_owner.get("email")
                    if owner_email:
                        netsuite_employee = netsuite.search_employee_by_email(owner_email)
                        _sleep_between_api_calls()
                        if netsuite_employee:
                            netsuite_sales_rep_id = netsuite_employee.get("id")
                            logger.info(
                                "Mapped HubSpot owner %s to NetSuite employee %s",
                                owner_email,
                                netsuite_sales_rep_id,
                            )
                        else:
                            logger.warning(
                                "No NetSuite employee for owner email %s",
                                owner_email,
                            )
                    else:
                        logger.warning("HubSpot owner %s has no email", hubspot_owner_id)
                else:
                    logger.warning("HubSpot owner %s not found", hubspot_owner_id)
            except Exception:
                logger.exception("Error resolving sales rep from deal owner")

        netsuite_invoice_id = netsuite.get_invoice_by_deal_id(deal_id)
        time.sleep(1)

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
        
        logger.info("hubspot_venue: %s", hubspot_venue)
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

        logger.info("netsuite_invoice: %s", netsuite_invoice)
        if netsuite_sales_rep_id:
            netsuite_invoice["salesrep"] = {"id": int(netsuite_sales_rep_id)}

        netsuite_invoice["shipAddress"] = hubspot_venue_address

        netsuite.create_or_update_invoice(netsuite_invoice, netsuite_invoice_id)
        time.sleep(1)
        created_invoice_id = netsuite.get_invoice_by_deal_id(deal_id)
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

    except Exception:
        logger.exception("process_deal_to_invoice failed")
        return False


def process_payment(webhook_data: Dict[str, Any]) -> bool:
    """Create or update a NetSuite customer payment from a HubSpot payment webhook."""
    try:
        payment_id = webhook_data.get("objectId")
        if not payment_id:
            logger.error("Payment webhook missing objectId")
            return False

        hubspot_payment = hubspot.get_payment(payment_id, properties=["hs_initial_amount"])
        if not hubspot_payment:
            logger.error("HubSpot payment not found: %s", payment_id)
            return False

        payment_properties = hubspot_payment.get("properties") or {}

        hs_payment_deal = hubspot.get_payment_deal(payment_id)
        if not hs_payment_deal or not hs_payment_deal.get("id"):
            logger.error("No deal associated with payment %s", payment_id)
            return False

        deal_id = hs_payment_deal.get("id")
        hubspot_contact = hubspot.get_deal_billing_contact_or_default_contact(deal_id)
        if not hubspot_contact:
            logger.error("No contact for deal %s", deal_id)
            return False

        contact_email = (hubspot_contact.get("properties") or {}).get("email", "")
        if not contact_email:
            logger.error("Contact email missing for deal %s", deal_id)
            return False

        netsuite_customer = get_netsuite_customer_by_email(contact_email)
        if not netsuite_customer:
            logger.error("NetSuite customer not found: %s", contact_email)
            return False

        netsuite_customer_id = netsuite_customer.get("id")

        hubspot_invoice = hubspot.get_payment_invoice(payment_id)
        if not hubspot_invoice:
            logger.error("No invoice associated with payment %s", payment_id)
            return False

        hubspot_deal = hubspot.get_invoice_deal(hubspot_invoice.get("id"))
        if not hubspot_deal or not hubspot_deal.get("id"):
            logger.error("No deal associated with invoice")
            return False

        deal_id = hubspot_deal.get("id")
        netsuite_invoice_id = netsuite.get_invoice_by_deal_id(deal_id)
        if not netsuite_invoice_id:
            logger.error("NetSuite invoice not found for deal %s", deal_id)
            return False

        netsuite_payment_id = netsuite.get_payment_by_hubspot_id(payment_id)
        dprops = hubspot_deal.get("properties") or {}
        netsuite_venue = netsuite.get_or_create_venue(
            dprops.get("venue_name", ""),
            dprops.get("venue_netsuite_id", ""),
        )
        netsuite_venue_id = netsuite_venue.get("id")

        netsuite_payment: Dict[str, Any] = {
            "entity": {"id": netsuite_customer_id},
            "customer": {"id": netsuite_customer_id},
            "location": {"id": netsuite_venue_id},
            "memo": f"Payment for invoice {hubspot_invoice.get('id')}",
            "payment": float(payment_properties.get("hs_initial_amount") or 0),
            "tranId": hubspot_deal.get("id"),
            "externalId": payment_id,
            "apply": {
                "items": [{"doc": {"id": netsuite_invoice_id}, "apply": True}],
            },
        }
        if payment_properties.get("hs_payment_date"):
            netsuite_payment["tranDate"] = payment_properties["hs_payment_date"]

        netsuite.create_or_update_payment(netsuite_payment, netsuite_payment_id)
        _sleep_between_api_calls()

        if not netsuite.get_payment_by_hubspot_id(payment_id):
            logger.error("NetSuite payment not found after upsert: %s", payment_id)
            return False

        return True

    except Exception:
        logger.exception("process_payment failed")
        return False


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
            time.sleep(0.5)
            created = netsuite.get_venue_by_external_id(venue_id)
            netsuite_venue_id = created.get("id") if created else None

        if result.get("success") and netsuite_venue_id:
            try:
                hubspot.update_venue_properties(
                    venue_id,
                    {HUBSPOT_VENUE_NETSUITE_ID_PROPERTY: str(netsuite_venue_id)},
                )
                logger.info("Updated HubSpot venue %s with NetSuite id %s", venue_id, netsuite_venue_id)
            except Exception:
                logger.exception("Failed to push NetSuite id to HubSpot venue %s", venue_id)

        logger.info("Venue sync finished: %s", result)
        return True

    except Exception:
        logger.exception("process_venue failed")
        return False


def process_line_item(webhook_data: Dict[str, Any]) -> bool:
    """Refresh NetSuite invoice lines when a HubSpot line item changes."""
    try:
        is_valid, error_message, line_item_id = hubspot.validate_webhook_payload(webhook_data)
        if not is_valid:
            logger.error("Invalid line item webhook: %s", error_message)
            return False
        _sleep_between_api_calls()

        hubspot_line_item = hubspot.get_line_item_by_id(
            line_item_id,
            properties=[
                "name",
                "description",
                "quantity",
                "hs_sku",
                "price",
                "amount",
                "subcategory",
            ],
        )
        if not hubspot_line_item:
            logger.error("Line item not found: %s", line_item_id)
            return False

        hubspot_deal = hubspot.get_line_item_deal(line_item_id)
        if not hubspot_deal or not hubspot_deal.get("id"):
            logger.error("No deal for line item %s", line_item_id)
            return False

        deal_id = hubspot_deal.get("id")
        netsuite_invoice_id = netsuite.get_invoice_by_deal_id(deal_id)
        if not netsuite_invoice_id:
            logger.error("No NetSuite invoice for deal %s", deal_id)
            return False

        hubspot_deal_line_items = hubspot.get_deal_line_items(deal_id)
        hubspot_line_items: List[Dict[str, Any]] = []
        for line_item in hubspot_deal_line_items:
            _sleep_between_api_calls()
            hubspot_line_items.append(
                hubspot.get_line_item_detail(
                    line_item.get("id"),
                    properties=[
                        "name",
                        "description",
                        "quantity",
                        "hs_sku",
                        "price",
                        "amount",
                        "subcategory",
                    ],
                )
            )

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
        return False


def process_webhook_message(webhook_data: Dict[str, Any]) -> bool:
    """Route a single HubSpot webhook payload to the right handler."""
    is_venue_event = _is_venue_event(webhook_data)
    if not is_venue_event and not _should_process_object_id(webhook_data):
        return True

    subscription_type = webhook_data.get("subscriptionType", "")

    if subscription_type in ("deal.propertyChange", "deal.creation"):
        if not _should_process_deal_invoice(webhook_data):
            return True
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
    """SQS trigger: process one batch of webhook messages."""
    try:
        logger.info("SQS records: %s", len(event.get("Records", [])))
        success_count = 0
        failure_count = 0

        for record in event.get("Records", []):
            try:
                message_body = json.loads(record["body"])
                items = message_body if isinstance(message_body, list) else [message_body]
                for item in items:
                    logger.info(
                        "HubSpot webhook payload: %s",
                        json.dumps(item, separators=(",", ":"), default=str),
                    )
                    if process_webhook_message(item):
                        success_count += 1
                    else:
                        failure_count += 1
            except Exception:
                logger.exception(
                    "Failed record messageId=%s",
                    record.get("messageId", "unknown"),
                )
                failure_count += 1

        logger.info("Batch done: success=%s failure=%s", success_count, failure_count)
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Processing complete",
                    "success_count": success_count,
                    "failure_count": failure_count,
                }
            ),
        }

    except Exception as e:
        logger.exception("lambda_handler failed: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error", "message": str(e)}),
        }
