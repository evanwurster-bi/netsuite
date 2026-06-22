from datetime import datetime, timezone
import logging
import requests
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class DealInvoiceRejected(ValueError):
    """Deal field cannot be mapped to NetSuite; ack webhook without SQS retry."""


def _parse_whole_number_field(raw: Any, field_label: str) -> int:
    text = str(raw).strip()
    try:
        number = float(text)
    except (TypeError, ValueError):
        raise DealInvoiceRejected(
            f"Not synced: {field_label} must be a whole number (got {raw!r})"
        )
    if number != int(number):
        raise DealInvoiceRejected(
            f"Not synced: {field_label} must be a whole number (got {text!r})"
        )
    return int(number)

from config import (
    HUBSPOT_DEAL_BILLING_ASSOCIATION_LABEL,
    HUBSPOT_LINE_ITEM_OBJECT_TYPE_IDS,
    HUBSPOT_OBJECT_TYPE_VENUE,
    resolve_hubspot_api_key,
)


class HubSpotClient:
    """HubSpot CRM client. API key from Secrets Manager (Lambda) or env (local)."""
    
    def __init__(self, api_key: str = None, default_api_version: str = "v3"):
        self.api_key = api_key or resolve_hubspot_api_key()
        self.default_api_version = default_api_version
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
    
    def _get_base_url(self, api_version: str = None) -> str:
        """Get base URL for specified API version."""
        version = api_version or self.default_api_version
        return f'https://api.hubapi.com/crm/{version}'

    # Payment / invoice HubSpot helpers are disabled — see commented block at end of file.

    def get_contact(self, contact_id: str, api_version: str = None, properties: List[str] = None) -> Dict[str, Any]:
        """Fetch contact details from HubSpot API."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/contacts/{contact_id}'
        
        # Add properties parameter if specified
        if properties:
            url += f'?properties={",".join(properties)}'
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json()
    
    def get_deal(self, deal_id: str, api_version: str = None, properties: List[str] = None) -> Dict[str, Any]:
        """Fetch deal details from HubSpot API."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/deals/{deal_id}'
        
        # Add properties parameter if specified
        if properties:
            url += f'?properties={",".join(properties)}'
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json()
    
    def update_deal_properties(self, deal_id: str, properties: Dict[str, Any], api_version: str = None) -> Dict[str, Any]:
        """Update deal properties in HubSpot API."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/deals/{deal_id}'

        update_data = {
            'properties': properties
        }

        response = requests.patch(url, headers=self.headers, json=update_data)
        response.raise_for_status()

        return response.json()

    def get_venue_by_name(
        self,
        venue_name: str,
        api_version: str = None,
        properties: List[str] = None,
    ) -> Dict[str, Any] | None:
        """Fetch venue details from HubSpot API by name, optionally requesting specific properties."""
        if venue_name is None or not str(venue_name).strip():
            logger.info("[HubSpot] Venue search skipped (empty name)")
            return None

        normalized_name = str(venue_name).strip()
        target_name_lower = normalized_name.lower()
        base_url = self._get_base_url(api_version)
        url = f"{base_url}/objects/{HUBSPOT_OBJECT_TYPE_VENUE}/search"
        requested_properties = list(
            dict.fromkeys((properties or []) + ["name", "venue_name"])
        )
        after: str | None = None
        while True:
            body: Dict[str, Any] = {
                "limit": 100,
                "properties": requested_properties,
            }
            if after:
                body["after"] = after

            response = requests.post(url, headers=self.headers, json=body)
            if not response.ok:
                preview = (response.text or "")[:800]
                logger.warning(
                    "[HubSpot] Venue search failed objectType=%s status=%s body=%s",
                    HUBSPOT_OBJECT_TYPE_VENUE,
                    response.status_code,
                    preview,
                )
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])

            for result in results:
                props = result.get("properties") or {}
                for field in ("name", "venue_name"):
                    candidate = str(props.get(field) or "").strip().lower()
                    if candidate and candidate == target_name_lower:
                        logger.info(
                            "[HubSpot] Venue lowercase match for %r using field=%s",
                            venue_name,
                            field,
                        )
                        return result

            after = (((data.get("paging") or {}).get("next") or {}).get("after"))
            if not after:
                break

        logger.info("[HubSpot] Venue not found by lowercase match name=%r", venue_name)
        return None
    
    def get_venue_by_id(self, venue_id: str, api_version: str = None, properties: List[str] = None) -> Dict[str, Any]:
        """Fetch venue details by ID from HubSpot API."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/{HUBSPOT_OBJECT_TYPE_VENUE}/{venue_id}'
        
        # Add properties parameter if specified
        if properties:
            url += f'?properties={",".join(properties)}'
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json()
    
    def update_venue_properties(self, venue_id: str, properties: Dict[str, Any], api_version: str = None) -> Dict[str, Any]:
        """Update venue properties in HubSpot API."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/{HUBSPOT_OBJECT_TYPE_VENUE}/{venue_id}'
        
        # Prepare the update payload
        update_data = {
            'properties': properties
        }
        
        response = requests.patch(url, headers=self.headers, json=update_data)
        response.raise_for_status()
        
        return response.json()
    
    def get_line_item_by_id(self, line_item_id: str, api_version: str = None, properties: List[str] = None) -> Dict[str, Any]:
        return self.get_line_item_detail(line_item_id, api_version, properties)
    
    def get_line_item_deal(self, line_item_id: str, api_version: str = None) -> Dict[str, Any]:
        """Fetch the deal associated with a line item using associations API."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/line_items/{line_item_id}/associations/deals'
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        deals = response.json().get('results', [])
        if deals:
            deal_id = deals[0]['id']
            return self.get_deal(deal_id, api_version)

        return None

    def get_line_item_deal_id(self, line_item_id: str, api_version: str = None) -> str | None:
        """Return only the parent deal id for a line item (no deal fetch).

        Used to resolve the per-deal lock key cheaply; ``get_line_item_deal`` fetches the
        full deal and is used by the handlers that need its properties.
        """
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/line_items/{line_item_id}/associations/deals'
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        deals = response.json().get('results', [])
        return str(deals[0]['id']) if deals else None

    def get_deal_contacts(self, deal_id: str, api_version: str = None) -> List[Dict[str, Any]]:
        """Fetch the contacts associated with a deal."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/deals/{deal_id}/associations/contacts'
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json().get('results', [])
    
    def get_deal_billing_contact_or_default_contact(self, deal_id: str) -> Dict[str, Any]:
        """Fetch the contacts associated to the deal and returns the one with the Billing associaition label. If no billing contact is found, returns the default contact."""
        associtation_type = HUBSPOT_DEAL_BILLING_ASSOCIATION_LABEL
        contacts = self.get_deal_contacts(deal_id, 'v4')

        if len(contacts) == 0:
            return None
        
        return_contact_id = contacts[0].get('toObjectId')
        for contact in contacts:
            contact_association_types = contact.get('associationTypes', [])

            for contact_association_type in contact_association_types:
                if contact_association_type.get('label') == associtation_type:
                    #Found biling contact, return the contact id
                    return_contact_id = contact.get('toObjectId')
                    break
            
            
        
        if return_contact_id:
            return self.get_contact(return_contact_id, 'v3')
        return None
    
    def get_deal_line_items(self, deal_id: str, api_version: str = None) -> List[Dict[str, Any]]:
        """Fetch line items for a specific deal."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/deals/{deal_id}/associations/line_items'
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json().get('results', [])

    def get_line_item_details_batch(
        self,
        line_item_ids: List[str],
        api_version: str = None,
        properties: List[str] = None,
    ) -> List[Dict[str, Any]]:
        """Batch-read line item properties (up to 100 ids per HubSpot request)."""
        if not line_item_ids:
            return []

        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/line_items/batch/read'
        props = list(properties or [])
        results: List[Dict[str, Any]] = []
        chunk_size = 100

        for start in range(0, len(line_item_ids), chunk_size):
            chunk = line_item_ids[start : start + chunk_size]
            payload = {
                "properties": props,
                "inputs": [{"id": line_item_id} for line_item_id in chunk],
            }
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            results.extend(response.json().get("results", []))

        return results
    
    def get_line_item_detail(self, line_item_id: str, api_version: str = None, properties: List[str] = None) -> Dict[str, Any]:
        """Fetch details for a specific line item."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/line_items/{line_item_id}'
        if properties:
            url += f'?properties={",".join(properties)}'
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json()
    
    def map_to_netsuite_format(self, hubspot_deal: Dict[str, Any], netsuite_customer_id: str, netsuite_customer_subsidiary_id: str, venue_netsuite_id: str, line_items_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Map HubSpot deal data to NetSuite invoice format."""
        # Extract relevant fields from HubSpot deal
        properties = hubspot_deal.get('properties', {})
        
        # event_start_date_and_time is "2025-08-01T12:30:00Z"; convert to a NetSuite date.
        # Guard against a missing/malformed value so one bad deal can't crash the sync —
        # tranDate is simply omitted (NetSuite defaults to the current date).
        raw_event_start = properties.get('event_start_date_and_time')
        event_start_str = None
        if raw_event_start:
            try:
                event_start = datetime.strptime(raw_event_start, '%Y-%m-%dT%H:%M:%SZ')
                event_start_str = event_start.replace(tzinfo=timezone.utc).strftime('%Y-%m-%d')
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid event_start_date_and_time=%r on deal %s; omitting tranDate",
                    raw_event_start,
                    hubspot_deal.get('id'),
                )

        # HubSpot deal event_type → NetSuite cseg_nsm_order_type (customrecord_cseg_nsm_order_type IDs).
        # Only types that exist in the segment Values tab are mapped; others omit cseg on the invoice.
        event_type_mapping = {
            'Corporate': '102',
            'Social': '101',
            'Wedding': '1',
            'Tasting': '104',
            'Tour': '105',
            'Athletic': '103',
        }
        segment_order_type = event_type_mapping.get(properties.get('event_type', ''), '')

        # Map to NetSuite invoice format
        netsuite_invoice = {
            'entity': {
                'id': netsuite_customer_id,
            },
            'subsidiary': {
                'id': netsuite_customer_subsidiary_id,
            },
            'tranId': hubspot_deal.get('id'),
            'custbody_hs_deal_id': hubspot_deal.get('id'),
            'custbody_hs_deal_name': properties.get('dealname', ''),
            'memo': properties.get('dealname', ''),
            'externalId': str(hubspot_deal.get('id')),  # Use HubSpot deal ID as external ID
            'location': {
                'id': venue_netsuite_id
            },
            'item': {
                'items': []
            }
        }
        if event_start_str:
            netsuite_invoice['tranDate'] = event_start_str
        if segment_order_type:
            netsuite_invoice['cseg_nsm_order_type'] = segment_order_type
        production_fee = properties.get('production_fee')
        if production_fee is not None and str(production_fee).strip() != '':
            netsuite_invoice['custbody_prod_fee_markup'] = float(production_fee)
        guest_count = properties.get('netsuite_est_guest_count')
        if guest_count is not None and str(guest_count).strip() != '':
            netsuite_invoice['custbody_guest_count'] = _parse_whole_number_field(
                guest_count,
                "netsuite_est_guest_count",
            )
        for line_item in line_items_data:
            item = {
                'item': {
                    'id': line_item.get('item', {}).get('id')
                },
                # 'line': 1,
                'quantity': line_item.get('quantity'),
                'amount': line_item.get('amount'),
                'description': line_item.get('description'),
                'rate': line_item.get('rate'),
                'refName': line_item.get('refName', ''),
            }
            if line_item.get('line'):
                item['line'] = int(line_item.get('line'))
            netsuite_invoice['item']['items'].append(item)
            
            
        
        
        
        return netsuite_invoice
    
    def validate_webhook_payload(self, payload: Dict[str, Any]) -> tuple[bool, str, str]:
        """Validate webhook payload and extract object ID."""
        subscription_type = payload.get('subscriptionType')
        object_type_id = payload.get('objectTypeId')

        if subscription_type not in [
            'deal.creation', 'deal.propertyChange',
            'venue.creation', 'venue.propertyChange',
            'line_item.creation', 'line_item.propertyChange', 'line_item.deletion',
            'object.creation', 'object.propertyChange',
        ]:
            return False, 'Invalid subscription type', None
        
        object_id = payload.get('objectId')
        if not object_id:
            return False, 'No object ID provided', None
        
        # For deal creation, objectId is enough to proceed.
        if subscription_type == 'deal.creation':
            return True, '', str(object_id)

        # For deal property changes, process all property changes.
        if subscription_type == 'deal.propertyChange':
            return True, '', str(object_id)
            
        elif subscription_type in ['object.creation', 'object.propertyChange']:
            if object_type_id == HUBSPOT_OBJECT_TYPE_VENUE:
                return True, '', str(object_id)
            # elif object_type_id == HUBSPOT_OBJECT_TYPE_PAYMENT:
            #     return True, '', str(object_id)
            elif object_type_id in HUBSPOT_LINE_ITEM_OBJECT_TYPE_IDS:
                return True, '', str(object_id)
            else:
                return False, 'Invalid object type', None
        
        
        return True, '', str(object_id) 
    
    def get_owner_by_id(self, owner_id: str, api_version: str = None) -> Dict[str, Any]:
        """Fetch owner details from HubSpot API."""
        # Use the owners API endpoint (this is separate from the CRM API)
        base_url = "https://api.hubapi.com"
        url = f'{base_url}/crm/v3/owners/{owner_id}'
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json()