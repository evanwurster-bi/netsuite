from datetime import datetime, timezone
import logging
import time
import requests
from typing import Any, Dict, List, Optional

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


def _hubspot_path_for_log(url: str) -> str:
    if "api.hubapi.com" in url:
        return url.split("api.hubapi.com", 1)[1].split("?")[0]
    return url


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Parse a numeric ``Retry-After`` header (seconds); HTTP-date form is ignored."""
    if not value:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(seconds, 60.0))


from config import (
    HUBSPOT_DEAL_BILLING_ASSOCIATION_LABEL,
    HUBSPOT_LINE_ITEM_OBJECT_TYPE_IDS,
    HUBSPOT_OBJECT_TYPE_VENUE,
    HUBSPOT_VENUE_NAME_SEARCH_PROPERTIES,
    resolve_hubspot_api_key,
)


def _venue_exact_name_filter_groups(
    venue_name: str,
    fields: tuple[str, ...] = HUBSPOT_VENUE_NAME_SEARCH_PROPERTIES,
) -> List[Dict[str, Any]]:
    return [
        {
            "filters": [
                {"propertyName": field, "operator": "EQ", "value": venue_name},
            ]
        }
        for field in fields
    ]


def _venue_token_name_filter_groups(
    venue_name: str,
    fields: tuple[str, ...] = HUBSPOT_VENUE_NAME_SEARCH_PROPERTIES,
) -> List[Dict[str, Any]]:
    return [
        {
            "filters": [
                {"propertyName": field, "operator": "CONTAINS_TOKEN", "value": venue_name},
            ]
        }
        for field in fields
    ]


def _pick_venue_from_results(
    results: List[Dict[str, Any]],
    *,
    target_name_lower: str,
    original_name: str,
    name_fields: tuple[str, ...] = HUBSPOT_VENUE_NAME_SEARCH_PROPERTIES,
) -> Dict[str, Any] | None:
    for result in results:
        props = result.get("properties") or {}
        for field in name_fields:
            candidate = str(props.get(field) or "").strip().lower()
            if candidate and candidate == target_name_lower:
                logger.info(
                    "[HubSpot] Venue lowercase match for %r using field=%s",
                    original_name,
                    field,
                )
                return result
    return None


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

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """HTTP call with retries for HubSpot rate limiting (mirrors NetSuite ``make_request``)."""
        max_retries = 3
        retry_delay = 2
        attempt = 0
        response: Optional[requests.Response] = None

        while attempt < max_retries:
            try:
                if method == "GET":
                    response = requests.get(url, headers=self.headers, **kwargs)
                elif method == "POST":
                    response = requests.post(url, headers=self.headers, **kwargs)
                elif method == "PATCH":
                    response = requests.patch(url, headers=self.headers, **kwargs)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                path = _hubspot_path_for_log(url)
                if response.status_code == 429:
                    logger.warning(
                        "[HubSpot] %s %s -> 429 (rate limit), retry %s/%s",
                        method,
                        path,
                        attempt + 1,
                        max_retries,
                    )
                elif response.ok:
                    logger.info("[HubSpot] %s %s -> %s", method, path, response.status_code)
                else:
                    preview = (response.text or "")[:400].replace("\n", " ")
                    logger.error(
                        "[HubSpot] %s %s -> %s %s",
                        method,
                        path,
                        response.status_code,
                        preview,
                    )

                if response.status_code != 429:
                    response.raise_for_status()
                    return response

                attempt += 1
                if attempt < max_retries:
                    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                    sleep_time = (
                        retry_after
                        if retry_after is not None
                        else retry_delay * (2 ** (attempt - 1))
                    )
                    logger.info(
                        "[HubSpot] 429 backoff %.1fs (retry %s/%s)",
                        sleep_time,
                        attempt,
                        max_retries,
                    )
                    time.sleep(sleep_time)
                    continue

                response.raise_for_status()

            except requests.exceptions.RequestException as exc:
                if attempt == max_retries - 1:
                    raise exc
                attempt += 1
                time.sleep(retry_delay * (2 ** (attempt - 1)))

        if response is None:
            raise RuntimeError(f"HubSpot request {method} {url} produced no response")
        if response.status_code == 429:
            response.raise_for_status()
        return response

    def get_contact(self, contact_id: str, api_version: str = None, properties: List[str] = None) -> Dict[str, Any]:
        """Fetch contact details from HubSpot API."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/contacts/{contact_id}'
        
        # Add properties parameter if specified
        if properties:
            url += f'?properties={",".join(properties)}'
        
        response = self._request("GET", url)
        
        return response.json()
    
    def get_deal(self, deal_id: str, api_version: str = None, properties: List[str] = None) -> Dict[str, Any]:
        """Fetch deal details from HubSpot API."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/deals/{deal_id}'
        
        # Add properties parameter if specified
        if properties:
            url += f'?properties={",".join(properties)}'
        
        response = self._request("GET", url)
        
        return response.json()
    
    def update_deal_properties(self, deal_id: str, properties: Dict[str, Any], api_version: str = None) -> Dict[str, Any]:
        """Update deal properties in HubSpot API."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/deals/{deal_id}'

        update_data = {
            'properties': properties
        }

        response = self._request("PATCH", url, json=update_data)

        return response.json()

    def get_venue_by_name(
        self,
        venue_name: str,
        api_version: str = None,
        properties: List[str] = None,
    ) -> Dict[str, Any] | None:
        """Fetch a venue custom object by name (case-insensitive on ``name`` / ``venue_name``)."""
        if venue_name is None or not str(venue_name).strip():
            logger.info("[HubSpot] Venue search skipped (empty name)")
            return None

        normalized_name = str(venue_name).strip()
        target_name_lower = normalized_name.lower()
        name_fields = HUBSPOT_VENUE_NAME_SEARCH_PROPERTIES
        requested_properties = list(
            dict.fromkeys((properties or []) + list(name_fields))
        )

        exact_match = self._search_venue_by_filters(
            filter_groups=_venue_exact_name_filter_groups(normalized_name),
            requested_properties=requested_properties,
            target_name_lower=target_name_lower,
            original_name=normalized_name,
            api_version=api_version,
            paginate=False,
        )
        if exact_match is not None:
            return exact_match

        token_match = self._search_venue_by_filters(
            filter_groups=_venue_token_name_filter_groups(normalized_name),
            requested_properties=requested_properties,
            target_name_lower=target_name_lower,
            original_name=normalized_name,
            api_version=api_version,
            paginate=True,
        )
        if token_match is not None:
            return token_match

        logger.info("[HubSpot] Venue not found by lowercase match name=%r", venue_name)
        return None

    def _search_venue_by_filters(
        self,
        *,
        filter_groups: List[Dict[str, Any]],
        requested_properties: List[str],
        target_name_lower: str,
        original_name: str,
        api_version: str | None,
        paginate: bool,
    ) -> Dict[str, Any] | None:
        after: str | None = None
        pages_read = 0
        max_pages = 5 if paginate else 1

        while pages_read < max_pages:
            body: Dict[str, Any] = {
                "filterGroups": filter_groups,
                "properties": requested_properties,
                "limit": 100,
            }
            if after:
                body["after"] = after

            data = self._post_venue_search(body, api_version=api_version)
            match = _pick_venue_from_results(
                data.get("results", []),
                target_name_lower=target_name_lower,
                original_name=original_name,
                name_fields=HUBSPOT_VENUE_NAME_SEARCH_PROPERTIES,
            )
            if match is not None:
                return match

            if not paginate:
                return None

            after = (((data.get("paging") or {}).get("next") or {}).get("after"))
            if not after:
                return None
            pages_read += 1

        logger.info(
            "[HubSpot] Venue token search exhausted pagination for name=%r",
            original_name,
        )
        return None

    def _post_venue_search(
        self,
        body: Dict[str, Any],
        api_version: str | None = None,
    ) -> Dict[str, Any]:
        base_url = self._get_base_url(api_version)
        url = f"{base_url}/objects/{HUBSPOT_OBJECT_TYPE_VENUE}/search"
        try:
            response = self._request("POST", url, json=body)
            return response.json()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 400:
                preview = (exc.response.text or "")[:400].replace("\n", " ")
                logger.error(
                    "[HubSpot] venue search 400 objectType=%s filters=%s response=%s",
                    HUBSPOT_OBJECT_TYPE_VENUE,
                    body.get("filterGroups"),
                    preview,
                )
                raise DealInvoiceRejected(
                    "Not synced: HubSpot venue search rejected the query "
                    "(check HUBSPOT_OBJECT_TYPE_VENUE and HUBSPOT_VENUE_NAME_SEARCH_PROPERTIES)"
                ) from exc
            raise
    
    def get_venue_by_id(self, venue_id: str, api_version: str = None, properties: List[str] = None) -> Dict[str, Any]:
        """Fetch venue details by ID from HubSpot API."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/{HUBSPOT_OBJECT_TYPE_VENUE}/{venue_id}'
        
        # Add properties parameter if specified
        if properties:
            url += f'?properties={",".join(properties)}'
        
        response = self._request("GET", url)
        
        return response.json()
    
    def update_venue_properties(self, venue_id: str, properties: Dict[str, Any], api_version: str = None) -> Dict[str, Any]:
        """Update venue properties in HubSpot API."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/{HUBSPOT_OBJECT_TYPE_VENUE}/{venue_id}'
        
        # Prepare the update payload
        update_data = {
            'properties': properties
        }
        
        response = self._request("PATCH", url, json=update_data)
        
        return response.json()
    
    def get_line_item_deal_id(self, line_item_id: str, api_version: str = None) -> str | None:
        """Return only the parent deal id for a line item (no deal fetch)."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/line_items/{line_item_id}/associations/deals'
        response = self._request("GET", url)

        deals = response.json().get('results', [])
        return str(deals[0]['id']) if deals else None

    def get_deal_contacts(self, deal_id: str, api_version: str = None) -> List[Dict[str, Any]]:
        """Fetch the contacts associated with a deal."""
        base_url = self._get_base_url(api_version)
        url = f'{base_url}/objects/deals/{deal_id}/associations/contacts'
        response = self._request("GET", url)
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
        response = self._request("GET", url)
        
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
            response = self._request("POST", url, json=payload)
            results.extend(response.json().get("results", []))

        return results
    
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
        
        response = self._request("GET", url)
        
        return response.json()