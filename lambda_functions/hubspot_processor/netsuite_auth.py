import base64
import logging
import os
import time
import jwt
from typing import Any, Dict, List, Optional
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _netsuite_path_for_log(url: str) -> str:
    """Short path for logs (no query string secrets)."""
    if "/services/rest/" in url:
        return url.split("/services/rest/", 1)[1].split("?")[0]
    return url


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Parse a numeric ``Retry-After`` header (seconds); HTTP-date form is ignored.

    Clamped to [0, 60] so a hostile or oversized value cannot stall the worker.
    """
    if not value:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(seconds, 60.0))


def _id_from_location(response: Optional[requests.Response]) -> Optional[str]:
    """Extract the new record's internal id from a NetSuite create response.

    NetSuite returns the id in the ``Location`` header (``.../record/v1/<type>/<id>``) on a
    successful create. Reading it from the response is strongly consistent — unlike the
    ``externalId`` search — so it is the reliable way to learn a just-created record's id
    without waiting for the search index to catch up.
    """
    if response is None:
        return None
    location = (response.headers or {}).get("Location", "")
    if not location:
        return None
    candidate = location.rstrip("/").rsplit("/", 1)[-1].strip()
    return candidate or None


def _unescape_pem_line_breaks(s: str) -> str:
    """Turn literal ``\\n`` / ``\\r`` (as stored in env or YAML) into real newlines; repeat for double-escaped values."""
    for _ in range(8):
        t = s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")
        if t == s:
            break
        s = t
    return s.replace("\r\n", "\n").replace("\r", "\n")


def _pem_private_key_bytes_from_cert_string(raw: str) -> bytes:
    """Normalize ``NETSUITE_CERT_STRING`` into PEM bytes for ``load_pem_private_key``.

    Accepts:

    * Multiline PEM (real newlines, as in a ``.pem`` file).
    * One-line PEM with literal ``\\n`` / ``\\r`` between lines (common in env / SAM).
    * Base64-encoded PEM (single line, no ``-----BEGIN`` until decoded).
    """
    s = (raw or "").strip()
    if not s:
        raise ValueError("NETSUITE_CERT_STRING is empty")
    s = s.removeprefix("\ufeff")
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
        s = s[1:-1].strip()

    s = _unescape_pem_line_breaks(s)

    begin = s.find("-----BEGIN")
    if begin >= 0:
        pem_text = s[begin:].rstrip()
        # Body lines may still carry literal backslash-n if escaping was layered.
        pem_text = _unescape_pem_line_breaks(pem_text)
        pem_bytes = pem_text.encode("utf-8")
        if pem_bytes[:1] != b"-":
            raise ValueError("PEM text did not start with '-' after normalization.")
        return pem_bytes

    cleaned = "".join(s.split())
    decoded = base64.b64decode(cleaned, validate=False)
    if b"-----BEGIN" not in decoded:
        raise ValueError(
            "NETSUITE_CERT_STRING must be PEM (multiline, one line with \\n, or base64 PEM); run scripts/encode_pem_for_sam.py with scripts/private.pem."
        )
    return decoded


class NetSuiteAuth:
    """NetSuite REST OAuth (client credentials + JWT). Same env contract as ``generate_token.py``."""

    def __init__(self) -> None:
        self.account_id = os.environ["NETSUITE_ACCOUNT_ID"]
        self.client_id = os.environ["NETSUITE_CLIENT_ID"]
        self.cert_id = os.environ["NETSUITE_CERT_ID"]
        self.subsidiary_id = os.environ["NETSUITE_SUBSIDIARY_ID"]
        self.token_url = (
            f"https://{self.account_id}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token"
        )
        self.base_url = (
            f"https://{self.account_id}.suitetalk.api.netsuite.com/services/rest"
        )

        # Cached OAuth access token, reused across warm invocations until near expiry.
        self._access_token: Optional[str] = None
        self._access_token_expiry: float = 0.0

        raw_cert = os.environ.get("NETSUITE_CERT_STRING", "").strip()
        pem_bytes = _pem_private_key_bytes_from_cert_string(raw_cert)

        try:
            self.private_key = serialization.load_pem_private_key(
                pem_bytes,
                password=None,
            )
            if not isinstance(self.private_key, ec.EllipticCurvePrivateKey):
                raise ValueError("Private key must be an EC key (Elliptic Curve)")
        except Exception as e:
            raise Exception(f"Error loading private key from environment: {str(e)}") from e

    def _generate_jwt(self) -> str:
        """Client-assertion JWT (same claims as ``generate_token`` / NetSuite certificate mapping)."""
        now = int(time.time())
        header = {"typ": "JWT", "alg": "ES256", "kid": self.cert_id}
        payload = {
            "iss": self.client_id,
            "scope": "restlets,rest_webservices,suite_analytics",
            "aud": self.token_url,
            "exp": now + 3600,
            "iat": now,
        }
        token = jwt.encode(payload, self.private_key, algorithm="ES256", headers=header)
        if isinstance(token, bytes):
            return token.decode("ascii")
        return str(token)

    def get_access_token(self) -> str:
        """OAuth 2.0 client_credentials + jwt-bearer, with a cached token.

        The token is cached on the instance and reused until ~60s before expiry. The
        processor keeps a module-level ``NetSuiteAuth``, so the cache survives across warm
        Lambda invocations and avoids minting a JWT and a token call on every request.
        """
        now = time.time()
        if self._access_token and now < self._access_token_expiry - 60:
            return self._access_token

        assertion = self._generate_jwt()
        response = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                "client_assertion": assertion,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
        payload = response.json()
        self._access_token = payload["access_token"]
        self._access_token_expiry = now + float(payload.get("expires_in", 3600))
        return self._access_token

    def make_request(self, method: str, endpoint: str, data: Optional[Dict] = None, additional_headers: Optional[Dict] = None, params: Optional[Dict] = None) -> requests.Response:
        """Make an authenticated request to NetSuite API with retries for rate limiting."""
        max_retries = 3
        retry_delay = 2  # Initial delay in seconds
        attempt = 0
        response: Optional[requests.Response] = None

        while attempt < max_retries:
            try:
                access_token = self.get_access_token()

                url = f"{self.base_url}/{endpoint}"
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                }
                if additional_headers:
                    headers.update(additional_headers)

                if method == 'GET':
                    response = requests.get(url, headers=headers, params=data)
                elif method == 'POST':
                    response = requests.post(url, headers=headers, json=data)
                elif method == 'PUT':
                    response = requests.put(url, headers=headers, json=data)
                elif method == 'PATCH':
                    response = requests.patch(url, headers=headers, json=data, timeout=1000, params=params)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                path = _netsuite_path_for_log(url)
                if response.status_code == 429:
                    logger.warning(
                        "[NetSuite] %s %s -> 429 (rate limit), retry %s/%s",
                        method,
                        path,
                        attempt + 1,
                        max_retries,
                    )
                elif response.ok:
                    logger.info("[NetSuite] %s %s -> %s", method, path, response.status_code)
                else:
                    preview = (response.text or "")[:400].replace("\n", " ")
                    logger.error(
                        "[NetSuite] %s %s -> %s %s",
                        method,
                        path,
                        response.status_code,
                        preview,
                    )

                if response.status_code != 429:
                    response.raise_for_status()
                    return response

                # Handle rate limiting: honor Retry-After when present, else exponential backoff.
                attempt += 1
                if attempt < max_retries:
                    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                    sleep_time = (
                        retry_after
                        if retry_after is not None
                        else retry_delay * (2 ** (attempt - 1))
                    )
                    logger.info(
                        "[NetSuite] 429 backoff %.1fs (retry %s/%s)",
                        sleep_time,
                        attempt,
                        max_retries,
                    )
                    time.sleep(sleep_time)
                    continue

                response.raise_for_status()

            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise e
                attempt += 1
                time.sleep(retry_delay * (2 ** (attempt - 1)))

        if response is None:
            raise RuntimeError(f"NetSuite request {method} {endpoint} produced no response")
        return response
    
    def get_customer_by_email(self, email: str) -> Dict[str, Any]:
        """Get customer by email."""
        # Escape double quotes so an odd address can't break or inject into the q filter.
        safe_email = str(email or "").replace('"', '\\"')
        response = self.make_request('GET', 'record/v1/customer', {'q': f'email IS "{safe_email}"'})
        items = response.json().get('items', [])
        if items:
            return items[0]
        return None
    
    def get_customer_subsidiaries(self, customer_id: str) -> List[Dict[str, Any]]:
        """Return subsidiaries shared with a customer via customerSubsidiaryRelationship."""
        payload = {
            "q": (
                "SELECT subsidiary, isPrimarySub "
                "FROM customerSubsidiaryRelationship "
                f"WHERE entity = {int(customer_id)}"
            )
        }
        response = self.make_request(
            'POST',
            'query/v1/suiteql',
            payload,
            additional_headers={'Prefer': 'transient'},
        )
        return response.json().get('items', [])
    
    def create_or_update_invoice(self, netsuite_invoice: Dict[str, Any], invoice_id: str = None) -> Optional[str]:
        """Create or update an invoice, keyed by externalId (the HubSpot deal id).

        Returns the NetSuite **internal id** on success and ``None`` on a non-success status.
        The id comes from the known ``invoice_id`` on update, or from the create response's
        ``Location`` header — never from a follow-up ``externalId`` search, which is
        eventually consistent and would make a just-created invoice look missing.

        Idempotency safeguard: if a concurrent worker created the invoice between the caller's
        existence check and this POST, NetSuite rejects the duplicate externalId; we recover
        by re-resolving the invoice by externalId and patching it instead of duplicating.
        """
        ext = str(netsuite_invoice.get("externalId", ""))
        if invoice_id:
            response = self.make_request(
                "PATCH",
                f"record/v1/invoice/{invoice_id}",
                netsuite_invoice,
                params={"replace": "item"},
            )
            logger.info("[NetSuite] Invoice PATCH id=%s externalId=%s -> %s", invoice_id, ext, response.status_code)
            return str(invoice_id) if response.status_code in (200, 201, 204) else None

        try:
            response = self.make_request("POST", "record/v1/invoice", netsuite_invoice)
        except requests.exceptions.HTTPError:
            existing_id = self.get_invoice_by_deal_id(ext) if ext else None
            if not existing_id:
                raise
            logger.warning(
                "[NetSuite] Invoice POST failed but externalId=%s already exists (id=%s); patching instead",
                ext,
                existing_id,
            )
            response = self.make_request(
                "PATCH",
                f"record/v1/invoice/{existing_id}",
                netsuite_invoice,
                params={"replace": "item"},
            )
            return str(existing_id) if response.status_code in (200, 201, 204) else None

        logger.info("[NetSuite] Invoice POST externalId=%s -> %s", ext, response.status_code)
        if response.status_code not in (200, 201, 204):
            return None
        # Prefer the strongly-consistent Location id; fall back to search only if absent.
        return _id_from_location(response) or self.get_invoice_by_deal_id(ext)
    
    def get_invoice_by_deal_id(self, deal_id: str) -> str:
        """Get invoice by deal ID."""
        response = self.make_request('GET', 'record/v1/invoice', {'q': f'externalId IS "{deal_id}"'})
        items = response.json().get('items', [])
        if items:
            return items[0]['id']
        return None

    def get_invoice_number(self, invoice_id: str) -> str:
        """Get the NetSuite invoice document number (tranId) for an internal invoice id."""
        response = self.make_request('GET', f'record/v1/invoice/{invoice_id}')
        return response.json()['tranId']
    
    def get_payment_by_hubspot_id(self, hubspot_payment_id: str) -> str:
        """Get payment by HubSpot payment ID."""
        response = self.make_request('GET', 'record/v1/customerpayment', {'q': f'externalId IS "{hubspot_payment_id}"'})
        items = response.json().get('items', [])
        if items:
            return items[0]['id']
        return None
    
    def create_or_update_payment(self, netsuite_payment: Dict[str, Any], payment_id: str = None) -> Dict[str, Any]:
        """Create or update payment in NetSuite."""
        if payment_id:
            # Update existing payment
            response = self.make_request('PATCH', f'record/v1/customerpayment/{payment_id}', netsuite_payment)
        else:
            # Create new payment
            response = self.make_request('POST', 'record/v1/customerpayment', netsuite_payment)
        if response.status_code == 204:
            return {'success': True, 'message': 'Payment updated successfully'}
        elif response.status_code == 201:
            return {'success': True, 'message': 'Payment created successfully'}
        else:
            return {'success': False, 'message': f'Payment operation failed with status {response.status_code}'}

    # NOTE: Customer deposit / payment-application helpers are kept (commented) for a
    # future requirement. They are not wired into any handler yet.
    # def get_customer_deposit_by_hubspot_id(self, hubspot_deal_id: str) -> str:
    #     """Get customer deposit by HubSpot deal ID."""
    #     response = self.make_request('GET', 'record/v1/customerdeposit', {'q': f'externalId IS "{hubspot_deal_id}"'})
    #     items = response.json().get('items', [])
    #     if items:
    #         return items[0]['id']
    #     return None
    #
    # def create_or_update_customer_deposit(self, netsuite_customer_deposit: Dict[str, Any], customer_deposit_id: str = None) -> Dict[str, Any]:
    #     """Create or update customer deposit in NetSuite."""
    #     if customer_deposit_id:
    #         # Update existing customer deposit
    #         response = self.make_request('PATCH', f'record/v1/customerdeposit/{customer_deposit_id}', netsuite_customer_deposit)
    #     else:
    #         # Create new customer deposit
    #         response = self.make_request('POST', 'record/v1/customerdeposit', netsuite_customer_deposit)
    #
    #     if response.status_code == 204:
    #         return {'success': True, 'message': 'Customer deposit updated successfully'}
    #     elif response.status_code == 201:
    #         return {'success': True, 'message': 'Customer deposit created successfully'}
    #     else:
    #         return {'success': False, 'message': f'Customer deposit operation failed with status {response.status_code}'}
    #
    # def deposit_application_to_invoice(self, netsuite_deposit_id: str, netsuite_invoice_id: str) -> Dict[str, Any]:
    #     """Apply deposit to invoice in NetSuite."""
    #     response = self.make_request('POST', f'record/v1/customerDeposit/{netsuite_deposit_id}/!transform/depositApplication',
    #     {
    #         "deposit": {"id": netsuite_deposit_id},
    #         "apply": {
    #             "items": [
    #                 {
    #                     "doc": {
    #                         "id": netsuite_invoice_id,
    #                     },
    #                     "apply": True
    #                 }
    #             ]
    #         }
    #     })
    #     return response.json()
    #
    # def apply_payment_to_invoice(self, netsuite_payment_id: str, netsuite_invoice_id: str) -> Dict[str, Any]:
    #     """Apply payment to invoice in NetSuite."""
    #     response = self.make_request('POST', f'record/v1/customerPayment/{netsuite_payment_id}/!transform/paymentApplication',
    #     {
    #         "payment": {"id": netsuite_payment_id},
    #         "apply": {
    #             "items": [
    #                 {
    #                     "doc": {
    #                         "id": netsuite_invoice_id,
    #                     },
    #                     "apply": True
    #                 }
    #             ]
    #         }
    #     })
    #     return response.json()

    def get_venue_by_external_id(self, external_id: str) -> str:
        """Get venue by external ID."""
        response = self.make_request('GET', 'record/v1/location', {'q': f'externalId IS "{external_id}"'})
        items = response.json().get('items', [])
        if items and len(items) > 0:
            return items[0]
        return None

    def get_venue_by_name(self, venue_name: str) -> str:
        """Get venue by name."""
        response = self.make_request('GET', 'record/v1/location', {'q': f'name IS "{venue_name}"'})
        items = response.json().get('items', [])
        if items and len(items) > 0:
            return items[0]
        return None
    
    def get_or_create_venue(self, venue_name: str, venue_external_id: str) -> Dict[str, Any]:
        """Resolve a NetSuite location by name, creating it or back-filling its externalId.

        Always returns the resolved location dict, or raises — it never returns ``None``, so
        callers can safely read ``.get('id')``.
        """
        venue_by_name = self.get_venue_by_name(venue_name)
        if venue_by_name:
            existing_external_id = venue_by_name.get('externalId')
            if existing_external_id is None:
                # Location exists without an externalId; back-fill it so future lookups match.
                self.create_or_update_venue(
                    {"name": venue_name, "externalId": venue_external_id},
                    venue_by_name.get('id'),
                )
                resolved = self.get_venue_by_external_id(venue_external_id)
                if not resolved:
                    raise Exception(f"Failed to back-fill externalId for venue {venue_name}")
                return resolved
            if existing_external_id == venue_external_id:
                return venue_by_name
            raise Exception(
                f"Venue with name {venue_name} already exists with external ID "
                f"{existing_external_id}, which differs from the provided {venue_external_id}"
            )

        self.create_or_update_venue({
            "name": venue_name,
            "externalId": venue_external_id,
            "subsidiary": {"items": [{"id": self.subsidiary_id}]},
        })
        resolved = self.get_venue_by_external_id(venue_external_id)
        if not resolved:
            raise Exception(f"Failed to create venue {venue_name}")
        return resolved
    
    def create_or_update_venue(self, netsuite_venue: Dict[str, Any], venue_id: str = None) -> Dict[str, Any]:
        """Create or update venue in NetSuite.

        On success the result includes the location's internal ``id`` (the known id on
        update, or the create response's ``Location`` id) so callers don't have to re-search
        by externalId — which is eventually consistent right after a write.
        """
        if venue_id:
            # Update existing venue
            response = self.make_request('PATCH', f'record/v1/location/{venue_id}', netsuite_venue)
        else:
            # Create new venue
            response = self.make_request('POST', 'record/v1/location', netsuite_venue)

        if response.status_code in (200, 201, 204):
            return {
                'success': True,
                'message': 'Venue created/updated successfully',
                'id': str(venue_id) if venue_id else _id_from_location(response),
            }
        return {'success': False, 'message': f'Venue operation failed with status {response.status_code}'}
        

    
    def search_item_by_sku(self, sku: str = "") -> Optional[Dict[str, Any]]:
        """Resolve NetSuite item by SKU only (itemid)."""
        normalized_sku = str(sku or "").strip()
        if not normalized_sku:
            logger.warning("[NetSuite] Missing SKU for item lookup")
            return None

        safe_sku = normalized_sku.replace("'", "''")
        payload = {
            "q": f"SELECT id, itemid, displayname, upccode FROM item WHERE itemid = '{safe_sku}'"
        }
        response = self.make_request(
            'POST',
            'query/v1/suiteql',
            payload,
            additional_headers={'Prefer': 'transient'}
        )
        if response.status_code == 200:
            items = response.json().get('items', [])
            if items:
                row = items[0]
                logger.info(
                    "[NetSuite] Item match by sku=%s netsuite_id=%s",
                    normalized_sku,
                    row.get("id"),
                )
                return row

        logger.warning(
            "[NetSuite] No item match for sku=%s",
            normalized_sku,
        )
        return None
        
    def update_invoice_line_items(self, invoice_id: str, line_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Replace all line items for a specific invoice.

        Uses ``?replace=item`` so NetSuite fully rebuilds the ``item`` sublist on every
        update instead of appending the incoming lines to the existing ones (which is
        what causes duplicate rows on the invoice).
        """
        update_data = {
            'item': {
                'items': line_items
            }
        }

        response = self.make_request(
            'PATCH',
            f'record/v1/invoice/{invoice_id}',
            update_data,
            params={"replace": "item"},
        )

        logger.info(
            "[NetSuite] Invoice line items PATCH id=%s count=%s -> %s",
            invoice_id,
            len(line_items),
            response.status_code,
        )

        if response.status_code in (200, 204):
            return {'success': True, 'message': 'Invoice line items updated successfully'}
        else:
            return {'success': False, 'message': f'Invoice line items update failed with status {response.status_code}'}
    
    def search_employee_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Search for an employee by email in NetSuite using SuiteQL."""
        
        # Sanitize email to prevent SQL injection
        safe_email = email.replace("'", "''")
        
        payload = {
            "q": f"SELECT id, entityid, email FROM employee WHERE email = '{safe_email}'"
        }
        
        try:
            response = self.make_request('POST', 'query/v1/suiteql', payload, additional_headers={'Prefer': 'transient'})
            
            if response.status_code == 200:
                results = response.json()
                employees = results.get('items', [])
                if employees:
                    return employees[0]
                logger.info("[NetSuite] No employee row for email=%s", email)
            else:
                logger.warning(
                    "[NetSuite] Employee SuiteQL failed status=%s",
                    response.status_code,
                )

        except Exception as e:
            logger.exception("[NetSuite] Employee search error: %s", e)
        
        return None