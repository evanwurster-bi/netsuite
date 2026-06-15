"""Scenario 05 (NetSuite write succeeds, HubSpot writeback fails -> retryable, idempotent).

See docs/failure-scenarios/05-partial-write-hubspot-writeback-fails.md.

These fakes implement just the methods ``reconcile_deal_invoice`` calls, on a happy path, so
we can drive it to the final HubSpot writeback and assert what happens when steps fail.
"""

import pytest

import sqs_processor as sp


class FakeHubSpot:
    def __init__(self):
        self.raise_on_writeback = False
        self.writeback_calls = 0

    def get_deal_billing_contact_or_default_contact(self, deal_id):
        return {"properties": {"email": "buyer@example.com"}}

    def get_deal(self, deal_id, properties=None):
        return {
            "id": deal_id,
            "properties": {
                sp.DEAL_VENUE_NAME_PROPERTY: "Grand Hall",
                "dealname": "Test Deal",
                "netsuite_invoice_number": "",  # no prior number -> "Invoice created"
            },
        }

    def get_deal_line_items(self, deal_id):
        return [{"id": "li-1"}]

    def get_line_item_detail(self, line_item_id, properties=None):
        return {
            "id": line_item_id,
            "properties": {
                "hs_sku": "3001",  # starts with 3 -> kept
                "quantity": "2",
                "amount": "100",
                "price": "50",
                "name": "Catering",
                "subcategory": "",
            },
        }

    def get_venue_by_name(self, venue_name, properties=None):
        return {"id": "hs-venue-1", "properties": {"name": venue_name}}

    def map_to_netsuite_format(self, deal, customer_id, subsidiary_id, venue_id, lines):
        return {"externalId": str(deal.get("id")), "item": {"items": lines}}

    def update_deal_properties(self, deal_id, props):
        self.writeback_calls += 1
        if self.raise_on_writeback:
            raise RuntimeError("HubSpot 502 during writeback")
        return {}


class FakeNetSuite:
    def __init__(self):
        self.invoice_exists = False
        self.upsert_ok = True
        self.create_calls = 0

    def get_customer_by_email(self, email):
        return {"id": "cust-1"}

    def get_customer_subsidiaries(self, customer_id):
        return [{"subsidiary": "1"}]  # matches NETSUITE_SUBSIDIARY_ID in conftest

    def get_invoice_by_deal_id(self, deal_id):
        return "inv-1" if self.invoice_exists else None

    def get_or_create_venue(self, name, external_id):
        return {"id": "loc-1"}

    def search_item_by_sku(self, sku):
        return {"id": "item-1"}

    def create_or_update_invoice(self, invoice, invoice_id=None):
        self.create_calls += 1
        self.invoice_exists = True  # NetSuite committed
        return self.upsert_ok

    def get_invoice_number(self, invoice_id):
        return "INV-1001"


@pytest.fixture
def wired(monkeypatch, no_sleep):
    fake_hs = FakeHubSpot()
    fake_ns = FakeNetSuite()
    monkeypatch.setattr(sp, "hubspot", fake_hs)
    monkeypatch.setattr(sp, "netsuite", fake_ns)
    return fake_hs, fake_ns


def test_happy_path_returns_true(wired):
    fake_hs, fake_ns = wired
    assert sp.reconcile_deal_invoice("123") is True
    assert fake_ns.create_calls == 1
    assert fake_hs.writeback_calls == 1


def test_writeback_failure_raises_so_it_retries(wired):
    fake_hs, fake_ns = wired
    fake_hs.raise_on_writeback = True
    with pytest.raises(RuntimeError):
        sp.reconcile_deal_invoice("123")
    # NetSuite invoice WAS committed; recovery is a redrive, not a rollback.
    assert fake_ns.invoice_exists is True


def test_retry_after_writeback_failure_does_not_duplicate(wired):
    fake_hs, fake_ns = wired
    fake_hs.raise_on_writeback = True
    with pytest.raises(RuntimeError):
        sp.reconcile_deal_invoice("123")  # attempt 1: invoice created, writeback fails

    fake_hs.raise_on_writeback = False
    assert sp.reconcile_deal_invoice("123") is True  # attempt 2: succeeds
    # Second pass found the existing invoice (update path) -> only the recovered writeback,
    # never a duplicate create beyond the idempotent upsert.
    assert fake_ns.invoice_exists is True


def test_failed_upsert_raises_before_stamping(wired):
    fake_hs, fake_ns = wired
    fake_ns.upsert_ok = False
    with pytest.raises(RuntimeError):
        sp.reconcile_deal_invoice("123")
    assert fake_hs.writeback_calls == 0  # never stamped "Invoice created" on a failed upsert
