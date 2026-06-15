"""Scenarios 03/04 (per-deal serialization) and 08 (crash-safe lock).

See docs/failure-scenarios/03, 04, and 08.
"""

import time

import pytest

import locks
import sqs_processor as sp


# --- Lock behavior (scenarios 03 & 08) -----------------------------------------------------

def test_acquire_then_release(fake_lock_table):
    with locks.deal_lock("deal-1"):
        assert "deal-1" in fake_lock_table.items
    assert "deal-1" not in fake_lock_table.items


def test_contention_raises_lock_not_acquired(fake_lock_table):
    with locks.deal_lock("deal-1"):
        with pytest.raises(locks.LockNotAcquired):
            with locks.deal_lock("deal-1"):  # second holder for same deal
                pass


def test_different_deals_do_not_block(fake_lock_table):
    with locks.deal_lock("deal-1"):
        with locks.deal_lock("deal-2"):  # different key -> no contention
            assert {"deal-1", "deal-2"} <= set(fake_lock_table.items)


def test_expired_lock_is_reacquirable(fake_lock_table):
    fake_lock_table.items["deal-1"] = {"deal_id": "deal-1", "owner": "old", "expiresAt": int(time.time()) - 1}
    with locks.deal_lock("deal-1"):
        assert fake_lock_table.items["deal-1"]["owner"] != "old"  # TTL let us take it


def test_release_does_not_steal_a_newer_lock(fake_lock_table):
    # Worker B currently owns the lock; a late Worker A must not delete it.
    fake_lock_table.items["deal-1"] = {"deal_id": "deal-1", "owner": "B", "expiresAt": int(time.time()) + 960}
    locks._release("deal-1", "A")
    assert fake_lock_table.items["deal-1"]["owner"] == "B"


# --- Lock-key resolution (scenario 04: parent/child share one key) -------------------------

def test_deal_event_locks_on_its_own_id():
    assert sp._resolve_lock_key({"objectId": "123", "subscriptionType": "deal.propertyChange"}) == "123"


def test_line_item_and_deal_resolve_to_same_key(monkeypatch):
    monkeypatch.setattr(sp.hubspot, "get_line_item_deal_id", lambda _id: "123")
    deal_key = sp._resolve_lock_key({"objectId": "123", "subscriptionType": "deal.propertyChange"})
    line_key = sp._resolve_lock_key({"objectId": "999", "subscriptionType": "line_item.propertyChange"})
    assert deal_key == line_key == "123"  # => mutually exclusive, no concurrent invoice write


def test_payment_event_resolves_to_parent_deal(monkeypatch):
    monkeypatch.setattr(sp.hubspot, "get_payment_deal_id", lambda _id: "123")
    key = sp._resolve_lock_key(
        {"objectId": "55", "subscriptionType": "object.creation", "objectTypeId": sp.HUBSPOT_OBJECT_TYPE_PAYMENT}
    )
    assert key == "123"


def test_venue_event_uses_venue_scoped_key():
    assert sp._resolve_lock_key({"objectId": "77", "subscriptionType": "venue.creation"}) == "venue:77"
