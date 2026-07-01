"""Scenarios 03/04 (per-deal serialization) and 08 (crash-safe lock).

See docs/failure-scenarios/03, 04, and 08.
"""

import time
from contextlib import contextmanager

import locks
import sqs_processor as sp


@contextmanager
def _with_lock(key: str):
    acquired, token = locks.try_acquire(key)
    assert acquired, f"lock not acquired: {key}"
    try:
        yield
    finally:
        locks.release_lock(key, token)


# --- Lock behavior (scenarios 03 & 08) -----------------------------------------------------

def test_acquire_then_release(fake_lock_table):
    with _with_lock("deal-1"):
        assert "deal-1" in fake_lock_table.items
    assert "deal-1" not in fake_lock_table.items


def test_contention_marks_pending_resync(fake_lock_table):
    acquired, token = locks.try_acquire("deal-1")
    assert acquired
    assert not locks.try_acquire("deal-1")[0]
    locks.mark_pending_resync("deal-1")
    assert fake_lock_table.items["deal-1"]["pending_resync"] is True
    locks.release_lock("deal-1", token)


def test_different_deals_do_not_block(fake_lock_table):
    with _with_lock("deal-1"):
        with _with_lock("deal-2"):
            assert {"deal-1", "deal-2"} <= set(fake_lock_table.items)


def test_expired_lock_is_reacquirable(fake_lock_table):
    fake_lock_table.items["deal-1"] = {"deal_id": "deal-1", "owner": "old", "expiresAt": int(time.time()) - 1}
    with _with_lock("deal-1"):
        assert fake_lock_table.items["deal-1"]["owner"] != "old"


def test_release_does_not_steal_a_newer_lock(fake_lock_table):
    fake_lock_table.items["deal-1"] = {"deal_id": "deal-1", "owner": "B", "expiresAt": int(time.time()) + 960}
    locks._release("deal-1", "A")
    assert fake_lock_table.items["deal-1"]["owner"] == "B"


def test_release_aliases_reserved_keyword_owner(monkeypatch):
    captured = {}

    class CapturingTable:
        def delete_item(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(locks, "_table", CapturingTable())
    locks._release("deal-1", "tok")
    assert "#owner" in captured["ConditionExpression"]
    assert captured["ExpressionAttributeNames"] == {"#owner": "owner"}


# --- Lock-key resolution (scenario 04: parent/child share one key) -------------------------

def test_deal_event_locks_on_its_own_id():
    assert sp._resolve_lock_key({"objectId": "123", "subscriptionType": "deal.propertyChange"}) == "123"


def test_line_item_and_deal_resolve_to_same_key(monkeypatch):
    monkeypatch.setattr(sp.hubspot, "get_line_item_deal_id", lambda _id: "123")
    deal_key = sp._resolve_lock_key({"objectId": "123", "subscriptionType": "deal.propertyChange"})
    line_key = sp._resolve_lock_key({"objectId": "999", "subscriptionType": "line_item.propertyChange"})
    assert deal_key == line_key == "123"


def test_venue_event_uses_venue_scoped_key():
    assert sp._resolve_lock_key({"objectId": "77", "subscriptionType": "venue.creation"}) == "venue:77"
