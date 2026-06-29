"""State reconciliation: pending_resync coalescing on lock contention."""

import json

import locks
import sqs_processor as sp


def test_lock_busy_coalesces_without_exception(fake_lock_table, monkeypatch):
    monkeypatch.setattr(sp, "_dispatch_webhook_message", lambda _item: True)
    monkeypatch.setattr(sp, "_drain_pending_resync", lambda _k, _t: None)

    acquired, token = locks.try_acquire("deal-1")
    assert acquired

    webhook = {"objectId": "deal-1", "subscriptionType": "deal.propertyChange"}
    assert sp.process_webhook_message(webhook) is True
    assert fake_lock_table.items["deal-1"]["pending_resync"] is True

    locks.release_lock("deal-1", token)


def test_lock_busy_does_not_report_batch_failure(fake_lock_table, monkeypatch):
    acquired, token = locks.try_acquire("deal-99")
    assert acquired

    monkeypatch.setattr(sp, "_dispatch_webhook_message", lambda _item: True)
    monkeypatch.setattr(sp, "_drain_pending_resync", lambda _k, _t: None)

    resp = sp.lambda_handler(
        {
            "Records": [
                {
                    "messageId": "m-coalesced",
                    "body": json.dumps({"objectId": "deal-99", "subscriptionType": "deal.propertyChange"}),
                }
            ]
        },
        None,
    )
    assert resp["batchItemFailures"] == []
    assert fake_lock_table.items["deal-99"]["pending_resync"] is True

    locks.release_lock("deal-99", token)


def test_holder_drains_pending_resync(fake_lock_table, monkeypatch):
    reconcile_calls: list[str] = []

    monkeypatch.setattr(sp, "_reconcile_lock_key", lambda key: reconcile_calls.append(key) or True)

    acquired, token = locks.try_acquire("deal-55")
    assert acquired
    locks.mark_pending_resync("deal-55")
    locks.mark_pending_resync("deal-55")

    sp._drain_pending_resync("deal-55", token)

    assert reconcile_calls == ["deal-55"]
    assert fake_lock_table.items["deal-55"]["pending_resync"] is False


def test_consume_pending_is_atomic_for_holder_only(fake_lock_table):
    acquired_a, token_a = locks.try_acquire("deal-7")
    assert acquired_a
    locks.mark_pending_resync("deal-7")

    assert locks.consume_pending_resync("deal-7", token_a) is True
    assert locks.consume_pending_resync("deal-7", token_a) is False

    locks.mark_pending_resync("deal-7")
    assert locks.consume_pending_resync("deal-7", "wrong-token") is False
    assert fake_lock_table.items["deal-7"]["pending_resync"] is True

    locks.release_lock("deal-7", token_a)


def test_real_errors_still_report_batch_failure(monkeypatch):
    monkeypatch.setattr(
        sp,
        "process_webhook_message",
        lambda _item: (_ for _ in ()).throw(RuntimeError("netsuite down")),
    )
    resp = sp.lambda_handler(
        {
            "Records": [
                {
                    "messageId": "m-fail",
                    "body": json.dumps({"objectId": "1", "subscriptionType": "deal.propertyChange"}),
                }
            ]
        },
        None,
    )
    assert resp["batchItemFailures"] == [{"itemIdentifier": "m-fail"}]
