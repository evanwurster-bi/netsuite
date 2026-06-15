"""Scenario 02 (failures redrive instead of being dropped) + retryable-vs-permanent split.

See docs/failure-scenarios/02-failures-silently-dropped.md.
"""

import json

import sqs_processor as sp


def _record(message_id, body):
    return {"messageId": message_id, "body": json.dumps(body)}


def _event(*records):
    return {"Records": list(records)}


def test_exception_is_reported_as_batch_failure(monkeypatch):
    monkeypatch.setattr(sp, "process_webhook_message", lambda _i: (_ for _ in ()).throw(RuntimeError("transient")))
    resp = sp.lambda_handler(_event(_record("m1", {"objectId": 1, "subscriptionType": "deal.creation"})), None)
    assert resp["batchItemFailures"] == [{"itemIdentifier": "m1"}]


def test_false_is_acked_and_logged_loudly(monkeypatch, caplog):
    # A permanent business rejection (False) is ACKed immediately (no DLQ churn) but is
    # surfaced with a loud, greppable WARNING so it's visible in the integration logs.
    monkeypatch.setattr(sp, "process_webhook_message", lambda _i: False)
    with caplog.at_level("WARNING"):
        resp = sp.lambda_handler(
            _event(_record("m1", {"objectId": 1, "subscriptionType": "deal.creation"})), None
        )
    assert resp["batchItemFailures"] == []  # acked, not retried
    assert any("[rejected]" in r.getMessage() for r in caplog.records)


def test_success_is_acked(monkeypatch):
    monkeypatch.setattr(sp, "process_webhook_message", lambda _i: True)
    resp = sp.lambda_handler(_event(_record("m1", {"objectId": 1, "subscriptionType": "deal.creation"})), None)
    assert resp["batchItemFailures"] == []


def test_only_the_failing_record_is_reported(monkeypatch):
    def selective(item):
        if item["objectId"] == 2:
            raise RuntimeError("boom")
        return True

    monkeypatch.setattr(sp, "process_webhook_message", selective)
    resp = sp.lambda_handler(
        _event(
            _record("m1", {"objectId": 1, "subscriptionType": "deal.creation"}),
            _record("m2", {"objectId": 2, "subscriptionType": "deal.creation"}),
        ),
        None,
    )
    assert resp["batchItemFailures"] == [{"itemIdentifier": "m2"}]
