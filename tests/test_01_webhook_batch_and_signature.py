"""Scenarios 01 (batched webhook truncation) and 07 (signature verification).

See docs/failure-scenarios/01-batched-webhook-truncation.md and 07-unauthorized-webhook.md.
"""

import base64
import hashlib
import hmac
import json
import time

import lambda_function as wh


def _capture_batch(monkeypatch):
    sent = []

    def fake_send(QueueUrl, Entries):
        sent.extend(Entries)
        return {"Successful": Entries, "Failed": []}

    monkeypatch.setattr(wh.sqs, "send_message_batch", fake_send)
    return sent


# --- Scenario 01: every event in a batch is enqueued ---------------------------------------

def test_all_events_in_batch_are_enqueued(monkeypatch):
    sent = _capture_batch(monkeypatch)
    body = json.dumps(
        [
            {"objectId": 1, "subscriptionType": "deal.creation"},
            {"objectId": 2, "subscriptionType": "line_item.propertyChange"},
            {"objectId": 3, "subscriptionType": "deal.propertyChange"},
        ]
    )
    resp = wh.lambda_handler({"body": body}, None)
    assert resp["statusCode"] == 204
    assert len(sent) == 3  # before the fix, only 1 would have been sent


def test_invalid_events_skipped_valid_kept(monkeypatch):
    sent = _capture_batch(monkeypatch)
    body = json.dumps(
        [
            {"objectId": 1, "subscriptionType": "deal.creation"},
            {"objectId": 2},  # missing subscriptionType -> invalid
        ]
    )
    resp = wh.lambda_handler({"body": body}, None)
    assert resp["statusCode"] == 204
    assert len(sent) == 1


def test_partial_enqueue_failure_returns_500(monkeypatch):
    monkeypatch.setattr(
        wh.sqs,
        "send_message_batch",
        lambda QueueUrl, Entries: {"Successful": Entries[:1], "Failed": [{"Id": "1"}]},
    )
    body = json.dumps(
        [
            {"objectId": 1, "subscriptionType": "deal.creation"},
            {"objectId": 2, "subscriptionType": "deal.creation"},
        ]
    )
    resp = wh.lambda_handler({"body": body}, None)
    assert resp["statusCode"] == 500  # HubSpot will resend the whole delivery


def test_missing_queue_url_fails_loud(monkeypatch):
    monkeypatch.setattr(wh, "SQS_QUEUE_URL", "")
    resp = wh.lambda_handler(
        {"body": json.dumps({"objectId": 1, "subscriptionType": "deal.creation"})}, None
    )
    assert resp["statusCode"] == 500


# --- Scenario 07: signature verification ---------------------------------------------------

def _signed_event(body, ts, secret, host="api.example.com", path="/hubspot/webhook"):
    base = "POST" + f"https://{host}{path}" + body + str(ts)
    sig = base64.b64encode(hmac.new(secret.encode(), base.encode(), hashlib.sha256).digest()).decode()
    return {
        "body": body,
        "rawPath": path,
        "rawQueryString": "",
        "isBase64Encoded": False,
        "requestContext": {"http": {"method": "POST", "path": path}},
        "headers": {
            "host": host,
            "x-forwarded-proto": "https",
            "x-hubspot-signature-v3": sig,
            "x-hubspot-request-timestamp": str(ts),
        },
    }


def test_valid_signature_accepted(monkeypatch):
    monkeypatch.setattr(wh, "HUBSPOT_CLIENT_SECRET", "s3cret")
    ev = _signed_event('[{"objectId":1,"subscriptionType":"deal.creation"}]', int(time.time() * 1000), "s3cret")
    ok, reason = wh._verify_signature(ev)
    assert ok, reason


def test_tampered_body_rejected(monkeypatch):
    monkeypatch.setattr(wh, "HUBSPOT_CLIENT_SECRET", "s3cret")
    ev = _signed_event('[{"objectId":1}]', int(time.time() * 1000), "s3cret")
    ev["body"] = '[{"objectId":2}]'  # tamper after signing
    ok, reason = wh._verify_signature(ev)
    assert not ok and reason == "signature mismatch"


def test_stale_timestamp_rejected(monkeypatch):
    monkeypatch.setattr(wh, "HUBSPOT_CLIENT_SECRET", "s3cret")
    ev = _signed_event("[]", int(time.time() * 1000) - 10 * 60 * 1000, "s3cret")
    ok, reason = wh._verify_signature(ev)
    assert not ok and "window" in reason


def test_missing_secret_fails_closed(monkeypatch):
    monkeypatch.setattr(wh, "HUBSPOT_CLIENT_SECRET", "")
    ev = _signed_event("[]", int(time.time() * 1000), "whatever")
    ok, reason = wh._verify_signature(ev)
    assert not ok and "unset" in reason


def test_handler_returns_401_when_enabled_and_unsigned(monkeypatch):
    monkeypatch.setattr(wh, "HUBSPOT_SIGNATURE_VERIFICATION_ENABLED", True)
    monkeypatch.setattr(wh, "HUBSPOT_CLIENT_SECRET", "s3cret")
    resp = wh.lambda_handler({"body": "[]", "headers": {}}, None)
    assert resp["statusCode"] == 401
