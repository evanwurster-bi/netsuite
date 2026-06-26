"""HubSpot HTTP retries for rate limiting (mirrors NetSuite make_request)."""

import hubspot as hs
import requests


def test_retry_after_numeric():
    assert hs._parse_retry_after("3") == 3.0


def test_retry_after_http_date_ignored():
    assert hs._parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") is None


def test_retry_after_clamped_to_60():
    assert hs._parse_retry_after("9999") == 60.0


def test_429_retries_then_succeeds(monkeypatch):
    client = hs.HubSpotClient(api_key="test")
    calls = {"n": 0}
    sleeps: list[float] = []

    class Resp:
        def __init__(self, status_code, headers=None, text=""):
            self.status_code = status_code
            self.headers = headers or {}
            self.text = text
            self.ok = 200 <= status_code < 300

        def raise_for_status(self):
            if not self.ok:
                raise requests.exceptions.HTTPError(response=self)

        def json(self):
            return {"id": "deal-1"}

    def fake_get(url, headers=None, **kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            return Resp(429, headers={"Retry-After": "0"})
        return Resp(200)

    monkeypatch.setattr(hs.requests, "get", fake_get)
    monkeypatch.setattr(hs.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = client.get_deal("deal-1")
    assert result == {"id": "deal-1"}
    assert calls["n"] == 3
    assert sleeps == [0.0, 0.0]


def test_429_exhausts_retries(monkeypatch):
    client = hs.HubSpotClient(api_key="test")

    class Resp:
        status_code = 429
        headers = {}
        text = "rate limit"
        ok = False

        def raise_for_status(self):
            raise requests.exceptions.HTTPError(response=self)

    monkeypatch.setattr(hs.requests, "get", lambda *a, **k: Resp())
    monkeypatch.setattr(hs.time, "sleep", lambda _seconds: None)

    try:
        client.get_deal("deal-1")
        assert False, "should have raised"
    except requests.exceptions.HTTPError:
        pass
