"""Scenario 06 (token cache + Retry-After) and 03 (externalId collision recovery).

See docs/failure-scenarios/06-netsuite-rate-limit-and-token.md and 03-duplicate-invoice-concurrent-create.md.
"""

import requests

import netsuite_auth as na


# --- Retry-After parsing (scenario 06) -----------------------------------------------------

def test_retry_after_numeric():
    assert na._parse_retry_after("3") == 3.0


def test_retry_after_http_date_ignored():
    assert na._parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") is None


def test_retry_after_clamped_to_60():
    assert na._parse_retry_after("9999") == 60.0


def test_retry_after_none():
    assert na._parse_retry_after(None) is None


# --- Token caching (scenario 06) -----------------------------------------------------------

def test_token_is_cached_across_calls(monkeypatch):
    auth = na.NetSuiteAuth()
    monkeypatch.setattr(auth, "_generate_jwt", lambda: "jwt")
    calls = {"n": 0}

    class Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"access_token": "tok", "expires_in": 3600}

    def fake_post(url, data=None, headers=None):
        calls["n"] += 1
        return Resp()

    monkeypatch.setattr(na.requests, "post", fake_post)
    assert auth.get_access_token() == "tok"
    assert auth.get_access_token() == "tok"
    assert calls["n"] == 1  # second call served from cache


def test_expired_token_refreshes(monkeypatch):
    auth = na.NetSuiteAuth()
    monkeypatch.setattr(auth, "_generate_jwt", lambda: "jwt")
    calls = {"n": 0}

    class Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"access_token": "tok", "expires_in": 3600}

    monkeypatch.setattr(na.requests, "post", lambda *a, **k: (calls.update(n=calls["n"] + 1), Resp())[1])
    auth.get_access_token()
    auth._access_token_expiry = 0.0  # force expiry
    auth.get_access_token()
    assert calls["n"] == 2


# --- externalId collision recovery (scenario 03) -------------------------------------------

def test_create_returns_location_id(monkeypatch):
    # The new record id comes from the strongly-consistent Location header, not a search.
    auth = na.NetSuiteAuth()

    class Resp:
        status_code = 204
        headers = {
            "Location": "https://acct.suitetalk.api.netsuite.com/services/rest/record/v1/invoice/98765"
        }

    monkeypatch.setattr(auth, "make_request", lambda *a, **k: Resp())
    assert auth.create_or_update_invoice({"externalId": "123"}) == "98765"


def test_update_returns_known_id(monkeypatch):
    auth = na.NetSuiteAuth()

    class Resp:
        status_code = 204
        headers = {}

    monkeypatch.setattr(auth, "make_request", lambda *a, **k: Resp())
    assert auth.create_or_update_invoice({"externalId": "123"}, invoice_id="42") == "42"


def test_create_collision_recovers_with_patch(monkeypatch):
    auth = na.NetSuiteAuth()
    seq = []

    class Resp:
        def __init__(self, code):
            self.status_code = code
            self.headers = {}

    def fake_make_request(method, endpoint, data=None, additional_headers=None, params=None):
        seq.append((method, endpoint))
        if method == "POST":
            raise requests.exceptions.HTTPError("externalId already in use")
        return Resp(204)  # the recovery PATCH

    monkeypatch.setattr(auth, "make_request", fake_make_request)
    monkeypatch.setattr(auth, "get_invoice_by_deal_id", lambda ext: "555")

    result_id = auth.create_or_update_invoice({"externalId": "123"})
    assert result_id == "555"  # recovered: returns the existing invoice id
    assert ("POST", "record/v1/invoice") in seq
    assert ("PATCH", "record/v1/invoice/555") in seq  # patched instead of duplicating


def test_create_collision_reraises_if_no_existing(monkeypatch):
    auth = na.NetSuiteAuth()

    def fake_make_request(method, endpoint, data=None, additional_headers=None, params=None):
        raise requests.exceptions.HTTPError("real error")

    monkeypatch.setattr(auth, "make_request", fake_make_request)
    monkeypatch.setattr(auth, "get_invoice_by_deal_id", lambda ext: None)

    try:
        auth.create_or_update_invoice({"externalId": "123"})
        assert False, "should have re-raised"
    except requests.exceptions.HTTPError:
        pass


def test_id_from_location_parsing():
    class Resp:
        headers = {"Location": "https://x/services/rest/record/v1/invoice/12345/"}

    assert na._id_from_location(Resp()) == "12345"
    assert na._id_from_location(type("R", (), {"headers": {}})()) is None
