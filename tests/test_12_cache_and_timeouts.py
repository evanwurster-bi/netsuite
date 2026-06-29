"""Speed work: TTL cache, cached subsidiary/sales-rep lookups, and request timeouts."""

import time

import cache
import hubspot as hs
import netsuite_auth as na
import sqs_processor as sp


# --- TTLCache --------------------------------------------------------------------------------

def test_ttlcache_hit_and_miss():
    c = cache.TTLCache(ttl_seconds=60)
    assert c.get("k") is None
    c.set("k", "v")
    assert c.get("k") == "v"


def test_ttlcache_expires():
    c = cache.TTLCache(ttl_seconds=-1)  # already expired
    c.set("k", "v")
    assert c.get("k") is None


def test_get_or_compute_caches_and_skips_recompute():
    c = cache.TTLCache(ttl_seconds=60)
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return "v"

    assert c.get_or_compute("k", compute) == "v"
    assert c.get_or_compute("k", compute) == "v"
    assert calls["n"] == 1  # second call served from cache


def test_get_or_compute_does_not_cache_none():
    c = cache.TTLCache(ttl_seconds=60)
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return None

    c.get_or_compute("k", compute)
    c.get_or_compute("k", compute)
    assert calls["n"] == 2  # None is never cached -> recomputed


# --- sales-rep resolution is cached by owner id ---------------------------------------------

def test_resolve_sales_rep_id_maps_owner_to_employee(monkeypatch):
    monkeypatch.setattr(sp.hubspot, "get_owner_by_id", lambda oid: {"email": "rep@x.com"})
    monkeypatch.setattr(sp.netsuite, "search_employee_by_email", lambda email: {"id": "emp-9"})
    assert sp._resolve_sales_rep_id("owner-1") == "emp-9"


def test_sales_rep_cache_skips_repeat_lookups(monkeypatch):
    sp._SALES_REP_CACHE._store.clear()
    owner_calls = {"n": 0}

    def get_owner(_oid):
        owner_calls["n"] += 1
        return {"email": "rep@x.com"}

    monkeypatch.setattr(sp.hubspot, "get_owner_by_id", get_owner)
    monkeypatch.setattr(sp.netsuite, "search_employee_by_email", lambda email: {"id": "emp-9"})

    first = sp._SALES_REP_CACHE.get_or_compute("owner-1", lambda: sp._resolve_sales_rep_id("owner-1"))
    second = sp._SALES_REP_CACHE.get_or_compute("owner-1", lambda: sp._resolve_sales_rep_id("owner-1"))
    assert first == second == "emp-9"
    assert owner_calls["n"] == 1  # only resolved once


# --- request timeouts are applied -----------------------------------------------------------

def test_netsuite_get_passes_timeout(monkeypatch):
    auth = na.NetSuiteAuth()
    monkeypatch.setattr(auth, "get_access_token", lambda: "tok")
    seen = {}

    class Resp:
        status_code = 200
        ok = True
        elapsed = None

        def raise_for_status(self):
            pass

    def fake_get(url, headers=None, params=None, timeout=None):
        seen["timeout"] = timeout
        return Resp()

    monkeypatch.setattr(na.requests, "get", fake_get)
    auth.make_request("GET", "record/v1/invoice", {"q": "x"})
    assert seen["timeout"] == na._HTTP_TIMEOUT  # not None / not 1000


def test_hubspot_request_passes_timeout(monkeypatch):
    client = hs.HubSpotClient(api_key="k")
    seen = {}

    class Resp:
        status_code = 200
        ok = True
        elapsed = None

        def raise_for_status(self):
            pass

    def fake_get(url, headers=None, timeout=None, **kwargs):
        seen["timeout"] = timeout
        return Resp()

    monkeypatch.setattr(hs.requests, "get", fake_get)
    client._request("GET", "https://api.hubapi.com/crm/v3/objects/deals/1")
    assert seen["timeout"] == hs._HTTP_TIMEOUT
