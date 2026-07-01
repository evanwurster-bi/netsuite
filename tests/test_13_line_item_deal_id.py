import hubspot as hs


class Resp:
    def __init__(self, payload, status_code=200):
        self.status_code = status_code
        self.headers = {}
        self.text = ""
        self.ok = 200 <= status_code < 300
        self._payload = payload

    def raise_for_status(self):
        if not self.ok:
            raise hs.requests.exceptions.HTTPError(response=self)

    def json(self):
        return self._payload


def test_parent_deal_from_v4_association_to_object_id(monkeypatch):
    client = hs.HubSpotClient(api_key="test")
    seen: list[str] = []

    def fake_get(url, headers=None, **kwargs):
        seen.append(url)
        return Resp({"results": [{"toObjectId": 456}]})

    monkeypatch.setattr(hs.requests, "get", fake_get)
    assert client.get_line_item_deal_id("line-2") == "456"
    assert len(seen) == 1
    assert seen[0] == "https://api.hubapi.com/crm/v4/objects/line_items/line-2/associations/deals"


def test_parent_deal_missing_returns_none(monkeypatch):
    client = hs.HubSpotClient(api_key="test")

    def fake_get(url, headers=None, **kwargs):
        return Resp({"results": []})

    monkeypatch.setattr(hs.requests, "get", fake_get)
    assert client.get_line_item_deal_id("line-4") is None


def test_parent_deal_404_returns_none_without_raising(monkeypatch):
    client = hs.HubSpotClient(api_key="test")

    def fake_get(url, headers=None, **kwargs):
        return Resp({}, status_code=404)

    monkeypatch.setattr(hs.requests, "get", fake_get)
    assert client.get_line_item_deal_id("56554938385") is None
