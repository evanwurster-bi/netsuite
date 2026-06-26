"""HubSpot venue lookup uses filtered CRM search instead of scanning the full catalog."""

from __future__ import annotations

from hubspot import HubSpotClient, _venue_exact_name_filter_groups, _venue_token_name_filter_groups


class RecordingHubSpotClient(HubSpotClient):
    def __init__(self):
        self.api_key = "test-key"
        self.default_api_version = "v3"
        self.headers = {}
        self.search_calls: list[dict] = []

    def _post_venue_search(self, body, api_version=None):
        self.search_calls.append(body)
        filter_groups = body.get("filterGroups", [])
        if filter_groups == _venue_exact_name_filter_groups("Grand Hall"):
            return {
                "results": [
                    {
                        "id": "venue-1",
                        "properties": {"name": "Grand Hall", "city": "Austin"},
                    }
                ]
            }
        if filter_groups == _venue_exact_name_filter_groups("grand hall"):
            return {"results": []}
        if filter_groups == _venue_token_name_filter_groups("grand hall"):
            return {
                "results": [
                    {
                        "id": "venue-1",
                        "properties": {"name": "Grand Hall", "city": "Austin"},
                    }
                ]
            }
        return {"results": []}


def test_exact_name_search_uses_eq_filters_and_returns_match():
    client = RecordingHubSpotClient()
    venue = client.get_venue_by_name(
        "Grand Hall",
        properties=["address", "city", "state", "zip_code"],
    )

    assert venue is not None
    assert venue["id"] == "venue-1"
    assert len(client.search_calls) == 1
    assert client.search_calls[0]["filterGroups"] == _venue_exact_name_filter_groups("Grand Hall")
    assert "address" in client.search_calls[0]["properties"]


def test_case_mismatch_falls_back_to_token_search():
    client = RecordingHubSpotClient()
    venue = client.get_venue_by_name("grand hall")

    assert venue is not None
    assert venue["id"] == "venue-1"
    assert len(client.search_calls) == 2
    assert client.search_calls[0]["filterGroups"] == _venue_exact_name_filter_groups("grand hall")
    assert client.search_calls[1]["filterGroups"] == _venue_token_name_filter_groups("grand hall")


def test_empty_name_skips_search():
    client = RecordingHubSpotClient()
    assert client.get_venue_by_name("") is None
    assert client.get_venue_by_name("   ") is None
    assert client.search_calls == []
