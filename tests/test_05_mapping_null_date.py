"""Scenario 09 (malformed/missing data does not crash the sync).

See docs/failure-scenarios/09-malformed-data-poison-message.md.
"""

import hubspot


def _deal(props):
    return {"id": "1", "properties": props}


def test_missing_event_date_omits_trandate():
    c = hubspot.HubSpotClient()
    inv = c.map_to_netsuite_format(_deal({}), "100", "1", "200", [])
    assert "tranDate" not in inv  # no crash, just omitted


def test_malformed_event_date_omits_trandate():
    c = hubspot.HubSpotClient()
    inv = c.map_to_netsuite_format(_deal({"event_start_date_and_time": "not-a-date"}), "100", "1", "200", [])
    assert "tranDate" not in inv


def test_valid_event_date_sets_trandate():
    c = hubspot.HubSpotClient()
    inv = c.map_to_netsuite_format(
        _deal({"event_start_date_and_time": "2025-08-01T12:30:00Z"}), "100", "1", "200", []
    )
    assert inv["tranDate"] == "2025-08-01"
