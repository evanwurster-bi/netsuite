"""Line-item consolidation, filters, and NetSuite SKU batch lookups."""

import sqs_processor as sp


def test_line_items_filtered_before_netsuite_lookup(monkeypatch):
    batch_calls: list[list[str]] = []

    def fake_batch(skus: list[str]):
        batch_calls.append(list(skus))
        return {sku: {"id": "ns-1"} for sku in skus}

    monkeypatch.setattr(sp.netsuite, "search_items_by_sku_batch", fake_batch)
    monkeypatch.setattr(sp, "_sleep_between_api_calls", lambda: None)

    items = [
        {
            "properties": {
                "hs_sku": "9999",
                "name": "bad sku prefix",
                "amount": 10,
                "quantity": 1,
                "price": 10,
            }
        },
        {
            "properties": {
                "hs_sku": "",
                "name": "missing sku",
                "amount": 10,
                "quantity": 1,
                "price": 10,
            }
        },
        {
            "properties": {
                "hs_sku": "3001",
                "name": "eligible",
                "amount": 25,
                "quantity": 2,
                "price": 12.5,
                "subcategory": "Food",
            }
        },
        {
            "properties": {
                "hs_sku": "3002",
                "name": "owned equipment zero",
                "amount": 0,
                "quantity": 1,
                "price": 0,
                "subcategory": "Owned Equipment",
            }
        },
    ]

    result = sp.process_deal_lineitems_change(items)
    assert result is not False
    assert batch_calls == [["3001"]]
    assert len(result) == 1
    assert result[0]["item"]["id"] == "ns-1"
    assert result[0]["amount"] == 25


def test_line_items_same_sku_consolidated_before_validation(monkeypatch):
    batch_calls: list[list[str]] = []

    def fake_batch(skus: list[str]):
        batch_calls.append(list(skus))
        return {sku: {"id": "ns-1"} for sku in skus}

    monkeypatch.setattr(sp.netsuite, "search_items_by_sku_batch", fake_batch)
    monkeypatch.setattr(sp, "_sleep_between_api_calls", lambda: None)

    items = [
        {
            "properties": {
                "hs_sku": "312345",
                "name": "line a",
                "amount": 20,
                "quantity": 2,
                "price": 10,
            }
        },
        {
            "properties": {
                "hs_sku": "312345",
                "name": "line b",
                "amount": 30,
                "quantity": 3,
                "price": 10,
            }
        },
        {
            "properties": {
                "hs_sku": "312345",
                "name": "line c",
                "amount": 10,
                "quantity": 1,
                "price": 10,
            }
        },
    ]

    result = sp.process_deal_lineitems_change(items)
    assert batch_calls == [["312345"]]
    assert len(result) == 1
    assert result[0]["quantity"] == 6
    assert result[0]["amount"] == 60
    assert result[0]["rate"] == 10


def test_line_items_batch_lookup_one_line_per_sku(monkeypatch):
    batch_calls: list[list[str]] = []

    def fake_batch(skus: list[str]):
        batch_calls.append(list(skus))
        return {
            "3001": {"id": "ns-1"},
            "3003": {"id": "ns-3"},
        }

    monkeypatch.setattr(sp.netsuite, "search_items_by_sku_batch", fake_batch)
    monkeypatch.setattr(sp, "_sleep_between_api_calls", lambda: None)

    items = [
        {
            "properties": {
                "hs_sku": "3001",
                "name": "first",
                "amount": 10,
                "quantity": 1,
                "price": 10,
            }
        },
        {
            "properties": {
                "hs_sku": "3001",
                "name": "duplicate sku",
                "amount": 20,
                "quantity": 2,
                "price": 10,
            }
        },
        {
            "properties": {
                "hs_sku": "3003",
                "name": "second",
                "amount": 5,
                "quantity": 1,
                "price": 5,
            }
        },
    ]

    result = sp.process_deal_lineitems_change(items)
    assert batch_calls == [["3001", "3003"]]
    assert len(result) == 2
    assert result[0]["item"]["id"] == "ns-1"
    assert result[0]["quantity"] == 3
    assert result[0]["amount"] == 30
    assert result[1]["item"]["id"] == "ns-3"


def test_consolidate_line_items_by_sku_sums_quantity_and_recalculates_rate():
    items = [
        {"properties": {"hs_sku": "3001", "quantity": 2, "amount": 20, "price": 10}},
        {"properties": {"hs_sku": "3001", "quantity": 3, "amount": 30, "price": 10}},
    ]

    consolidated = sp._consolidate_line_items_by_sku(items)
    assert len(consolidated) == 1
    props = consolidated[0]["properties"]
    assert props["hs_sku"] == "3001"
    assert props["quantity"] == 5
    assert props["amount"] == 50
    assert props["price"] == 10


def test_fetch_deal_line_item_details_uses_batch(monkeypatch):
    batch_calls: list[list[str]] = []

    monkeypatch.setattr(
        sp.hubspot,
        "get_deal_line_items",
        lambda deal_id: [{"id": "1"}, {"id": "2"}],
    )

    def fake_batch(line_item_ids, properties=None):
        batch_calls.append(line_item_ids)
        return [{"id": lid, "properties": {"hs_sku": "3001", "amount": 1}} for lid in line_item_ids]

    monkeypatch.setattr(sp.hubspot, "get_line_item_details_batch", fake_batch)
    monkeypatch.setattr(sp, "_sleep_between_api_calls", lambda: None)

    rows = sp._fetch_deal_line_item_details("deal-99")
    assert batch_calls == [["1", "2"]]
    assert len(rows) == 2
