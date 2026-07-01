"""Deal stage gates: create-only vs update-only invoice sync."""

from __future__ import annotations

import importlib


def _reload_stage_config(monkeypatch, *, create: str, update: str):
    monkeypatch.setenv("HUBSPOT_DEAL_STAGE_CREATE_ID", create)
    monkeypatch.setenv("HUBSPOT_DEAL_STAGE_UPDATE_ID", update)
    import config

    importlib.reload(config)
    import sqs_processor

    importlib.reload(sqs_processor)
    return sqs_processor


def _run_gate(
    sp,
    *,
    deal_id: str,
    dealstage: str,
    prior_netsuite_invoice: str,
    netsuite_invoice_id=None,
):
    hubspot_deal = {
        "properties": {
            "dealstage": dealstage,
            "prior_netsuite_invoice": prior_netsuite_invoice,
        }
    }
    return sp._deal_invoice_gate(deal_id, hubspot_deal, netsuite_invoice_id)


def test_create_stage_requires_prior_netsuite_invoice_no(monkeypatch, no_sleep):
    sp = _reload_stage_config(
        monkeypatch,
        create="1059843169",
        update="1059843170",
    )
    deal_updates: list[dict] = []

    monkeypatch.setattr(
        sp,
        "_set_deal_invoice_status",
        lambda deal_id, status: deal_updates.append({"deal_id": deal_id, "status": status}),
    )

    assert (
        _run_gate(
            sp,
            deal_id="deal-1",
            dealstage="1059843169",
            prior_netsuite_invoice="yes",
        )
        is False
    )
    assert "prior_netsuite_invoice" in deal_updates[0]["status"]


def test_create_stage_allows_when_prior_netsuite_invoice_is_no(monkeypatch, no_sleep):
    sp = _reload_stage_config(
        monkeypatch,
        create="1059843169",
        update="1059843170",
    )

    assert (
        _run_gate(
            sp,
            deal_id="deal-1",
            dealstage="1059843169",
            prior_netsuite_invoice="no",
        )
        is True
    )


def test_update_stage_requires_existing_invoice(monkeypatch, no_sleep):
    sp = _reload_stage_config(
        monkeypatch,
        create="1059843169",
        update="1059843170",
    )
    deal_updates: list[dict] = []

    monkeypatch.setattr(
        sp,
        "_set_deal_invoice_status",
        lambda deal_id, status: deal_updates.append({"deal_id": deal_id, "status": status}),
    )

    assert (
        _run_gate(
            sp,
            deal_id="deal-1",
            dealstage="1059843170",
            prior_netsuite_invoice="no",
        )
        is False
    )
    assert "update-only" in deal_updates[0]["status"]


def test_update_stage_requires_prior_netsuite_invoice_no(monkeypatch, no_sleep):
    sp = _reload_stage_config(
        monkeypatch,
        create="1059843169",
        update="1059843170",
    )
    deal_updates: list[dict] = []

    monkeypatch.setattr(
        sp,
        "_set_deal_invoice_status",
        lambda deal_id, status: deal_updates.append({"deal_id": deal_id, "status": status}),
    )

    assert (
        _run_gate(
            sp,
            deal_id="deal-1",
            dealstage="1059843170",
            prior_netsuite_invoice="yes",
            netsuite_invoice_id="inv-99",
        )
        is False
    )
    assert "prior_netsuite_invoice" in deal_updates[0]["status"]


def test_update_stage_allows_when_invoice_exists(monkeypatch, no_sleep):
    sp = _reload_stage_config(
        monkeypatch,
        create="1059843169",
        update="1059843170",
    )

    assert (
        _run_gate(
            sp,
            deal_id="deal-1",
            dealstage="1059843170",
            prior_netsuite_invoice="no",
            netsuite_invoice_id="inv-99",
        )
        is True
    )


def test_unknown_stage_is_skipped(monkeypatch, no_sleep):
    sp = _reload_stage_config(
        monkeypatch,
        create="1059843169",
        update="1059843170",
    )
    deal_updates: list[dict] = []

    monkeypatch.setattr(
        sp,
        "_set_deal_invoice_status",
        lambda deal_id, status: deal_updates.append({"deal_id": deal_id, "status": status}),
    )

    assert (
        _run_gate(
            sp,
            deal_id="deal-1",
            dealstage="9999999999",
            prior_netsuite_invoice="no",
        )
        is False
    )
    assert "not enabled for invoice sync" in deal_updates[0]["status"]
