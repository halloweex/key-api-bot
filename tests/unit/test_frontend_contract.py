"""The contract the Rust ports are held to, against the frontend's code.

`tests/frontend_contract.py` reads what a page can send out of the TypeScript;
the harnesses send exactly that. This file writes down what it reads today.
When the frontend starts sending something new — a parameter, a period, a
sales type, an endpoint — the reading changes, this fails, and whoever changed
the page has to teach the harness (and the port) the new input before it can
pass: a port is never proven on requests nobody makes while the real ones go
unasked.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests import frontend_contract as fc

REPO = Path(__file__).resolve().parents[2]

MARGIN_PARAMS = (
    "period", "start_date", "end_date", "sales_type",
    "source_id", "category_id", "brand", "promocode",
)
MARGIN_ENDPOINTS = tuple(
    f"/api/margin/{name}"
    for name in ("overview", "by-brand", "by-category", "trend", "brand-category", "alerts")
)


def test_what_the_margin_page_sends_is_what_its_harness_sends():
    c = fc.margin()
    assert c.params == MARGIN_PARAMS, (
        "the /margin page sends a different set of parameters now: teach "
        "tests/integration/test_rust_wave1_margin.py and the Rust port first"
    )
    assert c.endpoints == MARGIN_ENDPOINTS


def test_the_value_domains_are_the_frontends_types():
    assert fc.periods() == (
        "today", "yesterday", "week", "last_week", "month", "last_month",
        "last_7_days", "last_28_days",
    )
    assert fc.sales_types() == ("retail", "b2b", "exhibition", "internal", "all")


def test_every_margin_endpoint_is_a_get_route_of_the_app():
    routes = json.loads((REPO / "rust" / "contract" / "access.json").read_text())["routes"]
    gets = {r["path"] for r in routes if r["kind"] == "api" and "GET" in (r.get("methods") or [])}
    assert set(MARGIN_ENDPOINTS) <= gets


def test_a_narrowed_page_drops_the_filters_it_does_not_apply():
    """The reader follows `BY_PATH`: /traffic sends three of the six."""
    traffic = fc.contract("/traffic", ())
    assert "brand" not in traffic.params and "source_id" in traffic.params


def test_a_reshaped_hook_fails_loudly():
    with pytest.raises(fc.FrontendShapeError):
        fc.hook_calls(("useNoSuchHook",))
