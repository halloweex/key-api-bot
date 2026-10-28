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


PRODUCTS_ENDPOINTS = tuple(
    f"/api/products/intel/{name}"
    for name in ("summary", "pairs", "basket-distribution", "category-combos",
                 "brand-affinity", "momentum")
)


def test_what_the_products_page_sends_is_what_its_harness_sends():
    c = fc.products()
    assert c.params == MARGIN_PARAMS, (
        "the /products page sends a different set of parameters now: teach "
        "tests/integration/test_rust_wave1_products.py and the Rust port first"
    )
    assert c.endpoints == PRODUCTS_ENDPOINTS
    # `useProductPairs` adds the product a pair names, and nothing else does.
    assert c.extras == {"/api/products/intel/pairs": ("product_id",)}
    assert fc.query(c, period="week", sales_type="retail", brand="X",
                    endpoint="/api/products/intel/pairs", extra={"product_id": 42}) == (
        "period=week&sales_type=retail&brand=X&product_id=42")
    assert fc.query(c, period="week", sales_type="retail",
                    endpoint="/api/products/intel/pairs", extra={"product_id": None}) == (
        "period=week&sales_type=retail")
    with pytest.raises(fc.FrontendShapeError):
        fc.query(c, period="week", sales_type="retail",
                 endpoint="/api/products/intel/summary", extra={"product_id": 42})


def test_the_value_domains_are_the_frontends_types():
    assert fc.periods() == (
        "today", "yesterday", "week", "last_week", "month", "last_month",
        "last_7_days", "last_28_days",
    )
    assert fc.sales_types() == ("retail", "b2b", "exhibition", "internal", "all")
    assert fc.sources() == (1, 2, 4)


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


def test_the_query_is_built_as_use_query_params_builds_it():
    c = fc.margin()
    assert fc.query(c, period="month", sales_type="retail") == "period=month&sales_type=retail"
    # `custom` sends the two dates, or neither while one is missing.
    assert fc.query(c, period="custom", start_date="2026-10-01", end_date="2026-10-21",
                    sales_type="all") == "start_date=2026-10-01&end_date=2026-10-21&sales_type=all"
    assert fc.query(c, period="custom", start_date="2026-10-01", sales_type="b2b") == "sales_type=b2b"
    # A filter is sent only when truthy, in the store's order.
    assert fc.query(c, period="week", sales_type="retail", source_id=0, brand="",
                    promocode="SALE", category_id=12) == (
        "period=week&sales_type=retail&category_id=12&promocode=SALE")
    # A filter the page does not apply is left out, as `filtersForPath` does.
    assert fc.query(fc.contract("/traffic", ()), period="week", sales_type="retail",
                    brand="X", source_id=4) == "period=week&sales_type=retail&source_id=4"


def test_the_encoding_is_url_search_params():
    """WHATWG's serializer, which `urlencode` is not: `~` and `*` differ."""
    assert fc.encode([("brand", "Етуде Хаус"), ("x", "a*b~c/d&e=f+g")]) == (
        "brand=%D0%95%D1%82%D1%83%D0%B4%D0%B5+%D0%A5%D0%B0%D1%83%D1%81"
        "&x=a*b%7Ec%2Fd%26e%3Df%2Bg")
