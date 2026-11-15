"""The offline store: retail, but offline, and a category of its own (the
owner's decision of 2026-11-15).

KeyCRM's source 7, «Офлайн Магазин», is the shop's physical store, opened in
November 2026. Its seller is a manager on no retail list, so until this
change its orders were `internal` by the manager rule, outside revenue
(source 7 was in no revenue list), compared by no reconciliation arm, refused
400 by every dashboard filter, and a standing WARN on the integrity layer
(`pg_value_domain_orders_source_id`). What is held here, without a store:

* the constants — 7 synced, revenue, and the `offline` sales type known;
* Silver's CASE — source 7 is `offline` whoever the manager is, executed in
  DuckDB in memory over the rendering's own text;
* the source lists that read `SYNCED_SOURCE_IDS` rather than restate it;
* the API — `source_id=7` and `sales_type=offline` validated, `offline` not
  gated to admins, the exhibition and Opencart unmoved (decision 5);
* /traffic — untagged offline-store orders are organic, on a platform and a
  channel of their own, never the website's "source not identified";
* the labels in three languages, and the bot naming the store.

The Postgres half — Silver, Gold, `/api/summary`, the integrity check — is
`tests/integration/test_offline_store.py`.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from core import duckdb_constants as constants
from core.duckdb_constants import (
    EXHIBITION_SOURCE_ID, KNOWN_SALES_TYPES, OFFLINE_STORE_SOURCE_ID,
    REVENUE_SOURCE_IDS, SYNCED_SOURCE_IDS,
)

ROOT = Path(__file__).resolve().parents[2]
STORE = OFFLINE_STORE_SOURCE_ID


class TestTheConstants:
    def test_the_store_is_source_seven(self):
        """KeyCRM's id for «Офлайн Магазин», read from its source dictionary
        on 2026-11-15."""
        assert STORE == 7

    def test_it_is_synced_and_revenue_and_offline_is_a_sales_type(self):
        assert STORE in SYNCED_SOURCE_IDS
        assert STORE in REVENUE_SOURCE_IDS
        assert "offline" in KNOWN_SALES_TYPES

    def test_nothing_else_moved(self):
        """Opencart stays synced and not revenue; the exhibition stays both."""
        assert 3 in SYNCED_SOURCE_IDS and 3 not in REVENUE_SOURCE_IDS
        assert EXHIBITION_SOURCE_ID in REVENUE_SOURCE_IDS
        assert KNOWN_SALES_TYPES[:4] == ("retail", "b2b", "internal", "exhibition")


class TestTheSourceListsAgree:
    """`SYNCED_SOURCE_IDS` is what we pull, reconcile and know. The integrity
    layer's `KNOWN_SOURCE_IDS` and the reconciliation's `ACTIVE_SOURCES` were
    literals of their own, so registering source 7 meant finding both — the
    WARN stood until somebody did."""

    def test_the_integrity_layer_knows_what_we_sync(self):
        from core.pg_warehouse_dq import KNOWN_SOURCE_IDS

        assert KNOWN_SOURCE_IDS == frozenset(SYNCED_SOURCE_IDS)

    def test_the_reconciliation_compares_what_we_sync(self):
        from core.reconciliation_io import ACTIVE_SOURCES

        assert tuple(ACTIVE_SOURCES) == tuple(SYNCED_SOURCE_IDS)

    @pytest.mark.parametrize("module, name", [
        ("core/pg_warehouse_dq.py", "KNOWN_SOURCE_IDS"),
        ("core/reconciliation_io.py", "ACTIVE_SOURCES"),
    ])
    def test_neither_is_a_literal_again(self, module, name):
        """Parsed, not grepped: the assignment's value names no number."""
        tree = ast.parse((ROOT / module).read_text(encoding="utf-8"))
        (value,) = [
            node.value for node in tree.body
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == name for t in node.targets)
        ]
        assert not [n for n in ast.walk(value) if isinstance(n, ast.Constant)], (
            f"{module}'s {name} restates source ids; read SYNCED_SOURCE_IDS")

    def test_the_reconciliation_reads_the_store_on_every_arm(self):
        """The KeyCRM snapshot, Postgres and ClickHouse all filter on the one
        list, so a source-7 order is compared on all three or on none."""
        source = (ROOT / "core/reconciliation_io.py").read_text(encoding="utf-8")
        assert source.count('", ".join(str(s) for s in ACTIVE_SOURCES)') == 2
        assert "if src not in ACTIVE_SOURCES:" in source


# ─── Silver's CASE ──────────────────────────────────────────────────────────

_CLASSIFIED_RETAIL, _UNLISTED, _B2B = 4, 44, constants.B2B_MANAGER_ID


def _sales_types(rows):
    """`(order id, sales_type)` for `(id, source_id, manager_id)` rows, by the
    rendered CASE executed in DuckDB in memory. Manager 4 is classified
    retail, 44 not: the store's seller, as production classified it."""
    duckdb = pytest.importorskip("duckdb")
    from core.sql_dialect import DUCKDB
    from core.warehouse_sql import silver_sales_type_case

    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE TABLE orders (id INT, source_id INT, manager_id INT,"
                    " ordered_at TIMESTAMPTZ)")
        con.execute("CREATE TABLE managers (id INT, is_retail BOOLEAN)")
        con.execute("CREATE TABLE manager_classifications (manager_id INT,"
                    " is_retail BOOLEAN, valid_from DATE, valid_to DATE)")
        con.executemany("INSERT INTO orders VALUES (?, ?, ?, '2026-11-12 10:00:00+00')",
                        list(rows))
        con.execute("INSERT INTO manager_classifications VALUES"
                    f" ({_CLASSIFIED_RETAIL}, TRUE, '2020-01-01', NULL),"
                    f" ({_UNLISTED}, FALSE, '2020-01-01', NULL)")
        return dict(con.execute(
            f"SELECT o.id, {silver_sales_type_case(DUCKDB)} FROM orders o").fetchall())
    finally:
        con.close()


class TestTheSalesType:
    def test_the_store_is_offline_whoever_sells_there(self):
        """No manager, a retail manager, the store's own unlisted seller and
        even the wholesale manager: `offline` every time. Mutation: drop the
        branch — 44 reads `internal`, 4 `retail`, none `retail`; or move it
        below the manager branches — the same."""
        got = _sales_types([(1, STORE, None), (2, STORE, _CLASSIFIED_RETAIL),
                            (3, STORE, _UNLISTED), (4, STORE, _B2B)])
        assert got == {1: "offline", 2: "offline", 3: "offline", 4: "offline"}

    def test_the_other_sources_keep_their_rule(self):
        got = _sales_types([(1, 1, None), (2, 1, _CLASSIFIED_RETAIL), (3, 1, _UNLISTED),
                            (4, 2, _B2B), (5, EXHIBITION_SOURCE_ID, _CLASSIFIED_RETAIL)])
        assert got == {1: "retail", 2: "retail", 3: "internal", 4: "b2b", 5: "exhibition"}

    def test_the_branch_is_right_after_the_exhibitions(self):
        from core.sql_dialect import POSTGRES
        from core.warehouse_sql import silver_sales_type_case

        lines = [line.strip() for line in silver_sales_type_case(POSTGRES).splitlines()]
        assert lines[1:4] == [
            f"WHEN o.source_id = {EXHIBITION_SOURCE_ID} THEN 'exhibition'",
            f"WHEN o.source_id = {STORE} THEN 'offline'",
            "WHEN o.manager_id IS NULL THEN 'retail'",
        ]

    def test_the_store_has_a_source_name(self):
        from core.sql_dialect import POSTGRES
        from core.warehouse_sql import silver_select_sql

        assert f"WHEN {STORE} THEN 'Офлайн-магазин'" in silver_select_sql(POSTGRES)


# ─── The API ────────────────────────────────────────────────────────────────

class TestTheFilters:
    def test_source_seven_is_accepted(self):
        from core.validators import validate_source_id

        assert validate_source_id(STORE) == STORE

    @pytest.mark.parametrize("source_id", [EXHIBITION_SOURCE_ID, 6, 8, 99])
    def test_the_exhibition_and_unknown_ids_are_still_refused(self, source_id):
        """The exhibition's 400 is the owner's decision 5; this did not move it."""
        from core.exceptions import ValidationError
        from core.validators import validate_source_id

        with pytest.raises(ValidationError):
            validate_source_id(source_id)

    @pytest.mark.parametrize("given", ["offline", " Offline ", "OFFLINE"])
    def test_the_sales_type_is_accepted_and_folded(self, given):
        from core.validators import validate_sales_type

        assert validate_sales_type(given) == "offline"

    def test_summary_and_trend_read_gold_for_the_store_and_not_for_three_or_five(self):
        from core.repositories.revenue import RevenueMixin

        assert STORE in RevenueMixin._GOLD_SOURCE_IDS
        assert not {3, EXHIBITION_SOURCE_ID} & RevenueMixin._GOLD_SOURCE_IDS

    def test_reports_list_the_store(self):
        from core.repositories.revenue import _REPORT_SOURCE_NAMES

        assert _REPORT_SOURCE_NAMES == {1: "Instagram", 2: "Telegram", 4: "Shopify",
                                        STORE: "Офлайн-магазин"}

    def test_the_subcategories_drill_into_the_store(self):
        import asyncio
        from datetime import date

        from core.repositories.revenue import RevenueMixin

        seen = []

        async def run(sql, params=None):
            seen.append(sql)
            return []

        class Probe(RevenueMixin):
            _dashboard_run = staticmethod(run)

        asyncio.run(Probe().get_subcategory_breakdown(
            date(2026, 11, 1), date(2026, 11, 2), "Care", source_id=STORE,
            sales_type="offline"))
        (sql,) = seen
        assert f"l.source_id IN (1, 2, 4, {STORE})" in sql


class TestTheGate:
    """`internal` is the one admin-only sales type. The offline store is a
    sales channel, like the exhibition, and every viewer may read it."""

    @pytest.mark.parametrize("sales_type", ["offline", " Offline ", "exhibition", "retail"])
    def test_a_viewer_passes(self, sales_type):
        from web.routes.auth import require_admin_for_internal

        assert require_admin_for_internal({"user_id": 1, "role": "viewer"}, sales_type) is None

    def test_internal_still_refuses_a_viewer(self):
        from fastapi import HTTPException

        from web.routes.auth import require_admin_for_internal

        with pytest.raises(HTTPException) as refused:
            require_admin_for_internal({"user_id": 1, "role": "viewer"}, "internal")
        assert refused.value.status_code == 403


class TestTheAssistant:
    def test_it_can_ask_for_the_store(self):
        from core.chat_tools import TOOLS, _revenue_totals_sql

        assert "AND sales_type = 'offline'" in _revenue_totals_sql("offline")
        enums = [t["input_schema"]["properties"]["sales_type"]["enum"] for t in TOOLS
                 if "sales_type" in t["input_schema"]["properties"]]
        assert enums and all("offline" in e for e in enums)


# ─── /traffic ───────────────────────────────────────────────────────────────

def _fallback(source_id):
    """`(traffic_type, platform)` the tab gives an order with no UTM row,
    by the mixin's own two expressions executed in DuckDB in memory."""
    duckdb = pytest.importorskip("duckdb")
    from core.repositories.traffic import TrafficMixin

    return tuple(duckdb.connect().execute(
        f"SELECT {TrafficMixin._TRAFFIC_TYPE_EXPR}, {TrafficMixin._PLATFORM_EXPR} "
        "FROM (SELECT CAST(NULL AS VARCHAR) AS traffic_type, "
        "             CAST(NULL AS VARCHAR) AS platform) u, "
        "     (SELECT CAST(? AS INTEGER) AS source_id) s", [source_id]).fetchone())


class TestTraffic:
    def test_an_untagged_store_order_is_organic_on_its_own_platform(self):
        """Mutation: drop the branch — `('unknown', 'unattributed')`, the
        website's "source not identified"."""
        assert _fallback(STORE) == ("organic", "offline_store")

    def test_the_other_fallbacks_are_unmoved(self):
        assert [_fallback(s) for s in (1, 2, 3, 4, 5)] == [
            ("organic", "instagram"), ("organic", "telegram"), ("unknown", "unattributed"),
            ("unknown", "unattributed"), ("unknown", "unattributed")]

    def test_its_channel_is_its_own(self):
        from core.utm_classify import (
            ORGANIC_CHANNELS, _OFFLINE_STORE_SOURCE_ID, first_order_channel, organic_channel,
        )

        assert _OFFLINE_STORE_SOURCE_ID == STORE
        assert "offline_store" in ORGANIC_CHANNELS
        traffic_type, platform = _fallback(STORE)
        assert organic_channel(source_id=STORE, tagged=False, platform=platform,
                               utm_medium="", utm_campaign="") == "offline_store"
        assert first_order_channel(source_id=STORE, tagged=False, platform=platform,
                                   traffic_type=traffic_type, utm_medium="",
                                   utm_campaign="") == ("organic", "offline_store")

    def test_the_dry_run_places_it_as_the_tab_does(self):
        from scripts.utm_reclassify_dryrun import fallback_verdict

        assert fallback_verdict(STORE) == _fallback(STORE)

    def test_one_spelling_of_the_fallback_in_the_mixin(self):
        """The transactions and the campaigns carried copies of their own; a
        copy is where the next source is forgotten."""
        source = (ROOT / "core/repositories/traffic.py").read_text(encoding="utf-8")
        assert len(re.findall(r"THEN 'telegram'", source)) == 1
        assert len(re.findall(r"source_id IN \(1, 2", source)) == 1

    def test_the_routes_accept_the_platform(self):
        from web.routes.api.traffic import _VALID_PLATFORMS

        assert "offline_store" in _VALID_PLATFORMS


# ─── Labels ─────────────────────────────────────────────────────────────────

class TestLabels:
    @pytest.mark.parametrize("key", ["report.sales_type.offline", "traffic.platform.offline_store"])
    def test_core_says_it_in_three_languages(self, key):
        from core.i18n import t

        assert [t(key, lang) for lang in ("en", "uk", "ru")] == [
            "Offline store", "Офлайн-магазин", "Офлайн-магазин"]

    def test_the_bot_names_it_rather_than_printing_seven(self):
        from bot.config import SOURCE_MAPPING
        from core.config import SOURCE_MAPPING as CORE_MAPPING
        from core.models import SourceId

        assert SOURCE_MAPPING[STORE] == CORE_MAPPING[STORE] == "Офлайн-магазин"
        assert SourceId(STORE) is SourceId.OFFLINE_STORE
