"""Steps 1 and 2 of «Одна бронза»: the Gold reader and the buyer landing.

Step 1 was a switch (`KS_READ_GOLD`), and its tests were about what a switch
must guarantee: off by default, loud on a typo, intercepting exactly the
requests DuckDB's own Gold would have answered, and falling back visibly
instead of taking the dashboard down. Since step 14's S14-06 there is no
switch and no DuckDB to fall back to: what stays is the reader's predicate,
the routing that still keeps sources 3 and 5 away from it (the owner's
decision 5), the shape of the card row and the chart, and that neither ever
reaches for the DuckDB store.

Step 2's units are the conversions — the places where a Buyer model becomes
two stores' rows — and the one-body-two-engines contract for the order-lines
view, which is revision 0010's contract applied to a view: the migration
spells the SQL out, and this file re-renders the template and asserts the two
agree, so the Postgres view cannot drift from the DuckDB one without a test
naming the divergence.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from core import pg_gold_read
from core.landing_rows import buyer_row, contact_rows
from core.sql_dialect import DUCKDB, POSTGRES, order_lines_select


class _Buyer:
    def __init__(self, **kw):
        defaults = dict(
            id=1, full_name="Оксана К.", birthday=None, note=None,
            phone="+380501112233", email=None, manager_id=None,
            company_id=None, company_name=None, city="Київ", region=None,
            loyalty_program_name=None, loyalty_level_name=None,
            loyalty_discount=0, loyalty_amount=0,
            created_at=None, updated_at=None, phones=None, emails=None,
        )
        defaults.update(kw)
        self.__dict__.update(defaults)


class TestTheGoldReader:
    def test_rollup_predicate_for_all_sources(self):
        params = [date(2026, 8, 1), date(2026, 8, 27)]
        where = pg_gold_read._predicate("retail", None, params)
        assert "source_id IS NULL" in where
        assert "sales_type = $3" in where
        assert params[2] == "retail"

    def test_fine_row_predicate_for_one_source(self):
        params = [date(2026, 8, 1), date(2026, 8, 27)]
        where = pg_gold_read._predicate("all", 4, params)
        assert "source_id = $3" in where
        assert "sales_type" not in where  # 'all' spans the types

    @pytest.mark.asyncio
    @pytest.mark.parametrize("source", [3, 5])
    async def test_a_source_gold_is_not_asked_about_never_reaches_it(
        self, monkeypatch, source,
    ):
        """A source DuckDB's Gold had no column for is not asked of
        Postgres's Gold either, which could answer it: `/api/summary`
        answers zeros, the owner's decision 5 for step 14. Mutation: route
        3 or 5 to `_pg_gold_summary` — the fetch is awaited and the zeros
        become the source's money."""
        from core.duckdb_store import DuckDBStore

        fetch = AsyncMock(return_value=(9, 900.0, 1, 10.0))
        monkeypatch.setattr(pg_gold_read, "fetch_summary", fetch)

        mixin = DuckDBStore.__new__(DuckDBStore)
        out = await mixin.get_summary_stats(
            date(2026, 8, 1), date(2026, 8, 27), source_id=source,
        )
        assert (out["totalOrders"], out["totalRevenue"], out["avgCheck"],
                out["totalReturns"], out["returnsRevenue"]) == (0, 0, 0, 0, 0)
        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("source", [None, 1, 2, 4])
    async def test_every_other_source_is_asked_of_gold(self, monkeypatch, source):
        from core.duckdb_store import DuckDBStore

        fetch = AsyncMock(return_value=(2, 300.0, 0, 0.0))
        monkeypatch.setattr(pg_gold_read, "fetch_summary", fetch)

        mixin = DuckDBStore.__new__(DuckDBStore)
        out = await mixin.get_summary_stats(
            date(2026, 8, 1), date(2026, 8, 27), source_id=source, sales_type="b2b",
        )
        assert out["totalRevenue"] == 300.0 and out["avgCheck"] == 150.0
        fetch.assert_awaited_once_with(
            date(2026, 8, 1), date(2026, 8, 27), "b2b", source)


class TestTheBuyerConversions:
    def test_birthday_string_becomes_a_date(self):
        row = buyer_row(_Buyer(birthday="1992-04-17"))
        assert row[2] == date(1992, 4, 17)

    def test_timestamps_parse_both_separators(self):
        a = buyer_row(_Buyer(created_at="2026-08-27 12:00:00+00:00"))
        b = buyer_row(_Buyer(created_at="2026-08-27T12:00:00+00:00"))
        assert a[15] == b[15] == datetime(2026, 8, 27, 12, tzinfo=timezone.utc)

    def test_an_unparseable_timestamp_raises_rather_than_writing_none(self):
        # DuckDB will have stored the value; a silent None here would be a
        # discrepancy this module manufactured.
        with pytest.raises(ValueError):
            buyer_row(_Buyer(created_at="вчора ввечері"))

    def test_contacts_first_is_primary_empties_skipped_dupes_collapsed(self):
        rows = contact_rows(_Buyer(
            phones=["+380501112233", "", "+380501112233", "+380671234567"],
            emails=["a@b.ua"],
        ))
        assert (1, "phone", "+380501112233", True) in rows
        assert (1, "phone", "+380671234567", False) in rows
        assert (1, "email", "a@b.ua", True) in rows
        assert len(rows) == 3


class TestOneBodyTwoEngines:
    """The order-lines view: migration 0011 froze the Postgres rendering."""

    def test_the_two_renderings_differ_only_in_table_names(self):
        dk = order_lines_select(DUCKDB)
        pg = order_lines_select(POSTGRES)
        undone = (
            pg.replace("bronze.order_products", "order_products")
            .replace("silver.orders", "silver_orders")
            .replace("bronze.products", "products")
            .replace("bronze.categories", "categories")
        )
        assert undone == dk

    def test_migration_0011_froze_exactly_the_postgres_rendering(self):
        migration = Path("migrations/versions/0011_buyers_lines_vitrina.py")
        text = migration.read_text(encoding="utf-8")
        # The frozen view body, whitespace-normalised — structure, not prose.
        frozen = re.search(
            r"CREATE VIEW silver\.order_lines AS\n(.*?)\n\s*\"\"\"",
            text, re.S,
        )
        assert frozen, "migration no longer creates the view"
        normal = " ".join(frozen.group(1).split())
        rendered = " ".join(order_lines_select(POSTGRES).split())
        assert normal == rendered


class TestTheLockIsNeverHeldAcrossTheNetwork:
    """The review's sharpest live finding: the Postgres read used to happen
    inside `self.connection()` — the asyncio lock that serialises every DuckDB
    access — so a hung Postgres would have held the whole dashboard. Since
    step 14's S14-06 the dashboard has no DuckDB tail at all, and neither the
    summary nor the trend may touch the store."""

    @pytest.mark.asyncio
    async def test_summary_from_pg_never_touches_the_store(self, monkeypatch):
        from core import pg_gold_read
        from core.duckdb_store import DuckDBStore

        monkeypatch.setattr(
            pg_gold_read, "fetch_summary",
            AsyncMock(return_value=(10, 1000.0, 1, 50.0)),
        )

        store = DuckDBStore.__new__(DuckDBStore)

        def forbidden():
            raise AssertionError("store lock was taken for a PG-answered read")

        store.connection = forbidden  # type: ignore[method-assign]

        out = await store.get_summary_stats(date(2026, 8, 1), date(2026, 8, 27))
        assert out["totalOrders"] == 10
        assert out["totalRevenue"] == 1000.0
        assert set(out) == {
            "totalOrders", "totalRevenue", "avgCheck", "totalReturns",
            "returnsRevenue", "startDate", "endDate",
        }

    @pytest.mark.asyncio
    async def test_trend_prefetch_happens_before_the_lock(self, monkeypatch):
        # Both series — current and comparison — must be awaited before
        # connection() is entered; the fake lock records the ordering.
        from core import pg_gold_read
        from core.duckdb_store import DuckDBStore

        calls: list = []

        async def fake_series(lo, hi, st, src):
            calls.append(("pg", lo, hi))
            return [(lo, 1.0, 1)]

        monkeypatch.setattr(pg_gold_read, "fetch_series", fake_series)

        store = DuckDBStore.__new__(DuckDBStore)

        def forbidden():
            raise AssertionError("store lock was taken for a PG-answered trend")

        store.connection = forbidden  # type: ignore[method-assign]

        out = await store.get_revenue_trend(
            date(2026, 8, 20), date(2026, 8, 21), include_comparison=True,
        )
        assert [c[0] for c in calls] == ["pg", "pg"]
        assert out["labels"]
