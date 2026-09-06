"""Steps 1 and 2 of «Одна бронза»: the read switch and the buyer landing.

Step 1 is a switch, so the tests are about what a switch must guarantee: off
by default, loud on a typo, intercepting exactly the requests DuckDB's own
Gold would have answered, and falling back visibly instead of taking the
dashboard down.

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
from core.pg_buyers import buyer_row, contact_rows
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


class TestTheReadSwitch:
    def test_off_by_default(self, monkeypatch):
        monkeypatch.delenv(pg_gold_read.ENV, raising=False)
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x")
        assert pg_gold_read.enabled() is False

    def test_needs_both_the_flag_and_a_dsn(self, monkeypatch):
        monkeypatch.setenv(pg_gold_read.ENV, "postgres")
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert pg_gold_read.enabled() is False
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x")
        assert pg_gold_read.enabled() is True

    def test_a_typo_raises_rather_than_serving_the_old_store(self, monkeypatch):
        # KS_BOT_STORE's rule: the variable that decides where revenue comes
        # from must stop loudly on an unknown value.
        monkeypatch.setenv(pg_gold_read.ENV, "postgress")
        with pytest.raises(ValueError):
            pg_gold_read.enabled()

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
    async def test_interception_mirrors_duckdb_routing(self, monkeypatch):
        # A source DuckDB's Gold cannot answer (no channel column) must not
        # reach Postgres either — flipping the flag changes the engine and
        # nothing else.
        from core.duckdb_store import DuckDBStore

        monkeypatch.setenv(pg_gold_read.ENV, "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x")
        fetch = AsyncMock()
        monkeypatch.setattr(pg_gold_read, "fetch_summary", fetch)

        mixin = DuckDBStore.__new__(DuckDBStore)
        out = await mixin._pg_gold_summary(
            date(2026, 8, 1), date(2026, 8, 27), "retail", 5,
        )
        assert out is None
        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_read_falls_back_not_down(self, monkeypatch):
        from core.duckdb_store import DuckDBStore

        monkeypatch.setenv(pg_gold_read.ENV, "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x")
        monkeypatch.setattr(
            pg_gold_read, "fetch_series",
            AsyncMock(side_effect=RuntimeError("pool exhausted")),
        )
        mixin = DuckDBStore.__new__(DuckDBStore)
        out = await mixin._pg_gold_series(
            date(2026, 8, 1), date(2026, 8, 27), "retail", None,
        )
        assert out is None  # DuckDB serves the request; the log said why


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


class TestTheHourlyIdsDiff:
    """The buyers backfill runs every hour, not once: the sync is a delta and
    never re-fetches an existing buyer, so a row lost from Postgres after a
    one-shot backfill would have had no repair path, ever — the audit's
    finding. The diff ships nothing when nothing is missing."""

    @staticmethod
    def _pool(had_history):
        class _Ctx:
            async def __aenter__(self):
                conn = AsyncMock()
                conn.fetchval = AsyncMock(return_value=had_history)
                return conn

            async def __aexit__(self, *a):
                return False

        class _Pool:
            def acquire(self):
                return _Ctx()

        return _Pool()

    @pytest.mark.asyncio
    async def test_runs_the_diff_every_tick(self, monkeypatch):
        from core import pg_buyers
        import core.pg as pg
        import core.pg_landing as pg_landing

        monkeypatch.setattr(pg_landing, "enabled", lambda: True)
        monkeypatch.setattr(pg, "get_pool", AsyncMock(return_value=self._pool(True)))
        ran = AsyncMock(return_value={"shipped": 0, "missing_was": 0})
        monkeypatch.setattr(pg_buyers, "backfill_buyers", ran)

        out = await pg_buyers.hourly_ids_diff(store=object())
        assert out == {"shipped": 0, "missing_was": 0}
        ran.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_first_fill_is_initialisation_not_a_heal(self, monkeypatch):
        from core import pg_buyers
        import core.pg as pg
        import core.pg_landing as pg_landing

        monkeypatch.setattr(pg_landing, "enabled", lambda: True)
        monkeypatch.setattr(pg, "get_pool", AsyncMock(return_value=self._pool(None)))
        monkeypatch.setattr(
            pg_buyers, "backfill_buyers",
            AsyncMock(return_value={"shipped": 20060, "missing_was": 20060}),
        )
        pg_buyers.last_heal.clear()

        await pg_buyers.hourly_ids_diff(store=object())
        assert not pg_buyers.last_heal  # первый налив — не «лечение»

    @pytest.mark.asyncio
    async def test_a_later_diff_that_ships_is_a_heal(self, monkeypatch):
        from core import pg_buyers
        import core.pg as pg
        import core.pg_landing as pg_landing

        monkeypatch.setattr(pg_landing, "enabled", lambda: True)
        monkeypatch.setattr(pg, "get_pool", AsyncMock(return_value=self._pool(True)))
        monkeypatch.setattr(
            pg_buyers, "backfill_buyers",
            AsyncMock(return_value={"shipped": 3, "missing_was": 3}),
        )
        pg_buyers.last_heal.clear()

        await pg_buyers.hourly_ids_diff(store=object())
        assert pg_buyers.last_heal["shipped"] == 3

    @pytest.mark.asyncio
    async def test_stands_down_without_a_dsn(self, monkeypatch):
        from core import pg_buyers
        import core.pg_landing as pg_landing

        monkeypatch.setattr(pg_landing, "enabled", lambda: False)
        ran = AsyncMock()
        monkeypatch.setattr(pg_buyers, "backfill_buyers", ran)

        out = await pg_buyers.hourly_ids_diff(store=object())
        assert "skipped" in out
        ran.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failure_is_returned_not_raised(self, monkeypatch):
        from core import pg_buyers
        import core.pg_landing as pg_landing

        monkeypatch.setattr(pg_landing, "enabled", lambda: True)
        monkeypatch.setattr(
            pg_buyers, "backfill_buyers",
            AsyncMock(side_effect=RuntimeError("pg down")),
        )
        out = await pg_buyers.hourly_ids_diff(store=object())
        assert "error" in out


class TestTheLockIsNeverHeldAcrossTheNetwork:
    """The review's sharpest live finding: the Postgres read used to happen
    inside `self.connection()` — the asyncio lock that serialises every DuckDB
    access — so a hung Postgres would have held the whole dashboard. Now the
    Gold-only branch answered from Postgres must never touch the store at all."""

    @pytest.mark.asyncio
    async def test_summary_from_pg_never_touches_the_store(self, monkeypatch):
        from core import pg_gold_read
        from core.duckdb_store import DuckDBStore

        monkeypatch.setenv(pg_gold_read.ENV, "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x")
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

        monkeypatch.setenv(pg_gold_read.ENV, "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x")
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
