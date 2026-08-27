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
