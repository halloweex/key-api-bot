"""Steps 5–6: silver into ClickHouse, Gold derived there, the archive inherited.

The load-bearing test in this file is `TestOneSilverTwoGolds`: from one
Silver, ClickHouse derives the Gold Postgres derives, both grains, at zero
tolerance — which is what dissolves the "рукописная проекция №3" objection.
A measure rewritten for ClickHouse by hand, or a roll-up grouped or keyed
differently, is named there before the daily comparison has to. It needs a
live ClickHouse at KS_CH_URL as well as the test Postgres.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from core import ch_history, ch_silver
from core.ch_silver import (
    SILVER_COLUMNS,
    _SILVER_DDL,
    compare_gold_cells,
    parse_tsv,
    render_tsv,
)
from core.data_quality import Severity
from tests.helpers.recon_chains_ddl import declared_columns
from tests.helpers.recon_clickhouse import (
    FakeClickHouse,
    archive_in_postgres,
    clickhouse_at_the_transport,
    fresh_clickhouse_copies,
    kept_mirror_state,
    needs_clickhouse,
)
from tests.integration.pg_tab_seed import DSN, derive, needs_postgres, seed, tab_in_postgres


def _silver_row(**overrides):
    base = {
        "id": 1001, "source_id": 1, "status_id": 20,
        "grand_total": Decimal("1834.50"),
        "ordered_at": datetime(2026, 8, 27, 12, 30, 5, tzinfo=timezone.utc),
        "buyer_id": 7, "manager_id": None,
        "order_date": date(2026, 8, 27), "is_return": False,
        "sales_type": "retail", "is_active_source": True,
        "source_name": "Instagram", "is_new_customer": True,
        "buyer_first_order_date": date(2026, 8, 27), "promocode": None,
    }
    base.update(overrides)
    return tuple(base[c] for c in SILVER_COLUMNS)


def _gold_cell(**overrides):
    base = {
        "date": date(2026, 8, 27), "sales_type": "retail", "source_id": 1,
        "revenue": Decimal("1834.50"), "orders_count": 1,
        "unique_customers": 1, "new_customers": 1, "returning_customers": 0,
        "returns_count": 0, "returns_revenue": Decimal("0.00"),
        "avg_order_value": Decimal("1834.50"),
    }
    base.update(overrides)
    order = ("date", "sales_type", "source_id", "revenue", "orders_count",
             "unique_customers", "new_customers", "returning_customers",
             "returns_count", "returns_revenue", "avg_order_value")
    return tuple(base[c] for c in order)


class TestSilverWireFormat:
    def test_round_trip_is_identity(self):
        rows = [
            _silver_row(),
            _silver_row(id=1002, is_return=True, buyer_id=None,
                        promocode="WELCOME", ordered_at=None),
            _silver_row(id=1003, sales_type="exhibition", source_id=5,
                        is_new_customer=False),
        ]
        assert parse_tsv(render_tsv(rows).decode("utf-8")) == rows

    def test_bools_render_as_clickhouse_bools(self):
        line = render_tsv([_silver_row()]).decode("utf-8")
        cells = line.strip().split("\t")
        assert cells[SILVER_COLUMNS.index("is_return")] == "false"
        assert cells[SILVER_COLUMNS.index("is_new_customer")] == "true"

    def test_datetimes_render_without_offset(self):
        # DateTime('UTC') refuses '+00:00'; the renderer writes bare seconds.
        line = render_tsv([_silver_row()]).decode("utf-8")
        assert "+00" not in line
        assert "2026-08-27 12:30:05" in line

    def test_the_table_declares_exactly_the_shipped_columns(self):
        """The ship inserts SILVER_COLUMNS by name into the table this DDL
        creates, and the round trip reads them back by name. A set, so the
        statement's layout, types and column order stay its own."""
        assert declared_columns(_SILVER_DDL.format(name="silver.orders")) == set(SILVER_COLUMNS)


# One Silver for both engines, every measure non-trivial on DAY: one buyer on
# two sources (a roll-up's distinct count is not the sum of its fine rows'),
# a returning buyer, a return, b2b, and Opencart, which counts as no revenue.
DAY, EARLIER = date(2026, 6, 10), date(2026, 5, 11)


def _at(day, hour):
    return datetime(day.year, day.month, day.day, hour, tzinfo=timezone.utc)


async def _every_measure(conn, pool):
    await seed(conn, orders=[
        # (id, source_id, status_id, grand_total, ordered_at, buyer, manager, promocode)
        (3, 1, 12, 50.00, _at(EARLIER, 9), 502, None, None),
        (1, 1, 12, 100.00, _at(DAY, 9), 501, None, None),
        (2, 4, 12, 300.00, _at(DAY, 10), 501, None, None),
        (4, 1, 12, 250.00, _at(DAY, 11), 502, None, None),
        (5, 1, 19, 80.00, _at(DAY, 12), 503, None, None),
        (6, 2, 12, 1000.00, _at(DAY, 13), 504, 15, None),
        (7, 3, 12, 500.00, _at(DAY, 14), 505, None, None),
    ])
    await derive(pool)


@needs_postgres
@needs_clickhouse
class TestOneSilverTwoGolds:
    @pytest.mark.asyncio
    async def test_clickhouse_derives_the_gold_postgres_derives(self, monkeypatch):
        """`reconcile_clickhouse` ships Postgres' Silver, derives Gold inside
        ClickHouse and sets it against Postgres' Gold cell by cell, both
        grains: no finding is agreement. Then ClickHouse's Gold for DAY is
        read directly — the roll-up's buyers are not the sum of its fine
        rows'. Mutation: rewrite a measure for ClickHouse alone, or group the
        roll-up by source too — a `ch_engines_gold_*` finding."""
        from core.ch_common import execute

        async with kept_mirror_state(ch_silver.SILVER_STATE, ch_silver.GOLD_STATE), \
                tab_in_postgres(DSN, monkeypatch, _every_measure):
            await fresh_clickhouse_copies()
            issues = await ch_silver.reconcile_clickhouse()
            text = await execute(
                "SELECT sales_type, source_id, toString(revenue), orders_count,"
                " unique_customers, new_customers, returning_customers, returns_count,"
                " toString(returns_revenue) FROM gold.daily_revenue"
                f" WHERE date = toDate('{DAY.isoformat()}')"
                " ORDER BY sales_type, source_id NULLS FIRST FORMAT TabSeparated")

        assert [(i.check_name, i.description) for i in issues] == []
        assert [tuple(line.split("\t")) for line in text.strip().splitlines()] == [
            ("b2b", "\\N", "1000", "1", "1", "1", "0", "0", "0"),
            ("b2b", "2", "1000", "1", "1", "1", "0", "0", "0"),
            ("retail", "\\N", "650", "3", "2", "1", "1", "1", "80"),
            ("retail", "1", "350", "2", "2", "1", "1", "1", "80"),
            ("retail", "3", "0", "0", "0", "0", "0", "0", "0"),
            ("retail", "4", "300", "1", "1", "1", "0", "0", "0"),
        ]


class TestTheEngineVerdict:
    def test_identical_cells_no_findings(self):
        cells = [_gold_cell(), _gold_cell(source_id=None, sales_type="b2b")]
        assert compare_gold_cells(cells, list(cells)) == []

    def test_one_cent_on_avg_is_the_licensed_difference(self):
        pg = [_gold_cell()]
        ch = [_gold_cell(avg_order_value=Decimal("1834.51"))]
        assert compare_gold_cells(pg, ch) == []

    def test_two_cents_on_avg_is_not(self):
        pg = [_gold_cell()]
        ch = [_gold_cell(avg_order_value=Decimal("1834.52"))]
        (issue,) = compare_gold_cells(pg, ch)
        assert issue.check_name == "ch_engines_gold_mismatch"

    def test_a_cent_anywhere_else_is_a_defect(self):
        pg = [_gold_cell()]
        ch = [_gold_cell(revenue=Decimal("1834.51"))]
        (issue,) = compare_gold_cells(pg, ch)
        assert issue.severity is Severity.CRITICAL
        assert "revenue" in issue.description

    def test_uniq_counts_compare_exact(self):
        pg = [_gold_cell(unique_customers=48)]
        ch = [_gold_cell(unique_customers=47)]
        (issue,) = compare_gold_cells(pg, ch)
        assert "unique_customers" in issue.description

    def test_missing_and_extra_cells_are_named(self):
        pg = [_gold_cell(), _gold_cell(sales_type="b2b")]
        ch = [_gold_cell(), _gold_cell(sales_type="internal")]
        names = sorted(i.check_name for i in compare_gold_cells(pg, ch))
        assert names == ["ch_engines_gold_extra", "ch_engines_gold_missing"]


class TestTheArchiveCopy:
    def test_microseconds_survive_the_render(self):
        row = (1, 1001, datetime(2026, 8, 27, 19, 27, 25, 123456,
                                 tzinfo=timezone.utc), "baseline",
               1, 20, 4, Decimal("10.00"), None, 7, None, None, None, None)
        assert b"19:27:25.123456" in ch_history.render_tsv([row])

    @pytest.mark.asyncio
    async def test_unconfigured_stands_down(self, monkeypatch):
        monkeypatch.delenv(ch_silver.URL_ENV, raising=False)
        assert await ch_history.ship_history() == {
            "skipped": f"{ch_silver.URL_ENV} is not set"
        }
        assert await ch_history.reconcile_ch_history() == []

    @needs_postgres
    @pytest.mark.asyncio
    @pytest.mark.parametrize("copy, expected", [
        pytest.param(range(9_995, 10_005), [], id="equal-buckets-mean-silence"),
        # One version below the copy's own MAX(id) lost: its bucket is short.
        # Mutation: invert or drop the bucket comparison.
        pytest.param([i for i in range(9_995, 10_005) if i != 10_001],
                     [("ch_history_buckets", "CRITICAL", 1, (1,))],
                     id="a-short-bucket-is-critical"),
    ])
    async def test_the_buckets_below_the_copy_s_ceiling(self, monkeypatch, copy, expected):
        """Ten versions in Postgres across two 10 000-id buckets, the ship
        healthy; ClickHouse holds `copy`."""
        now = datetime.now(timezone.utc)
        async with archive_in_postgres(monkeypatch) as archive:
            await archive.versions(range(9_995, 10_005), captured_at=now - timedelta(days=2))
            await archive.shipped(last_ok_at=now - timedelta(minutes=30))
            clickhouse_at_the_transport(monkeypatch, FakeClickHouse(archive=copy))
            found = await ch_history.reconcile_ch_history()
        assert [(i.check_name, i.severity.value, i.count, tuple(i.sample_ids))
                for i in found] == expected


class TestTheColumnListsHaveOneHome:
    """The review's finding: the copies restated their column lists, so a
    revision 0012 adding a column to Postgres would silently never reach
    ClickHouse while every round-trip check stayed green — the copy would be
    verifying itself against its own stale list. So what is shipped and
    compared is held to the tables Postgres holds, as migrated."""

    @needs_postgres
    @pytest.mark.asyncio
    async def test_every_column_postgres_holds_is_shipped_and_compared(self):
        import asyncpg

        from core import ch_gold

        conn = await asyncpg.connect(os.environ["KS_PG_DSN"])
        try:
            rows = await conn.fetch(
                "SELECT table_schema || '.' || table_name AS t, column_name "
                "FROM information_schema.columns "
                "WHERE table_schema || '.' || table_name = ANY($1::text[])",
                ["silver.orders", "gold.daily_revenue"])
        finally:
            await conn.close()
        held = {}
        for r in rows:
            held.setdefault(r["t"], set()).add(r["column_name"])
        # Silver: what the ship reads out of Postgres and inserts.
        assert held["silver.orders"] == set(ch_silver.SILVER_COLUMNS)
        # Gold: what ClickHouse's derivation writes and the comparison reads,
        # and the columns its codec parses.
        assert held["gold.daily_revenue"] == set(ch_silver.GOLD_COLUMNS) == set(ch_gold.COLUMNS)

    def test_clickhouse_s_gold_is_parsed_in_the_order_it_is_selected(self):
        """The read-back SELECTs ch_silver's Gold columns and parses the
        answer with ch_gold's codec, by position: a copy in another order
        would read every cell into the wrong column."""
        from core import ch_gold

        assert tuple(ch_gold.COLUMNS) == tuple(ch_silver.GOLD_COLUMNS)

    def test_type_maps_cover_exactly_the_shared_columns(self):
        from core import ch_history, ch_silver

        assert set(ch_silver._TYPES) == set(ch_silver.SILVER_COLUMNS)
        assert set(ch_history._TYPES) == set(ch_history.COLUMNS)

    def test_gold_type_map_covers_the_cell_shape_too(self):
        from core import ch_gold

        assert set(ch_gold._GOLD_TYPES) == set(ch_gold.COLUMNS)
