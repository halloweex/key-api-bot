"""Gold, computed in Postgres, at a grain DuckDB's table cannot hold.

The interesting tests here are not the ones that read the SQL. They are the
ones that **run** it: DuckDB speaks `GROUPING SETS` too, so the Postgres
projection can be rendered against `silver_orders` and its output compared,
row for row, with the Gold that DuckDB built from the same Silver in the same
process. That turns the claim this whole design rests on — "every one of
DuckDB's fourteen measures has an exact counterpart at the new grain" — into
something executed rather than argued.

It also pins the reason the roll-up rows exist at all. `TestWhyTheRollUpIsStored`
constructs the case measured on production 29 times: one buyer, two channels,
one day. Summing the per-source rows says two customers. The roll-up says one.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.duckdb_constants import REVENUE_SOURCE_IDS
from core.duckdb_store import (
    DUCKDB_GOLD_COLUMNS,
    GOLD_MEASURES,
    GOLD_REVENUE_SELECT_SQL,
    GOLD_SOURCE_MEASURES,
    DuckDBStore,
)
from core.mirror_reconciliation import (
    _GOLD_ADDITIVE,
    _GOLD_ROLLUP_MEASURES,
    _GOLD_SOURCE_MEASURES,
    GOLD_GRACE_MINUTES,
    GOLD_PG_TABLE,
    SILVER_GRACE_MINUTES,
    compare_gold,
)
from core.pg_gold import (
    GOLD_COLUMNS,
    GOLD_MEASURE_COLUMNS,
    GOLD_TABLE,
    insert_sql,
    select_sql,
)
from core.sql_dialect import DUCKDB

# Anchored on this file, not on the working directory: a suite that only passes
# when pytest is launched from the repository root passes for a reason nobody
# wrote down.
_VERSIONS = Path(__file__).resolve().parents[2] / "migrations" / "versions"


# ── fixtures ──────────────────────────────────────────────────────────────────


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "gold.duckdb")
    await store.connect()
    return store


def _insert_order(conn, oid, source_id, buyer_id, when, total="1000.00", status=12):
    conn.execute(
        """
        INSERT INTO orders (
            id, source_id, status_id, grand_total, ordered_at, created_at,
            updated_at, buyer_id, manager_id, manager_comment, promocode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
        """,
        [oid, source_id, status, total, when, when, when, buyer_id],
    )


async def _seed(store, orders):
    """`orders` is a list of (id, source_id, buyer_id, total).

    Midday UTC, one day back: Silver derives `order_date` as the Kyiv date, and
    anchoring at midday keeps the seeded instant and the derived date on the
    same day at every offset Kyiv has ever used. The neighbouring Gold test was
    broken three hours out of twenty-four for exactly this reason.
    """
    when = datetime.now(timezone.utc).replace(
        hour=12, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    async with store.connection() as conn:
        for oid, source_id, buyer_id, total in orders:
            _insert_order(conn, oid, source_id, buyer_id, when, total)
    await store.refresh_warehouse_layers(trigger="manual")
    return when


def _run_pg_projection(conn):
    """The Postgres Gold projection, executed against DuckDB's Silver.

    Same text `core/pg_gold.py` sends to Postgres, rendered for the other
    engine by the same dialect object the Silver projection uses.
    """
    rows = conn.execute(select_sql(DUCKDB)).fetchall()
    keyed = {}
    for row in rows:
        date, sales_type, source_id = row[0], row[1], row[2]
        keyed[(date, sales_type, source_id)] = dict(
            zip(GOLD_MEASURE_COLUMNS, row[3:])
        )
    return keyed


def _duckdb_gold(conn):
    columns = list(DUCKDB_GOLD_COLUMNS)
    rows = conn.execute(
        f"SELECT {', '.join(columns)} FROM gold_daily_revenue"
    ).fetchall()
    return {
        (r[0], r[1]): dict(zip(columns[2:], r[2:]))
        for r in rows
    }




def _stored(value):
    """What lands in the column, not what the projection returned.

    `avg_order_value` is a division: DuckDB's DECIMAL `/` promotes to DOUBLE,
    PostgreSQL's NUMERIC `/` keeps sixteen-odd digits, and **both** are rounded
    to two places by the INSERT — DuckDB into DECIMAL(12,2), Postgres into
    NUMERIC(12,2). Comparing the unrounded projection output compares an
    intermediate neither store keeps.
    """
    return Decimal(str(value)).quantize(Decimal("0.01"))


def _migration_columns():
    """The column names `CREATE TABLE gold.daily_revenue` declares, in order."""
    import re

    ddl = _VERSIONS.joinpath("0007_gold_daily_revenue.py").read_text(
        encoding="utf-8"
    )
    body = ddl.split("CREATE TABLE gold.daily_revenue (", 1)[1]
    columns = []
    for raw in body.splitlines():
        line = raw.strip()
        if line.startswith("--") or not line:
            continue
        if line.startswith("CONSTRAINT") or line.startswith(")"):
            break
        match = re.match(r"^([a-z_]+)\s+(DATE|TEXT|INTEGER|NUMERIC)", line)
        if match:
            columns.append(match.group(1))
    return columns


# ── one definition of a measure ──────────────────────────────────────────────


class TestOneDefinitionOfAGoldMeasure:
    """#101 changed `sales_type` in one copy of the Silver projection and not
    the other, and it took under a day. Gold now has two shapes on two engines,
    which is exactly the situation that invites a second copy of the arithmetic.
    """

    def test_every_shared_measure_appears_verbatim_in_both_renderings(self):
        duckdb_sql = GOLD_REVENUE_SELECT_SQL
        postgres_sql = select_sql()
        for name, expression in GOLD_MEASURES.items():
            assert expression in duckdb_sql, f"{name} missing from DuckDB Gold"
            assert expression in postgres_sql, f"{name} missing from Postgres Gold"

    def test_postgres_stores_every_shared_measure_and_no_channel_columns(self):
        """The channel columns are what `source_id` replaced. Carrying them as
        well would keep the shape that cannot express the exhibition."""
        assert set(GOLD_MEASURE_COLUMNS) == set(GOLD_MEASURES)
        assert not set(GOLD_MEASURE_COLUMNS) & set(GOLD_SOURCE_MEASURES)

    def test_the_insert_names_the_columns_it_selects(self):
        sql = insert_sql()
        assert f"INSERT INTO {GOLD_TABLE} ({', '.join(GOLD_COLUMNS)})" in sql
        # Parsed, not grepped: the aliases the SELECT actually produces, in
        # order, must be the columns the INSERT names in order.
        aliases = [
            line.rsplit(" AS ", 1)[1].strip().rstrip(",")
            for line in select_sql().splitlines()
            if " AS " in line
        ]
        assert aliases == list(GOLD_COLUMNS)

    def test_the_writer_and_the_migration_agree_on_the_columns(self):
        """The two are written a file apart and neither imports the other, so
        nothing but this connects them. Parsed from the DDL rather than grepped
        for: a comment naming a column would satisfy a grep."""
        assert _migration_columns() == list(GOLD_COLUMNS)

    @pytest.mark.asyncio
    async def test_duckdb_column_order_matches_its_positional_insert(self, tmp_path):
        """`gold_daily_revenue` is written by an INSERT with no column list, so
        `DUCKDB_GOLD_COLUMNS` is load-bearing: a reordering there is a silent
        column shift, not an error. Read from the live table, not the DDL text.
        """
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                columns = [
                    r[0] for r in conn.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'gold_daily_revenue' "
                        "ORDER BY ordinal_position"
                    ).fetchall()
                ]
            assert tuple(columns) == DUCKDB_GOLD_COLUMNS
        finally:
            await store.close()

    def test_the_channel_mapping_is_only_sound_while_those_sources_are_active(self):
        """DuckDB's channel columns filter `source_id = n`; Postgres filters
        `is_active_source`. They agree exactly because 1, 2 and 4 are active —
        drop one from REVENUE_SOURCE_IDS and the reconciliation starts
        comparing two different questions.
        """
        mapped = {source_id for source_id, _ in _GOLD_SOURCE_MEASURES.values()}
        assert mapped <= set(REVENUE_SOURCE_IDS)


# ── the projection, executed ─────────────────────────────────────────────────


class TestTheTwoShapesAgree:
    """Run both projections over one Silver and check the mapping holds."""

    @pytest.mark.asyncio
    async def test_the_rollup_reproduces_every_shared_measure(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store, [
                (1, 1, 100, "1000.00"),
                (2, 2, 101, "250.50"),
                (3, 4, 100, "700.25"),
                (4, 1, 102, "99.99"),
            ])
            async with store.connection() as conn:
                gold = _duckdb_gold(conn)
                pg = _run_pg_projection(conn)

            assert gold, "the fixture produced no Gold to compare against"
            for (date, sales_type), dk in gold.items():
                rollup = pg[(date, sales_type, None)]
                for measure in _GOLD_ROLLUP_MEASURES:
                    assert _stored(dk[measure]) == _stored(rollup[measure]), (
                        f"{measure} differs on {date} {sales_type}"
                    )
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_fine_rows_reproduce_every_channel_column(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            await _seed(store, [
                (1, 1, 100, "1000.00"),
                (2, 2, 101, "250.50"),
                (3, 4, 100, "700.25"),
                (4, 1, 102, "99.99"),
            ])
            async with store.connection() as conn:
                gold = _duckdb_gold(conn)
                pg = _run_pg_projection(conn)

            for (date, sales_type), dk in gold.items():
                for dk_column, (source_id, pg_column) in _GOLD_SOURCE_MEASURES.items():
                    fine = pg.get((date, sales_type, source_id))
                    actual = Decimal(0) if fine is None else Decimal(str(fine[pg_column]))
                    assert Decimal(str(dk[dk_column])) == actual, (
                        f"{dk_column} differs on {date} {sales_type}"
                    )
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_exhibition_gets_a_row_where_it_had_no_column(self, tmp_path):
        """The whole reason the grain changed. Source 5 is ₴266,059.00 of
        production revenue that `SUM(the three channel columns)` cannot see.
        """
        store = await _make_store(tmp_path)
        try:
            await _seed(store, [
                (1, 1, 100, "1000.00"),
                (2, 5, 101, "266.00"),
            ])
            async with store.connection() as conn:
                gold = _duckdb_gold(conn)
                pg = _run_pg_projection(conn)

            cell = next(c for c in gold if c[1] == "exhibition")
            dk = gold[cell]
            # DuckDB: revenue is there, and every channel column is empty. On
            # production that is ₴266,059.00 across 177 orders — money in the
            # warehouse that a per-channel reading of Gold cannot see.
            assert Decimal(str(dk["revenue"])) == Decimal("266.00")
            assert all(
                Decimal(str(dk[c])) == 0
                for c in ("instagram_revenue", "telegram_revenue", "shopify_revenue")
            )
            assert all(
                int(dk[c]) == 0
                for c in ("instagram_orders", "telegram_orders", "shopify_orders")
            )
            # Postgres: the channel has a row, and it needed no new column.
            assert pg[(cell[0], "exhibition", 5)]["revenue"] == Decimal("266.00")
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_inactive_source_gets_a_row_of_zeroes(self, tmp_path):
        """Opencart is 2,470 orders and ₴5.8M gross, and none of it is revenue
        as this warehouse defines revenue. A zero row is `is_active_source`
        written out, and DuckDB already does the same one grain up.
        """
        store = await _make_store(tmp_path)
        try:
            await _seed(store, [
                (1, 1, 100, "1000.00"),
                (2, 3, 101, "5000.00"),
            ])
            async with store.connection() as conn:
                pg = _run_pg_projection(conn)

            opencart = [v for (_d, _s, src), v in pg.items() if src == 3]
            assert opencart, "no row for the inactive source at all"
            assert all(Decimal(str(row["revenue"])) == 0 for row in opencart)
        finally:
            await store.close()


class TestWhyTheRollUpIsStored:
    """29 cells of 2,107 on production, and this is their shape."""

    @pytest.mark.asyncio
    async def test_summing_the_fine_rows_overstates_the_customer_count(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            # One buyer, two channels, one day.
            await _seed(store, [
                (1, 1, 100, "500.00"),
                (2, 2, 100, "500.00"),
            ])
            async with store.connection() as conn:
                gold = _duckdb_gold(conn)
                pg = _run_pg_projection(conn)

            (date, sales_type), dk = next(iter(gold.items()))
            rollup = pg[(date, sales_type, None)]
            folded = sum(
                int(v["unique_customers"])
                for (d, s, src), v in pg.items()
                if src is not None and (d, s) == (date, sales_type)
            )

            assert int(dk["unique_customers"]) == 1
            assert int(rollup["unique_customers"]) == 1
            # The number the fine grain alone would have produced, and the
            # reason this table stores a roll-up instead of deriving one.
            assert folded == 2

        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_additive_measures_do_fold(self, tmp_path):
        """Which is what `gold_rollup_mismatch` is entitled to assert."""
        store = await _make_store(tmp_path)
        try:
            await _seed(store, [
                (1, 1, 100, "500.00"),
                (2, 2, 100, "500.00"),
                (3, 4, 101, "125.25"),
            ])
            async with store.connection() as conn:
                pg = _run_pg_projection(conn)

            cells = {(d, s) for (d, s, src) in pg if src is None}
            for date, sales_type in cells:
                rollup = pg[(date, sales_type, None)]
                for measure in _GOLD_ADDITIVE:
                    folded = sum(
                        Decimal(str(v[measure]))
                        for (d, s, src), v in pg.items()
                        if src is not None and (d, s) == (date, sales_type)
                    )
                    assert Decimal(str(rollup[measure])) == folded
        finally:
            await store.close()


# ── the comparison ───────────────────────────────────────────────────────────


DAY = "2026-08-01"


def _cell(**overrides):
    row = {c: Decimal(0) for c in _GOLD_ROLLUP_MEASURES}
    row.update({k: Decimal(str(v)) for k, v in overrides.items()})
    return row


def _dk_row(**overrides):
    row = {c: Decimal(0) for c in _GOLD_ROLLUP_MEASURES}
    row.update({c: Decimal(0) for c in _GOLD_SOURCE_MEASURES})
    row.update({k: Decimal(str(v)) for k, v in overrides.items()})
    return row


def _names(issues):
    return {i.check_name for i in issues}


class TestCompareGold:
    def test_identical_sides_report_nothing(self):
        dk = {(DAY, "retail"): _dk_row(revenue=100, instagram_revenue=100)}
        rollup = {(DAY, "retail"): _cell(revenue=100)}
        fine = {(DAY, "retail", 1): _cell(revenue=100)}
        assert compare_gold(dk, {}, rollup, fine) == []

    def test_a_missing_fine_row_reads_as_zero_not_as_a_missing_row(self):
        """Postgres writes a channel row only for channels that sold; DuckDB
        writes a zero into the column regardless."""
        dk = {(DAY, "retail"): _dk_row(revenue=100, instagram_revenue=100)}
        rollup = {(DAY, "retail"): _cell(revenue=100)}
        fine = {(DAY, "retail", 1): _cell(revenue=100)}
        assert compare_gold(dk, {}, rollup, fine) == []

    def test_a_cell_postgres_never_aggregated_is_critical(self):
        dk = {(DAY, "retail"): _dk_row(revenue=100)}
        issues = compare_gold(dk, {}, {}, {})
        assert _names(issues) == {"gold_missing_cells"}
        assert issues[0].severity.name == "CRITICAL"

    def test_a_cell_only_postgres_has_is_critical(self):
        rollup = {(DAY, "retail"): _cell(revenue=100)}
        issues = compare_gold({}, {}, rollup, {})
        assert "gold_orphan_cells" in _names(issues)

    def test_a_disagreeing_measure_names_the_column_and_both_values(self):
        dk = {(DAY, "retail"): _dk_row(revenue=100)}
        rollup = {(DAY, "retail"): _cell(revenue=101)}
        issues = [i for i in compare_gold(dk, {}, rollup, {})
                  if i.check_name == "gold_cell_values"]
        assert len(issues) == 1
        assert "revenue" in issues[0].description
        assert "DuckDB=100" in issues[0].description
        assert "Postgres=101" in issues[0].description

    def test_a_disagreeing_channel_is_caught_through_the_mapping(self):
        dk = {(DAY, "retail"): _dk_row(revenue=100, instagram_revenue=100)}
        rollup = {(DAY, "retail"): _cell(revenue=100)}
        fine = {(DAY, "retail", 1): _cell(revenue=99)}
        issues = [i for i in compare_gold(dk, {}, rollup, fine)
                  if i.check_name == "gold_cell_values"]
        assert "instagram_revenue" in issues[0].description

    def test_avg_order_value_may_differ_by_a_cent_and_nothing_else_may(self):
        """DuckDB's DECIMAL division promotes to DOUBLE; PostgreSQL divides
        exactly. Every other column is compared at zero."""
        dk = {(DAY, "retail"): _dk_row(avg_order_value=Decimal("8.00"))}
        rollup = {(DAY, "retail"): _cell(avg_order_value=Decimal("8.01"))}
        assert compare_gold(dk, {}, rollup, {}) == []

        dk = {(DAY, "retail"): _dk_row(avg_order_value=Decimal("8.00"))}
        rollup = {(DAY, "retail"): _cell(avg_order_value=Decimal("8.02"))}
        assert "gold_cell_values" in _names(compare_gold(dk, {}, rollup, {}))

        dk = {(DAY, "retail"): _dk_row(returns_revenue=Decimal("8.00"))}
        rollup = {(DAY, "retail"): _cell(returns_revenue=Decimal("8.01"))}
        assert "gold_cell_values" in _names(compare_gold(dk, {}, rollup, {}))

    def test_a_cell_holding_a_just_synced_order_is_not_reported(self):
        """Postgres is legitimately up to KS_PG_SILVER_INTERVAL_S behind."""
        now = datetime.now(timezone.utc)
        dk = {(DAY, "retail"): _dk_row(revenue=100)}
        fresh = {(DAY, "retail"): now - timedelta(minutes=1)}
        assert compare_gold(dk, fresh, {}, {}, now=now) == []

    def test_the_same_cell_is_reported_once_the_window_has_passed(self):
        now = datetime.now(timezone.utc)
        dk = {(DAY, "retail"): _dk_row(revenue=100)}
        stale = {(DAY, "retail"): now - timedelta(minutes=GOLD_GRACE_MINUTES + 1)}
        assert "gold_missing_cells" in _names(compare_gold(dk, stale, {}, {}, now=now))

    def test_the_grace_window_is_the_one_silver_uses(self):
        """Gold is rebuilt in the same call, on the same floor, so it is behind
        by the same amount. Two numbers here would drift apart."""
        assert GOLD_GRACE_MINUTES == SILVER_GRACE_MINUTES


class TestTheRollUpIsCheckedAgainstItsOwnFineRows:
    """The only check that sees sources 3 and 5, which have no DuckDB column."""

    def test_fine_rows_that_do_not_add_up_are_critical(self):
        rollup = {(DAY, "retail"): _cell(revenue=100)}
        fine = {(DAY, "retail", 1): _cell(revenue=60),
                (DAY, "retail", 5): _cell(revenue=30)}
        issues = compare_gold({}, {}, rollup, fine)
        assert "gold_rollup_mismatch" in _names(issues)

    def test_fine_rows_that_add_up_are_silent(self):
        rollup = {(DAY, "retail"): _cell(revenue=100)}
        fine = {(DAY, "retail", 1): _cell(revenue=70),
                (DAY, "retail", 5): _cell(revenue=30)}
        issues = [i for i in compare_gold({}, {}, rollup, fine)
                  if i.check_name == "gold_rollup_mismatch"]
        assert issues == []

    def test_it_does_not_add_up_the_distinct_counts(self):
        """Summing them is the error the roll-up row exists to prevent, so the
        check must not be the thing that commits it."""
        assert set(_GOLD_ADDITIVE).isdisjoint({
            "unique_customers", "new_customers", "returning_customers",
            "avg_order_value",
        })
        rollup = {(DAY, "retail"): _cell(unique_customers=1)}
        fine = {(DAY, "retail", 1): _cell(unique_customers=1),
                (DAY, "retail", 2): _cell(unique_customers=1)}
        issues = [i for i in compare_gold({}, {}, rollup, fine)
                  if i.check_name == "gold_rollup_mismatch"]
        assert issues == []


# ── the hook ─────────────────────────────────────────────────────────────────


class TestTheRefreshHook:
    def _scheduler(self):
        from core.scheduler import BackgroundScheduler

        BackgroundScheduler._pg_silver_last_at = None
        return BackgroundScheduler()

    @pytest.mark.asyncio
    async def test_gold_is_rebuilt_after_silver(self, monkeypatch):
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
        order = []
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_order_utm.ship_order_utm",
                   new=AsyncMock(side_effect=lambda _s: order.append("utm") or {})), \
             patch("core.pg_silver.rebuild_silver",
                   new=AsyncMock(side_effect=lambda: order.append("silver") or {})), \
             patch("core.pg_gold.rebuild_gold",
                   new=AsyncMock(side_effect=lambda: order.append("gold") or {})):
            await self._scheduler()._rebuild_postgres_layers({"status": "success"})
        # The UTM ship is last on purpose: nothing here derives from it, so a
        # shipping fault must not cost the revenue Gold (revision 0018).
        assert order[:2] == ["silver", "gold"]
        assert order[-1] == "utm"

    @pytest.mark.asyncio
    async def test_a_failed_silver_leaves_gold_alone(self, monkeypatch):
        """A Gold rebuilt over a stale Silver would stamp a fresh watermark on
        a stale answer, and that reads clean."""
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_silver.rebuild_silver",
                   side_effect=RuntimeError("postgres is down")), \
             patch("core.pg_gold.rebuild_gold", new=AsyncMock()) as gold:
            await self._scheduler()._rebuild_postgres_layers({"status": "success"})
        gold.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_failed_duckdb_refresh_leaves_both_alone(self, monkeypatch):
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_silver.rebuild_silver", new=AsyncMock()) as silver, \
             patch("core.pg_gold.rebuild_gold", new=AsyncMock()) as gold:
            await self._scheduler()._rebuild_postgres_layers({"status": "error"})
        silver.assert_not_called()
        gold.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_gold_fault_cannot_break_the_refresh(self, monkeypatch):
        """Rule 8: DuckDB is what the business looks at."""
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "0")
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_silver.rebuild_silver", new=AsyncMock(return_value={})), \
             patch("core.pg_gold.rebuild_gold",
                   side_effect=RuntimeError("gold is down")):
            await self._scheduler()._rebuild_postgres_layers({"status": "success"})

    @pytest.mark.asyncio
    async def test_one_floor_covers_both_layers(self, monkeypatch):
        monkeypatch.setenv("KS_PG_SILVER_INTERVAL_S", "600")
        scheduler = self._scheduler()
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_order_utm.ship_order_utm", new=AsyncMock(return_value={})), \
             patch("core.pg_silver.rebuild_silver", new=AsyncMock(return_value={})), \
             patch("core.pg_gold.rebuild_gold",
                   new=AsyncMock(return_value={})) as gold:
            await scheduler._rebuild_postgres_layers({"status": "success"})
            await scheduler._rebuild_postgres_layers({"status": "success"})
        assert gold.await_count == 1


class TestTheJobRunsIt:
    def test_the_mirror_landing_job_reconciles_gold_after_silver(self):
        """Gold aggregates the Silver the same run just compared, so a Gold
        finding reported before the Silver one would be an effect above its
        cause."""
        import inspect

        from core.scheduler import BackgroundScheduler

        source = inspect.getsource(BackgroundScheduler._run_dq_mirror_landing)
        assert source.index("await reconcile_silver(store)") < source.index(
            "await reconcile_gold(store)"
        )

    def test_the_gold_table_is_watched_under_the_same_layer(self):
        """All three comparisons run inside one call, so they cannot have
        different ages and a fourth layer would invent one."""
        from core.data_quality import WATCHED_LAYERS
        from core.mirror_reconciliation import MIRROR_LAYER

        assert MIRROR_LAYER in WATCHED_LAYERS
        assert GOLD_PG_TABLE == GOLD_TABLE
