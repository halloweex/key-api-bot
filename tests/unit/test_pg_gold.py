"""Gold, computed in Postgres, at a grain DuckDB's table cannot hold.

The interesting tests here are not the ones that read the SQL. They are the
ones that **run** it: the Postgres projection over Silver derived from Bronze,
its output compared, measure for measure, with the Gold that DuckDB built from
the same orders. (DuckDB speaks `GROUPING SETS` too, and until step 14's
S14-18 the projection was rendered for DuckDB over DuckDB's Silver here; it
runs in Postgres, by the derivation's own rebuilds, since.)
That turns the claim this whole design rests on — "every one of DuckDB's
fourteen measures has an exact counterpart at the new grain" — into
something executed rather than argued. DuckDB's own Gold went with its
warehouse rebuild in step 14's S14-15; what it answered on these fixtures is
a literal since (`DUCKDB_GOLD`, rule 9), captured on the commit before.

It also pins the reason the roll-up rows exist at all. `TestWhyTheRollUpIsStored`
constructs the case measured on production 29 times: one buyer, two channels,
one day. Summing the per-source rows says two customers. The roll-up says one.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core.mirror_reconciliation import (
    _GOLD_ADDITIVE,
    _GOLD_ROLLUP_MEASURES,
    GOLD_PG_TABLE,
    _gold_internal_findings,
)

# DuckDB's channel column → (source_id, the Postgres column holding the same
# number). The daily comparison read DuckDB's Gold through this mapping
# (`mirror_reconciliation._GOLD_SOURCE_MEASURES`) until step 14's S14-12
# deleted it; `TestTheTwoShapesAgree` still runs both projections over one
# Silver, so the mapping it asserts is written out here.
_GOLD_SOURCE_MEASURES = {
    "instagram_revenue": (1, "revenue"),
    "telegram_revenue": (2, "revenue"),
    "shopify_revenue": (4, "revenue"),
    "instagram_orders": (1, "orders_count"),
    "telegram_orders": (2, "orders_count"),
    "shopify_orders": (4, "orders_count"),
}
from core.pg_gold import (
    GOLD_COLUMNS,
    GOLD_MEASURE_COLUMNS,
    GOLD_TABLE,
    insert_sql,
    select_sql,
)
from core.warehouse_sql import GOLD_MEASURES
from tests.integration.pg_tab_seed import (
    DSN, derive, needs_postgres, seed, tab_in_postgres,
)

# DuckDB's `gold_daily_revenue` on the two fixtures below, its one cell each
# (retail, one day), as `_duckdb_gold` read it on the base of step 14's S14-15,
# before DuckDB's rebuild was deleted (rule 9). Numbers as DuckDB returned
# them: DECIMALs as strings, counts as ints.
DUCKDB_GOLD = {
    "four_orders": {
        "avg_order_value": "512.68", "instagram_orders": 2,
        "instagram_revenue": "1099.99", "new_customers": 3, "orders_count": 4,
        "returning_customers": 0, "returns_count": 0, "returns_revenue": "0.00",
        "revenue": "2050.74", "shopify_orders": 1, "shopify_revenue": "700.25",
        "telegram_orders": 1, "telegram_revenue": "250.50", "unique_customers": 3,
    },
    "one_buyer_two_channels": {
        "avg_order_value": "500.00", "instagram_orders": 1,
        "instagram_revenue": "500.00", "new_customers": 1, "orders_count": 2,
        "returning_customers": 0, "returns_count": 0, "returns_revenue": "0.00",
        "revenue": "1000.00", "shopify_orders": 0, "shopify_revenue": "0.00",
        "telegram_orders": 1, "telegram_revenue": "500.00", "unique_customers": 1,
    },
}
FOUR_ORDERS = [(1, 1, 100, "1000.00"), (2, 2, 101, "250.50"),
               (3, 4, 100, "700.25"), (4, 1, 102, "99.99")]

# Anchored on this file, not on the working directory: a suite that only passes
# when pytest is launched from the repository root passes for a reason nobody
# wrote down.
_VERSIONS = Path(__file__).resolve().parents[2] / "migrations" / "versions"


# ── fixtures ──────────────────────────────────────────────────────────────────


async def _pg_gold(monkeypatch, orders):
    """`{(date, sales_type, source_id): {measure: value}}` — Postgres's Gold,
    derived from Bronze by the derivation's own two rebuilds.

    `orders` is a list of (id, source_id, buyer_id, total). Midday UTC, one day
    back: Silver derives `order_date` as the Kyiv date, and anchoring at midday
    keeps the seeded instant and the derived date on the same day at every
    offset Kyiv has ever used. The neighbouring Gold test was broken three
    hours out of twenty-four for exactly this reason.
    """
    when = datetime.now(timezone.utc).replace(
        hour=12, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)

    async def populate(conn, pool):
        await seed(conn, orders=[(oid, source_id, 12, Decimal(total), when, buyer_id,
                                  None, None)
                                 for oid, source_id, buyer_id, total in orders])

    async with tab_in_postgres(DSN, monkeypatch, populate) as pool:
        await derive(pool)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT date, sales_type, source_id, {', '.join(GOLD_MEASURE_COLUMNS)} "
                f"FROM {GOLD_TABLE}")
    return {(r["date"], r["sales_type"], r["source_id"]):
            {c: r[c] for c in GOLD_MEASURE_COLUMNS} for r in rows}


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

    def test_every_shared_measure_appears_verbatim_in_the_rendering(self):
        """DuckDB's rendering (`GOLD_REVENUE_SELECT_SQL`) went in step 14's
        S14-15; ClickHouse's is held in `tests/unit/test_warehouse_sql.py`."""
        postgres_sql = select_sql()
        for name, expression in GOLD_MEASURES.items():
            assert expression in postgres_sql, f"{name} missing from Postgres Gold"

    def test_postgres_stores_every_shared_measure_and_no_channel_columns(self):
        """The channel columns are what `source_id` replaced. Carrying them as
        well would keep the shape that cannot express the exhibition."""
        assert set(GOLD_MEASURE_COLUMNS) == set(GOLD_MEASURES)
        assert not set(GOLD_MEASURE_COLUMNS) & set(_GOLD_SOURCE_MEASURES)

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

    # `test_duckdb_column_order_matches_its_positional_insert` stood here
    # until step 14's S14-15 deleted DuckDB's positional Gold INSERT.


class TestTheRebuildsWriteShape:
    """The review's A23: Gold was cleared with `TRUNCATE`, which made every
    dashboard read of Gold wait for the rebuild. It is a `DELETE` under a
    bounded `lock_timeout` now, in one transaction with the INSERT."""

    def test_the_statements_in_order(self):
        """Mutation: a `TRUNCATE` back as the clear, or the bound dropped."""
        import re

        from core.pg_gold import CLEAR_SQL, rebuild_gold
        from core.pg_silver import REBUILD_LOCK_TIMEOUT
        from tests.unit.test_pg_silver import _recorded

        conn = _recorded(rebuild_gold)
        assert conn.transactions == 1
        statements = conn.statements
        assert statements[0] == f"SET LOCAL lock_timeout = '{REBUILD_LOCK_TIMEOUT}'"
        assert statements[1] == CLEAR_SQL == f"DELETE FROM {GOLD_TABLE}"
        assert statements[2] == " ".join(insert_sql().split())
        assert not [s for s in statements if re.match(r"(UPDATE|TRUNCATE)\b", s)]

# ── the projection, executed ─────────────────────────────────────────────────


@needs_postgres
class TestTheTwoShapesAgree:
    """Postgres's Gold against DuckDB's on the same orders: the mapping holds."""

    @pytest.mark.asyncio
    async def test_the_rollup_reproduces_every_shared_measure(self, monkeypatch):
        pg = await _pg_gold(monkeypatch, FOUR_ORDERS)

        dk = DUCKDB_GOLD["four_orders"]
        (rollup,) = [v for (_d, s, src), v in pg.items() if src is None and s == "retail"]
        for measure in _GOLD_ROLLUP_MEASURES:
            if measure == "avg_order_value":
                # The one documented difference between the two engines: DuckDB's
                # DECIMAL `/` promotes to DOUBLE, so 2050.74 / 4 = 512.685 stored
                # as 512.68 there and 512.69 in Postgres's exact NUMERIC — the
                # cent `reconcile_gold` forgave (5 cells of 2,106 on production).
                # Until step 14's S14-18 the projection ran on DuckDB here and
                # the two agreed exactly.
                assert abs(_stored(dk[measure]) - _stored(rollup[measure])) <= Decimal(
                    "0.01"), measure
                continue
            assert _stored(dk[measure]) == _stored(rollup[measure]), measure

    @pytest.mark.asyncio
    async def test_the_fine_rows_reproduce_every_channel_column(self, monkeypatch):
        pg = await _pg_gold(monkeypatch, FOUR_ORDERS)

        dk = DUCKDB_GOLD["four_orders"]
        (date,) = {d for (d, s, src) in pg if src is None and s == "retail"}
        for dk_column, (source_id, pg_column) in _GOLD_SOURCE_MEASURES.items():
            fine = pg.get((date, "retail", source_id))
            actual = Decimal(0) if fine is None else Decimal(str(fine[pg_column]))
            assert Decimal(str(dk[dk_column])) == actual, dk_column

    @pytest.mark.asyncio
    async def test_the_exhibition_gets_a_row_where_it_had_no_column(self, monkeypatch):
        """The whole reason the grain changed. Source 5 is ₴266,059.00 of
        production revenue that `SUM(the three channel columns)` cannot see.
        """
        pg = await _pg_gold(monkeypatch, [
            (1, 1, 100, "1000.00"),
            (2, 5, 101, "266.00"),
        ])

        # DuckDB's Gold had revenue for it and every channel column empty —
        # on production ₴266,059.00 across 177 orders a per-channel reading
        # could not see (that half went with DuckDB's Gold in S14-15).
        # Postgres: the channel has a row, and it needed no new column.
        (date,) = {d for (d, s, src) in pg if s == "exhibition" and src is None}
        assert pg[(date, "exhibition", 5)]["revenue"] == Decimal("266.00")

    @pytest.mark.asyncio
    async def test_an_inactive_source_gets_a_row_of_zeroes(self, monkeypatch):
        """Opencart is 2,470 orders and ₴5.8M gross, and none of it is revenue
        as this warehouse defines revenue. A zero row is `is_active_source`
        written out, and DuckDB already does the same one grain up.
        """
        pg = await _pg_gold(monkeypatch, [
            (1, 1, 100, "1000.00"),
            (2, 3, 101, "5000.00"),
        ])

        opencart = [v for (_d, _s, src), v in pg.items() if src == 3]
        assert opencart, "no row for the inactive source at all"
        assert all(Decimal(str(row["revenue"])) == 0 for row in opencart)


@needs_postgres
class TestWhyTheRollUpIsStored:
    """29 cells of 2,107 on production, and this is their shape."""

    @pytest.mark.asyncio
    async def test_summing_the_fine_rows_overstates_the_customer_count(self, monkeypatch):
        # One buyer, two channels, one day.
        pg = await _pg_gold(monkeypatch, [
            (1, 1, 100, "500.00"),
            (2, 2, 100, "500.00"),
        ])

        dk = DUCKDB_GOLD["one_buyer_two_channels"]
        (date, sales_type) = next((d, s) for (d, s, src) in pg if src is None)
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


    @pytest.mark.asyncio
    async def test_the_additive_measures_do_fold(self, monkeypatch):
        """Which is what `gold_rollup_mismatch` is entitled to assert."""
        pg = await _pg_gold(monkeypatch, [
            (1, 1, 100, "500.00"),
            (2, 2, 100, "500.00"),
            (3, 4, 101, "125.25"),
        ])

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


# ── the comparison ───────────────────────────────────────────────────────────


DAY = "2026-08-01"


def _cell(**overrides):
    row = {c: Decimal(0) for c in _GOLD_ROLLUP_MEASURES}
    row.update({k: Decimal(str(v)) for k, v in overrides.items()})
    return row


def _names(issues):
    return {i.check_name for i in issues}


class TestTheRollUpIsCheckedAgainstItsOwnFineRows:
    """The only check that sees sources 3 and 5, which have no DuckDB column.
    Asked of `_gold_internal_findings` directly since step 14's S14-12 deleted
    `compare_gold`, which handed it the same two grains."""

    def test_fine_rows_that_do_not_add_up_are_critical(self):
        rollup = {(DAY, "retail"): _cell(revenue=100)}
        fine = {(DAY, "retail", 1): _cell(revenue=60),
                (DAY, "retail", 5): _cell(revenue=30)}
        issues = _gold_internal_findings(rollup, fine)
        assert "gold_rollup_mismatch" in _names(issues)

    def test_fine_rows_that_add_up_are_silent(self):
        rollup = {(DAY, "retail"): _cell(revenue=100)}
        fine = {(DAY, "retail", 1): _cell(revenue=70),
                (DAY, "retail", 5): _cell(revenue=30)}
        issues = [i for i in _gold_internal_findings(rollup, fine)
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
        issues = [i for i in _gold_internal_findings(rollup, fine)
                  if i.check_name == "gold_rollup_mismatch"]
        assert issues == []


# ── the roll-up check, standing alone (DN-28) ────────────────────────────────


class _Pool:
    """`fetch` returning Gold rows as asyncpg would, keyed by column name."""

    def __init__(self, rows):
        self.rows = rows
        self.sql = []

    async def fetch(self, sql, *args):
        self.sql.append(sql)
        return self.rows


def _gold_row(source_id, **measures):
    row = {c: Decimal(0) for c in _GOLD_ROLLUP_MEASURES}
    row.update({k: Decimal(str(v)) for k, v in measures.items()})
    return {"date": DAY, "sales_type": "retail", "source_id": source_id, **row}


class TestTheRollUpCheckStandsAlone:
    """Step 13 retired `reconcile_gold`, which was the only road to the one
    check that sees sources 3 and 5, and step 14's S14-12 deleted it.
    `pg_gold_internal_check` asks it of Postgres alone, on every run."""

    def _check(self, rows):
        from core.mirror_reconciliation import pg_gold_internal_check

        pool = _Pool(rows)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()) as revision:
            import asyncio

            issues = asyncio.run(pg_gold_internal_check())
        revision.assert_awaited_once()
        return issues, pool

    def test_it_finds_an_injected_roll_up_mismatch(self):
        issues, pool = self._check([
            _gold_row(None, revenue=100, orders_count=2),
            _gold_row(1, revenue=60, orders_count=1),
            _gold_row(5, revenue=30, orders_count=1),
        ])
        (issue,) = issues
        assert (issue.check_name, issue.severity.name, issue.count) == (
            "gold_rollup_mismatch", "CRITICAL", 1)
        assert "revenue" in issue.description and DAY in issue.description
        assert all("gold.daily_revenue" in sql for sql in pool.sql)

    def test_fine_rows_that_add_up_are_silent(self):
        issues, _ = self._check([
            _gold_row(None, revenue=100, orders_count=2),
            _gold_row(1, revenue=70, orders_count=1),
            _gold_row(5, revenue=30, orders_count=1),
        ])
        assert issues == []

    def test_it_reads_no_duckdb(self):
        """Nothing in it may reach the store: after step 13 its Gold is frozen,
        and after a compaction empty."""
        import ast
        import inspect
        import textwrap

        from core.mirror_reconciliation import pg_gold_internal_check

        tree = ast.parse(textwrap.dedent(inspect.getsource(pg_gold_internal_check)))
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | {
            n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        assert not names & {"store", "connection", "fetch_duckdb_gold", "get_store"}

# ── the hook ─────────────────────────────────────────────────────────────────


class TestTheJobRunsIt:
    def test_the_gold_table_is_watched_under_the_same_layer(self):
        """All three comparisons run inside one call, so they cannot have
        different ages and a fourth layer would invent one."""
        from core.data_quality import WATCHED_LAYERS
        from core.mirror_reconciliation import MIRROR_LAYER

        assert MIRROR_LAYER in WATCHED_LAYERS
        assert GOLD_PG_TABLE == GOLD_TABLE
