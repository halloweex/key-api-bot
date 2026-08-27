"""The order-version archive: the one table nothing can rebuild.

Three kinds of test here, and they are not interchangeable.

The **structural** ones parse — never grep. Six times in two days a test looking
for a string in a source file matched the comment explaining why the string was
absent, so every assertion below that is about the shape of the code walks an
AST or reads a rendered constant.

The **behavioural** ones drive `capture_versions` against a fake connection and
check the arithmetic around it.

The **executed** ones need a real PostgreSQL and skip without `KS_PG_DSN`. They
are the only ones that prove the design, because every claim this feature rests
on is a claim about what one SQL statement does to real rows: that an unchanged
order writes nothing, that a status change writes exactly one row, that a moved
`updated_at` on an otherwise identical header writes nothing, and that a NULL
compares as a value rather than poisoning the comparison. Run them with
`KS_PG_DSN=... pytest tests/unit/test_order_versions.py`.
"""
from __future__ import annotations

import ast
import importlib.util
import inspect
import os
from pathlib import Path

import pytest

from core import pg
from core.pg_order_versions import (
    CAPTURE_SQL,
    CARRIED_COLUMNS,
    TABLE,
    VERSIONED_COLUMNS,
    capture_versions,
)

REPO = Path(__file__).resolve().parents[2]
_PATH = REPO / "migrations" / "versions" / "0010_order_versions.py"

_spec = importlib.util.spec_from_file_location("_rev0010", _PATH)
_rev = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_rev)
MIGRATION = inspect.getsource(_rev.upgrade)
DOWN = inspect.getsource(_rev.downgrade)

DSN = os.getenv("KS_PG_DSN", "").strip()
needs_pg = pytest.mark.skipif(not DSN, reason="KS_PG_DSN is not set")


# ─── parsing helpers: none of these may fall back to a substring search ──────

def _declared_columns(ddl: str) -> set:
    """The column names of `CREATE TABLE app.order_versions`, from the DDL.

    Comments are stripped first — the DDL below is more comment than SQL, and
    those comments name every column they explain.
    """
    body = ddl.split(f"CREATE TABLE {TABLE} (", 1)[1]
    depth, end = 1, 0
    for i, ch in enumerate(body):
        depth += (ch == "(") - (ch == ")")
        if depth == 0:
            end = i
            break
    lines = [
        line.split("--", 1)[0].strip()
        for line in body[:end].splitlines()
    ]
    names = set()
    for line in lines:
        if not line or line.startswith(("CONSTRAINT", "PRIMARY", "UNIQUE")):
            continue
        names.add(line.split()[0].strip(","))
    return names


def _mentions(items, attr: str) -> bool:
    """Does any `async with` item call `.<attr>()`?"""
    return any(
        isinstance(n, ast.Attribute) and n.attr == attr
        for item in items
        for n in ast.walk(item.context_expr)
    )


def _calls(node, name: str) -> bool:
    for n in ast.walk(node):
        if not isinstance(n, ast.Call):
            continue
        fn = n.func
        if isinstance(fn, ast.Name) and fn.id == name:
            return True
        if isinstance(fn, ast.Attribute) and fn.attr == name:
            return True
    return False


def _sql_literals(path: Path):
    """Every string literal in a module that is not a docstring.

    The distinction is the whole point: these modules' docstrings discuss
    deleting and updating this table at length, and a search that could not
    tell prose from SQL would match the explanation of why it never happens.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef,
                             ast.FunctionDef, ast.AsyncFunctionDef)):
            first = node.body[0] if node.body else None
            if (isinstance(first, ast.Expr)
                    and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, str)):
                docstrings.add(id(first.value))
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in docstrings):
            yield node.value


# ─────────────────────────────────────────────────────────────────────────────
# structural
# ─────────────────────────────────────────────────────────────────────────────

class TestTheTableIsWhereItsMeaningPutsIt:
    def test_it_lands_in_app_not_bronze(self):
        """`app` is defined as what nothing can decide again. A transition
        KeyCRM cannot serve back is exactly that; `bronze` is the copy of what
        the source holds, and this is not a copy of anything."""
        assert f"CREATE TABLE {TABLE}" in MIGRATION
        assert "CREATE TABLE bronze." not in MIGRATION

    def test_it_creates_no_schema(self):
        assert "CREATE SCHEMA" not in MIGRATION

    def test_it_follows_the_bot_state_revision(self):
        assert _rev.down_revision == "0009_bot_state"
        assert _rev.revision == "0010_order_versions"

    def test_the_application_refuses_a_database_without_it(self):
        """`REQUIRED_REVISION` is what makes deploying `web` before `migrate`
        fail closed instead of writing into a table that is not there.

        The pin moves with the head migration — 0011 since the buyer landing
        and the витрина — and this assertion is the speed bump that makes the
        move a decision rather than a side effect."""
        assert pg.REQUIRED_REVISION == "0011_buyers_lines_vitrina"

    def test_money_keeps_the_scale_it_has_everywhere_else(self):
        """NUMERIC(12,2), as in `bronze.orders` and as DECIMAL(12,2) in DuckDB.
        A wider or narrower scale here would make the comparison report a
        change the rounding invented."""
        assert "NUMERIC(12,2)" in MIGRATION
        assert "double precision" not in MIGRATION.lower()

    def test_there_is_no_foreign_key_to_the_order(self):
        """KeyCRM retires rows — product 1055 is the standing proof one schema
        up. A version whose order is gone is the most interesting row in the
        table; a foreign key would refuse to keep it."""
        assert "REFERENCES" not in MIGRATION


class TestWhatCountsAsAChange:
    def test_the_migration_declares_exactly_the_compared_columns(self):
        """One home for the list. The DDL and `core.pg_order_versions` drifting
        apart is the failure this pins: the capture would compare a column the
        table does not store, or store one it never compares."""
        declared = _declared_columns(MIGRATION)
        bookkeeping = {"id", "order_id", "captured_at", "kind"}
        assert declared - bookkeeping == set(VERSIONED_COLUMNS) | set(CARRIED_COLUMNS)

    def test_updated_at_is_stored_and_not_compared(self):
        """KeyCRM does not move it on a status change, so it cannot be the key.
        And if it moves while every stored column is identical, then what this
        store holds has not changed — a row there would record an observation,
        which is what took `bronze_order_events` to 43 GB."""
        assert "updated_at" in CARRIED_COLUMNS
        assert "updated_at" not in VERSIONED_COLUMNS

    def test_created_at_is_neither_stored_nor_compared(self):
        """An order is created once. Carrying it would put a constant on every
        version of every order."""
        assert "created_at" not in VERSIONED_COLUMNS
        assert "created_at" not in CARRIED_COLUMNS

    def test_line_items_are_absent_entirely(self):
        """`order_status_refresh` runs daily with `skip_products=True` and
        carries no line items at all, so an archive that recorded them would
        minute the deletion of ~1,400 baskets every morning. Header only."""
        for forbidden in ("quantity", "price_sold", "product_id", "order_products"):
            assert forbidden not in MIGRATION, forbidden
            assert forbidden not in CAPTURE_SQL, forbidden


class TestTheCaptureReadsWhatWasWritten:
    def test_it_selects_from_the_stored_row_not_from_a_parameter(self):
        """The decisive one. `manager_comment` is upserted as
        COALESCE(EXCLUDED, stored) and 32,437 of 46,685 orders carry a value,
        so comparing an incoming payload against the last version would report
        "comment removed" on two orders in three, on every tick, forever."""
        assert "FROM bronze.orders o" in CAPTURE_SQL

    def test_it_is_one_statement(self):
        """Read, compare and insert in three round-trips would leave room for
        the header to move in between, and would cost three times as much on
        the 05:15 batch."""
        assert CAPTURE_SQL.strip().count(";") == 0
        assert CAPTURE_SQL.count("INSERT INTO") == 1

    def test_null_compares_as_a_value(self):
        """`manager_id` is NULL on every Shopify order. Under a NULL-unsafe
        comparison each of them would write a version on every tick."""
        assert "IS DISTINCT FROM" in CAPTURE_SQL

    def test_a_first_sighting_is_named_differently_from_a_change(self):
        assert "'create'" in CAPTURE_SQL and "'change'" in CAPTURE_SQL

    def test_the_baseline_is_seeded_by_the_migration_itself(self):
        """An empty archive makes the first row about each order ambiguous —
        not a transition, just whichever state it was in when something first
        touched it. Seeding at creation time makes every later row mean one
        thing."""
        assert "'baseline'" in MIGRATION
        assert "FROM bronze.orders" in MIGRATION

    def test_the_latest_version_lookup_has_an_index_to_serve_it(self):
        assert "(order_id, captured_at DESC, id DESC)" in MIGRATION
        assert "DISTINCT ON (order_id)" in CAPTURE_SQL


class TestItCannotLoseAVersionTheMirrorWouldHaveLost:
    """`mirror_orders` never raises, because the backfill can ship whatever it
    missed. The archive has no backfill and cannot have one, so it must not
    inherit that contract — it has to be inside the transaction that writes the
    row it describes."""

    def test_capture_is_called_inside_the_transaction_block(self):
        from core import pg_landing

        tree = ast.parse(inspect.getsource(pg_landing))
        fn = next(
            n for n in ast.walk(tree)
            if isinstance(n, ast.AsyncFunctionDef) and n.name == "write_orders"
        )
        blocks = [
            n for n in ast.walk(fn)
            if isinstance(n, ast.AsyncWith) and _mentions(n.items, "transaction")
        ]
        assert blocks, "write_orders no longer opens a transaction"
        assert any(
            _calls(block, "capture_versions") for block in blocks
        ), "capture_versions is not inside the transaction that writes the header"

    def test_capture_is_not_called_from_the_swallowing_wrapper(self):
        """`mirror_orders` is the one that catches. A capture there would be a
        version lost quietly, which is the whole failure this design exists to
        prevent."""
        from core import pg_landing

        tree = ast.parse(inspect.getsource(pg_landing))
        fn = next(
            n for n in ast.walk(tree)
            if isinstance(n, ast.AsyncFunctionDef) and n.name == "mirror_orders"
        )
        assert not _calls(fn, "capture_versions")

    def test_it_takes_a_connection_and_never_a_pool(self):
        """A second connection would put the version outside the transaction
        that writes the header — the one thing this must not do."""
        src = inspect.getsource(capture_versions)
        assert "get_pool" not in src
        assert "acquire" not in src


class TestTheSeedIsNotTheWritersOutput:
    """Found on production, not by reading: the first run of the check filed
    `order_versions_flooding` with count=46,695 — the baseline the migration had
    just seeded, all stamped `now()`, counted as a day's writing.

    These read the statements themselves. The comments explaining the exclusion
    necessarily contain the words a source-text search would look for, so a
    grep would have passed with the clause deleted."""

    def test_the_daily_rate_excludes_the_baseline(self):
        from core.mirror_reconciliation import ORDER_VERSIONS_RECENT_SQL

        assert "kind <> 'baseline'" in ORDER_VERSIONS_RECENT_SQL
        assert "captured_at >= $1" in ORDER_VERSIONS_RECENT_SQL

    def test_the_staleness_check_does_not_exclude_it(self):
        """The opposite decision, and deliberate: the baseline is what gives a
        freshly migrated archive its first 24 hours before anyone is asked why
        it is quiet."""
        from core.mirror_reconciliation import ORDER_VERSIONS_NEWEST_SQL

        assert "baseline" not in ORDER_VERSIONS_NEWEST_SQL
        assert "max(captured_at)" in ORDER_VERSIONS_NEWEST_SQL


class TestAppendOnlyIsAPropertyOfTheCodeNotAPromise:
    def test_no_statement_anywhere_updates_or_deletes_the_archive(self):
        """Enforced here rather than by a REVOKE: migrations run as `ks_app`,
        which therefore owns the table, and an owner can grant itself back
        anything it revoked. A REVOKE would look like a guarantee and be a
        speed bump.

        Parsed, not grepped — the docstrings in these very modules discuss
        deleting and updating this table, and a grep would match the sentence
        explaining why it never happens. Only non-docstring string literals are
        examined, which is where SQL lives."""
        offenders = []
        for path in sorted((REPO / "core").rglob("*.py")):
            for literal in _sql_literals(path):
                flat = " ".join(literal.lower().split())
                if f"update {TABLE}" in flat or f"delete from {TABLE}" in flat:
                    offenders.append(path.name)
        assert not offenders, f"append-only violated in: {offenders}"

    def test_the_downgrade_drops_rather_than_truncates(self):
        assert "DROP TABLE" in DOWN
        assert "TRUNCATE" not in DOWN


# ─────────────────────────────────────────────────────────────────────────────
# behavioural
# ─────────────────────────────────────────────────────────────────────────────

class _FakeConn:
    def __init__(self, returning=()):
        self.returning = list(returning)
        self.calls = []

    async def fetch(self, sql, *args):
        self.calls.append((sql, args))
        return self.returning


class TestTheCountItReports:
    @pytest.mark.asyncio
    async def test_it_reports_rows_actually_written(self):
        conn = _FakeConn(returning=[(1,), (2,)])
        assert await capture_versions(conn, [1, 2, 3]) == 2

    @pytest.mark.asyncio
    async def test_an_unchanged_batch_writes_nothing(self):
        """The 05:15 refresh offers ~1,400 ids with force_update=True on a day
        when the reconciliation found zero discrepancies. None of them is a
        change, and none of them may become a row."""
        conn = _FakeConn(returning=[])
        assert await capture_versions(conn, list(range(1400))) == 0

    @pytest.mark.asyncio
    async def test_no_ids_means_no_statement(self):
        conn = _FakeConn()
        assert await capture_versions(conn, []) == 0
        assert conn.calls == []

    @pytest.mark.asyncio
    async def test_it_passes_the_ids_as_one_array_parameter(self):
        conn = _FakeConn()
        await capture_versions(conn, [7, 8])
        (_sql, args), = conn.calls
        assert args == ([7, 8],)


# ─────────────────────────────────────────────────────────────────────────────
# executed — the only ones that prove it
# ─────────────────────────────────────────────────────────────────────────────

@needs_pg
class TestAgainstARealPostgres:
    @pytest.mark.asyncio
    async def test_the_whole_rule(self):
        import asyncpg

        conn = await asyncpg.connect(DSN)
        try:
            # Stand-ins with the production column types, so the comparison
            # under test runs against the types it will meet.
            await conn.execute("""
                CREATE TEMP TABLE bronze_orders (
                    id INTEGER PRIMARY KEY, source_id INTEGER,
                    status_id INTEGER, status_group_id INTEGER,
                    grand_total NUMERIC(12,2), ordered_at TIMESTAMPTZ,
                    buyer_id INTEGER, manager_id INTEGER,
                    manager_comment TEXT, promocode TEXT,
                    updated_at TIMESTAMPTZ)
            """)
            await conn.execute("""
                CREATE TEMP TABLE order_versions (
                    id BIGSERIAL PRIMARY KEY, order_id INTEGER NOT NULL,
                    captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    kind TEXT NOT NULL, source_id INTEGER,
                    status_id INTEGER, status_group_id INTEGER,
                    grand_total NUMERIC(12,2), ordered_at TIMESTAMPTZ,
                    buyer_id INTEGER, manager_id INTEGER,
                    manager_comment TEXT, promocode TEXT,
                    updated_at TIMESTAMPTZ)
            """)
            sql = (CAPTURE_SQL
                   .replace(TABLE, "order_versions")
                   .replace("bronze.orders", "bronze_orders"))

            async def capture():
                return len(await conn.fetch(sql, [1]))

            # A Shopify order: manager_id NULL, which is the case a NULL-unsafe
            # comparison would rewrite on every tick.
            await conn.execute(
                "INSERT INTO bronze_orders VALUES "
                "(1, 4, 12, NULL, 100.00, now(), 5, NULL, 'utm=x', NULL, now())"
            )
            assert await capture() == 1, "a first sighting must be recorded"
            assert await capture() == 0, "an unchanged order must write nothing"
            assert await capture() == 0, "and must keep writing nothing"

            await conn.execute("UPDATE bronze_orders SET status_id = 20 WHERE id = 1")
            assert await capture() == 1, "a status change is the point of the table"
            assert await capture() == 0

            await conn.execute("UPDATE bronze_orders SET updated_at = now() WHERE id = 1")
            assert await capture() == 0, (
                "a moved updated_at with an identical header is an observation, "
                "not a change"
            )

            await conn.execute(
                "UPDATE bronze_orders SET manager_comment = NULL WHERE id = 1")
            assert await capture() == 1, "losing the comment IS a change once stored"

            await conn.execute("UPDATE bronze_orders SET grand_total = 100.00 WHERE id = 1")
            assert await capture() == 0, "same money, different literal, no row"

            kinds = [r[0] for r in await conn.fetch(
                "SELECT kind FROM order_versions ORDER BY id")]
            assert kinds == ["create", "change", "change"]

            total = await conn.fetchval("SELECT count(*) FROM order_versions")
            assert total == 3
        finally:
            await conn.close()
