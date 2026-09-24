"""Which tables have changed hands — one answer, found by walking, not listed.

Stage 4 moves writes chain by chain. When a chain writes Postgres, the hourly
shipper and the daily comparison must both stop touching its tables, and must
stop together. Both ask `core.write_chains.stood_down_tables()`.

The guard below walks `core/` rather than trusting `WRITE_CHAINS`: a guard that
names its subjects guards only the ones somebody remembered (the mirror-spec
guard saw 2 of 7 groups; the volume bound was written into one gate of two; the
transient-key exclusion named a retired key). A new write chain that forgets to
register itself would keep being full-replaced out of a frozen DuckDB — the
silent hourly rollback — and this is what makes that fail in CI instead.
"""
from __future__ import annotations

import ast
import inspect
import pathlib
import textwrap

import pytest

CORE = pathlib.Path(__file__).resolve().parents[2] / "core"


def _declares_a_write_chain(path: pathlib.Path) -> bool:
    """Top-level `CHAIN_TABLES = ...` and `def writes_postgres` — parsed, not
    imported, so walking the package has no side effects."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names.update(t.id for t in targets if isinstance(t, ast.Name))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
    return {"CHAIN_TABLES", "writes_postgres"} <= names


class TestEveryWriteChainIsRegistered:
    def test_the_walk_finds_them_all_and_the_registry_holds_them_all(self):
        from core import write_chains

        found = {p.stem for p in CORE.glob("*.py") if _declares_a_write_chain(p)}
        registered = {m.__name__.rsplit(".", 1)[-1] for m in write_chains.WRITE_CHAINS}
        assert found, "the walk found no write chain — it is not looking"
        assert found == registered, (
            f"declared but not registered: {sorted(found - registered)}; "
            f"registered but not declaring: {sorted(registered - found)}")

    def test_the_walk_is_not_vacuous(self):
        """Both chains that exist today are found, so an empty walk cannot pass."""
        found = {p.stem for p in CORE.glob("*.py") if _declares_a_write_chain(p)}
        assert {"pg_inventory_write", "pg_expenses_write"} <= found


class TestTheShipperAndTheComparisonAskOneAnswer:
    def test_both_sites_call_the_registry_and_neither_spells_a_chain(self):
        from core import mirror_reconciliation, pg_operational

        for fn in (pg_operational.replicate_operational,
                   mirror_reconciliation.reconcile_operational):
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            calls = {n.func.id for n in ast.walk(tree)
                     if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
            # The checked form since DN-01: it never raises on a flag typo and
            # hands the error out to be recorded and reported.
            assert "stood_down_tables_checked" in calls, fn.__name__
            assert "writes_postgres" not in calls, (
                f"{fn.__name__} asks a single chain again — the rule would have two homes")


@pytest.fixture
def flags(monkeypatch):
    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES"):
        monkeypatch.delenv(env, raising=False)
    return monkeypatch


class TestWhatStandsDown:
    def test_nothing_while_every_chain_writes_duckdb(self, flags):
        from core.write_chains import stood_down_tables
        assert stood_down_tables() == frozenset()

    def test_only_the_expenses_table_when_only_chain_8_is_on(self, flags):
        from core.write_chains import stood_down_tables
        flags.setenv("KS_WRITE_EXPENSES", "postgres")
        assert stood_down_tables() == frozenset({"app.manual_expenses"})

    def test_the_union_when_both_are_on(self, flags):
        from core import pg_inventory_write
        from core.write_chains import stood_down_tables
        flags.setenv("KS_WRITE_EXPENSES", "postgres")
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        assert stood_down_tables() == (
            frozenset(pg_inventory_write.CHAIN_TABLES) | {"app.manual_expenses"})

    def test_an_unknown_expenses_value_raises(self, flags):
        from core.pg_expenses_write import writes_postgres
        flags.setenv("KS_WRITE_EXPENSES", "postgre")
        with pytest.raises(RuntimeError):
            writes_postgres()

    def test_the_expenses_table_is_one_the_shipper_actually_replaces(self):
        """Standing down a table the shipper never touched would be a no-op that
        reads as a guarantee."""
        from core.pg_expenses_write import CHAIN_TABLES
        from core.pg_operational import _FULL_REPLACE
        assert set(CHAIN_TABLES) <= {pg for pg, _d, _c, _o in _FULL_REPLACE}


# ─── DN-22a: the order write path asks the registry ──────────────────────────
#
# No chain declares an order table today, so every assertion below that sees
# the path stand down needs a fake chain 3 — the shape `test_chain_invariants`
# already uses for a third chain. Each "it ships nothing" test has a sibling
# without the fake chain that watches the same recorder fill up: a recorder that
# could not see a write would pass every stand-down test and prove nothing.

ORDERS = "bronze.orders"
LINES = "bronze.order_products"
WHEN = "2026-08-20T12:00:00+00:00"


def _order(order_id, products=2):
    return {
        "id": order_id, "source_id": 1, "status_id": 12, "status_group_id": 4,
        "grand_total": "100.00", "ordered_at": WHEN, "created_at": WHEN,
        "updated_at": WHEN, "buyer": {"id": 500 + order_id},
        "manager": {"id": 4}, "manager_comment": None, "promocode": None,
        "products": [
            {"name": f"Товар {i}", "quantity": 1, "price_sold": "50.00",
             "offer": {"product_id": 700 + i}}
            for i in range(products)
        ],
    }


class _Ctx:
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *exc):
        return False


class _Conn:
    """Answers every read with nothing and remembers every statement."""

    def __init__(self, recorder):
        self.recorder = recorder

    def transaction(self):
        return _Ctx()

    async def execute(self, sql, *args):
        self.recorder.sql.append(sql)
        return "UPDATE 1"

    async def executemany(self, sql, rows):
        self.recorder.sql.append(sql)

    async def fetch(self, sql, *args):
        self.recorder.sql.append(sql)
        if "meta.chain_watermarks" in sql:
            # The audit copy of the latch (`chain_latch.read_owners`).
            if self.recorder.owner_error is not None:
                raise self.recorder.owner_error
            return [{"key": f"owner:{table}", "value": at}
                    for table, at in self.recorder.owner_rows.items()]
        return []

    async def fetchval(self, sql, *args):
        self.recorder.sql.append(sql)
        return None

    async def fetchrow(self, sql, *args):
        self.recorder.sql.append(sql)
        return None


class _RecordingPool:
    def __init__(self):
        self.sql = []
        self.acquired = 0
        # `{table: latched_at}` as `meta.chain_watermarks` holds it, and what
        # reading it raises instead, when set.
        self.owner_rows = {}
        self.owner_error = None

    def acquire(self):
        self.acquired += 1
        return _Ctx(_Conn(self))

    def wrote(self, table: str) -> bool:
        return any(table in s and "INSERT INTO" in s for s in self.sql)

    def only_asked_who_owns(self) -> bool:
        """Read the owner rows and nothing else: no order table, no watermark,
        no write — and so no `app.order_versions` insert either."""
        return bool(self.sql) and all("meta.chain_watermarks" in s for s in self.sql)


@pytest.fixture
def pool(flags):
    """The recording pool behind `core.pg.get_pool`, with the mirror on."""
    from unittest.mock import AsyncMock

    from core import pg_landing

    flags.delenv(pg_landing.MIRROR_ENV, raising=False)
    pg_landing.reset_health()
    recorder = _RecordingPool()
    flags.setattr("core.pg.get_pool", AsyncMock(return_value=recorder))
    flags.setattr("core.pg.require_revision", AsyncMock())
    yield recorder
    pg_landing.reset_health()


def _never_reached_postgres(pool) -> None:
    """The local stand-down asked Postgres nothing at all. A pool that was
    never acquired is not enough to say so: `get_pool()` hands back the pool
    without acquiring anything, and `require_revision()` is patched out here,
    so either could run before the refusal and leave `acquired` at zero."""
    from core import pg

    assert pool.acquired == 0
    pg.get_pool.assert_not_awaited()
    pg.require_revision.assert_not_awaited()


@pytest.fixture
def order_chain(flags):
    """Register a fake chain that owns `tables`; its flag is `env()`."""
    import types

    from core import write_chains

    def register(tables=(ORDERS, LINES), env=lambda: True):
        fake = types.ModuleType("core.pg_orders_write")
        fake.WRITE_ENV = "KS_WRITE_ORDERS"
        fake.CHAIN_TABLES = tuple(tables)
        fake.env_writes_postgres = env
        flags.setattr(write_chains, "WRITE_CHAINS",
                      write_chains.WRITE_CHAINS + (fake,))
        return fake

    return register


async def _store(tmp_path, ids=()):
    """A DuckDB holding `ids`, written with the mirror patched out."""
    from unittest.mock import AsyncMock, patch

    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / "dn22a.duckdb")
    await store.connect()
    if ids:
        with patch("core.pg_landing.mirror_orders", new=AsyncMock()):
            await store.upsert_orders([_order(i) for i in ids])
    return store


def _chains_splitting_the_order_tables(chains) -> list:
    """The chains that declare one order table and not the other."""
    from core.write_chains import chain_name

    both = {ORDERS, LINES}
    return [chain_name(c) for c in chains
            if both & set(c.CHAIN_TABLES) and not both <= set(c.CHAIN_TABLES)]


class TestOwnershipOfTheOrderTablesPassesAsAUnit:
    """Why either order table stands both down. Not that they ship together —
    `write_orders(replace_products=False)` ships headers alone every day, from
    the 05:15 status refresh and the comment ship — but that whoever takes the
    headers takes their line items: a chain declares both or neither. Walked
    over the real registry, which `TestEveryWriteChainIsRegistered` holds equal
    to every module that declares a chain."""

    def test_no_registered_chain_declares_one_without_the_other(self):
        from core.write_chains import WRITE_CHAINS

        assert _chains_splitting_the_order_tables(WRITE_CHAINS) == []

    def test_the_check_sees_a_split_when_there_is_one(self):
        """So the empty answer above is a finding, not a check that cannot fail."""
        import types

        for tables in ((ORDERS,), (LINES,), (LINES, "app.something_else")):
            fake = types.ModuleType("core.pg_orders_write")
            fake.CHAIN_TABLES = tables
            assert _chains_splitting_the_order_tables((fake,)) == ["pg_orders_write"]
        whole = types.ModuleType("core.pg_orders_write")
        whole.CHAIN_TABLES = (ORDERS, LINES)
        assert _chains_splitting_the_order_tables((whole,)) == []


class TestTheOrderTablesAskOnlyTheirOwnChain:
    def test_nothing_stands_down_in_production_today(self, flags):
        """KS_WRITE_EXPENSES=postgres is live; its table is not an order table."""
        from core.pg_landing import order_tables_stood_down

        flags.setenv("KS_WRITE_EXPENSES", "postgres")
        assert order_tables_stood_down() == frozenset()

    def test_a_chain_that_declares_no_order_table_is_not_even_asked(
            self, flags, order_chain, caplog):
        """DN-01's rule, carried from sync keys to tables: an unrelated typo is
        not read, so the sync cannot log it once a minute."""
        from core.pg_landing import order_tables_stood_down

        asked = []
        order_chain(tables=("app.something_else",),
                    env=lambda: asked.append(1) or True)
        flags.setenv("KS_WRITE_EXPENSES", "postgre")
        with caplog.at_level("ERROR", logger="core.write_chains"):
            assert order_tables_stood_down() == frozenset()
        assert asked == []
        assert not caplog.records

    @pytest.mark.parametrize("tables", [(ORDERS,), (LINES,), (ORDERS, LINES)])
    def test_a_chain_on_either_table_is_seen(self, order_chain, tables):
        from core.pg_landing import order_tables_stood_down

        order_chain(tables=tables)
        assert order_tables_stood_down() == frozenset(tables)

    def test_the_owning_chains_typo_stands_it_down_and_does_not_raise(
            self, order_chain, caplog):
        from core.pg_landing import order_tables_stood_down

        def typo():
            raise RuntimeError("KS_WRITE_ORDERS='postgre' is not understood")

        order_chain(env=typo)
        with caplog.at_level("ERROR", logger="core.write_chains"):
            assert order_tables_stood_down() == frozenset({ORDERS, LINES})
        assert "postgre" in caplog.text

    def test_a_latched_owning_chain_stands_down_whatever_its_flag(self, order_chain):
        from core import chain_latch
        from core.pg_landing import order_tables_stood_down

        order_chain(env=lambda: False)
        chain_latch.MARKER_DIR.mkdir(parents=True, exist_ok=True)
        chain_latch.marker_path("pg_orders_write").write_text(
            '{"latched_at": "2026-09-18T12:00:00+00:00"}', encoding="utf-8")
        chain_latch.load()
        assert order_tables_stood_down() == frozenset({ORDERS, LINES})

    @pytest.mark.parametrize("config", ["none", "expenses", "inventory", "orders",
                                        "orders_typo", "orders_off"])
    def test_it_is_the_shippers_answer_narrowed(self, flags, order_chain, config):
        """One rule, two doors: the narrow question may never disagree with
        the whole one about a table they both name."""
        from core import pg_inventory_write
        from core.write_chains import stood_down_among, stood_down_tables

        def typo():
            raise RuntimeError("not understood")

        if config == "expenses":
            flags.setenv("KS_WRITE_EXPENSES", "postgres")
        elif config == "inventory":
            flags.setenv("KS_WRITE_INVENTORY", "postgres")
        elif config == "orders":
            order_chain()
        elif config == "orders_typo":
            order_chain(env=typo)
        elif config == "orders_off":
            order_chain(env=lambda: False)

        for asked in ({ORDERS, LINES}, {"app.manual_expenses", ORDERS},
                      set(pg_inventory_write.CHAIN_TABLES)):
            assert stood_down_among(asked) == stood_down_tables() & asked, asked


class TestTheSyncMirror:
    @pytest.mark.asyncio
    async def test_without_a_chain_the_recorder_sees_the_rows_and_the_archive(
            self, pool, tmp_path):
        """The control. Nothing below means anything unless this recorder can
        see a mirror write and a version capture when they happen."""
        from core.pg_order_versions import TABLE

        store = await _store(tmp_path)
        await store.upsert_orders([_order(1)])
        assert pool.wrote(ORDERS) and pool.wrote(LINES)
        assert pool.wrote(TABLE)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("tables", [(ORDERS,), (LINES,)])
    async def test_either_table_owned_ships_nothing_at_all(
            self, pool, order_chain, tmp_path, tables):
        """Ownership of the order tables passes as a unit, so a chain on either
        one stops both — and DuckDB's own write is untouched. No real chain may
        declare only one (`TestOwnershipOfTheOrderTablesPassesAsAUnit`); this
        is the stand-down holding if one ever did."""
        order_chain(tables=tables)
        store = await _store(tmp_path)
        result = await store.upsert_orders([_order(1)])

        assert result.changed_ids == [1]
        assert pool.acquired == 0 and pool.sql == []
        async with store.connection() as conn:
            assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 1

    @pytest.mark.asyncio
    async def test_a_status_refresh_ships_nothing_either(
            self, pool, order_chain, tmp_path):
        """The 05:15 shape: force_update, headers only — the path that would
        otherwise archive 1,400 forced rewrites against the chain's rows."""
        store = await _store(tmp_path, [1])
        order_chain()
        result = await store.upsert_orders(
            [_order(1)], force_update=True, skip_products=True)
        assert result.changed_ids == [1]
        assert pool.acquired == 0


class TestTheBackfillAndItsRepair:
    @pytest.mark.asyncio
    async def test_without_a_chain_it_writes(self, pool, tmp_path):
        from core.pg_backfill import backfill_orders
        from core.pg_order_versions import TABLE

        store = await _store(tmp_path, [1, 2])
        result = await backfill_orders(store)
        assert result["orders_shipped"] == 2
        assert pool.wrote(ORDERS) and pool.wrote(TABLE)

    @pytest.mark.asyncio
    async def test_the_ids_diff_and_the_header_only_repair_refuse_before_postgres(
            self, pool, order_chain, tmp_path):
        from core.pg_backfill import backfill_orders

        store = await _store(tmp_path, [1, 2])
        order_chain()
        with pytest.raises(RuntimeError, match="write chain"):
            await backfill_orders(store)
        _never_reached_postgres(pool)

    @pytest.mark.asyncio
    async def test_the_hourly_diff_stands_down_quietly(
            self, pool, order_chain, tmp_path, caplog):
        """A decision, not a fault: no ERROR every hour."""
        from core.pg_backfill import hourly_orders_ids_diff

        store = await _store(tmp_path, [1])
        order_chain(tables=(LINES,))
        with caplog.at_level("ERROR"):
            result = await hourly_orders_ids_diff(store)
        assert result == {"stood_down": [LINES]}
        _never_reached_postgres(pool)
        assert not [r for r in caplog.records if r.levelname == "ERROR"]


class TestTheCommentShip:
    @pytest.mark.asyncio
    async def test_without_a_chain_it_writes_a_backfill_version(self, pool, tmp_path):
        from core.pg_backfill import ship_orders_by_id
        from core.pg_order_versions import BACKFILL, TABLE

        store = await _store(tmp_path, [1])
        result = await ship_orders_by_id(store, [1], version_kind=BACKFILL)
        assert result["orders_shipped"] == 1
        assert pool.wrote(ORDERS) and pool.wrote(TABLE)

    @pytest.mark.asyncio
    async def test_it_is_skipped_so_the_callers_duckdb_half_still_runs(
            self, pool, order_chain, tmp_path):
        from core.pg_backfill import ship_orders_by_id
        from core.pg_order_versions import BACKFILL

        store = await _store(tmp_path, [1])
        order_chain()
        result = await ship_orders_by_id(store, [1], version_kind=BACKFILL)
        assert result["orders_shipped"] == 0
        assert result["stood_down"] == [LINES, ORDERS]
        assert "write chain" in result["skipped"]
        _never_reached_postgres(pool)


class _NoStore:
    """A DuckDB the comparison must not open once it has stood down."""

    def connection(self):  # pragma: no cover - the assertion is the point
        raise AssertionError("the stood-down comparison read DuckDB")


class TestTheBucketComparison:
    @pytest.mark.asyncio
    async def test_it_files_info_for_both_tables_and_reads_neither_store(
            self, pool, order_chain):
        from core.data_quality import Severity
        from core.mirror_reconciliation import reconcile_orders

        order_chain(tables=(ORDERS,))
        issues = await reconcile_orders(_NoStore())

        assert [(i.check_name, i.table_name, i.severity) for i in issues] == [
            ("mirror_stood_down", ORDERS, Severity.INFO),
            ("mirror_stood_down", LINES, Severity.INFO),
        ]
        assert ORDERS in issues[0].description
        _never_reached_postgres(pool)

    @pytest.mark.asyncio
    async def test_without_a_chain_it_compares(self, pool, tmp_path):
        """The control: the same call reads both stores and files no stand-down."""
        from core.mirror_reconciliation import reconcile_orders

        store = await _store(tmp_path, [1])
        issues = await reconcile_orders(store)
        assert pool.acquired > 0
        assert "mirror_stood_down" not in {i.check_name for i in issues}

    def test_its_lever_is_not_the_generic_mirror_advice(self):
        """`mirror_` says "wait for the re-ship", which is the one thing the
        stand-down exists to prevent."""
        from core.data_quality import remediation_for

        assert remediation_for(["mirror_stood_down"]) != remediation_for(
            ["mirror_missing_rows"])
        assert "never backfill" in remediation_for(["mirror_stood_down"])[0].lower()


class TestTheOwnerRowsStandTheOrderTablesDownToo:
    """DN-06's rule on the order paths that already hold a pool: stand down on
    either copy of the latch. The marker is the copy a lost `./data` loses;
    the owner row in Postgres is the one that survives it, beside the rows the
    chain wrote. Every case below has the flag at `duckdb` and NO marker, so
    the local answer is empty and only the owner row can say the tables moved.
    Each "writes nothing" is watched by the same recorder whose controls above
    see the rows and the version capture when they do happen."""

    @pytest.fixture
    def owned(self, pool, order_chain):
        from core import chain_latch
        from core.pg_landing import order_tables_stood_down

        order_chain(env=lambda: False)
        pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        assert not chain_latch.latched("pg_orders_write"), "the marker must be absent"
        assert order_tables_stood_down() == frozenset(), "the local answer must be empty"
        return pool

    @pytest.mark.asyncio
    async def test_one_owner_row_holds_both_order_tables(self, owned):
        """Ownership passes for a chain as a unit (`claimed_tables`)."""
        from core.pg_landing import order_tables_stood_down_or_owned

        assert await order_tables_stood_down_or_owned(owned) == frozenset({ORDERS, LINES})
        assert owned.only_asked_who_owns()

    @pytest.mark.asyncio
    async def test_another_chains_owner_row_is_not_an_order_table(self, pool):
        from core.pg_landing import order_tables_stood_down_or_owned

        pool.owner_rows = {"app.manual_expenses": "2026-09-20T08:00:00+00:00"}
        assert await order_tables_stood_down_or_owned(pool) == frozenset()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("row", [ORDERS, LINES])
    async def test_an_owner_row_no_registered_chain_declares_still_holds_both(
            self, pool, row):
        """An image rolled back to a build older than the orders chain: the
        owner row names a table no chain here declares, so `claimed_tables`
        alone would drop it and hand the tables back to DuckDB. Either order
        table owned holds both, the unit rule with no chain left to say it."""
        from core.pg_landing import (
            order_tables_stood_down, order_tables_stood_down_or_owned,
        )
        from core.write_chains import WRITE_CHAINS

        assert not [c for c in WRITE_CHAINS if {ORDERS, LINES} & set(c.CHAIN_TABLES)], (
            "the case needs a build in which no chain declares an order table")
        pool.owner_rows = {row: "2026-09-20T08:00:00+00:00"}
        assert order_tables_stood_down() == frozenset()
        assert await order_tables_stood_down_or_owned(pool) == frozenset({ORDERS, LINES})
        assert pool.only_asked_who_owns()

    @pytest.mark.asyncio
    async def test_after_a_rollback_the_backfill_still_refuses(self, pool, tmp_path):
        """The same case through a path: nothing shipped over the chain's rows."""
        from core.pg_backfill import backfill_orders

        pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        store = await _store(tmp_path, [1, 2])
        with pytest.raises(RuntimeError, match="write chain"):
            await backfill_orders(store)
        assert pool.only_asked_who_owns()

    @pytest.mark.asyncio
    async def test_the_local_answer_is_still_part_of_it(self, pool, order_chain):
        from core.pg_landing import order_tables_stood_down_or_owned

        order_chain()          # the flag says postgres; no owner row yet
        assert await order_tables_stood_down_or_owned(pool) == frozenset({ORDERS, LINES})

    @pytest.mark.asyncio
    async def test_an_unreadable_owner_row_is_not_an_absent_one(self, pool, order_chain):
        """No new failure policy: the read raises through, as it does in
        `replicate_operational` and `reconcile_operational`."""
        from core.pg_landing import order_tables_stood_down_or_owned

        order_chain(env=lambda: False)
        pool.owner_error = RuntimeError("meta.chain_watermarks unreadable")
        with pytest.raises(RuntimeError, match="unreadable"):
            await order_tables_stood_down_or_owned(pool)

    @pytest.mark.asyncio
    async def test_the_backfill_refuses_and_writes_nothing(self, owned, tmp_path):
        from core import pg
        from core.pg_backfill import backfill_orders

        store = await _store(tmp_path, [1, 2])
        with pytest.raises(RuntimeError, match="write chain"):
            await backfill_orders(store)
        assert owned.only_asked_who_owns()
        pg.require_revision.assert_awaited()

    @pytest.mark.asyncio
    async def test_the_hourly_diff_stands_down_quietly(self, owned, tmp_path, caplog):
        from core.pg_backfill import hourly_orders_ids_diff

        store = await _store(tmp_path, [1])
        with caplog.at_level("ERROR"):
            result = await hourly_orders_ids_diff(store)
        assert result == {"stood_down": [LINES, ORDERS]}
        assert owned.only_asked_who_owns()
        assert not [r for r in caplog.records if r.levelname == "ERROR"]

    @pytest.mark.asyncio
    async def test_an_unreadable_owner_row_is_the_hourly_diffs_error_not_a_ship(
            self, pool, order_chain, tmp_path):
        """The job's own contract — returned, never raised — and nothing
        shipped on the strength of a read that failed."""
        from core.pg_backfill import hourly_orders_ids_diff

        order_chain(env=lambda: False)
        pool.owner_error = RuntimeError("meta.chain_watermarks unreadable")
        store = await _store(tmp_path, [1])
        result = await hourly_orders_ids_diff(store)
        assert "unreadable" in result["error"]
        assert pool.only_asked_who_owns()

    @pytest.mark.asyncio
    async def test_the_comment_ship_is_skipped_and_writes_nothing(self, owned, tmp_path):
        from core.pg_backfill import ship_orders_by_id
        from core.pg_order_versions import BACKFILL

        store = await _store(tmp_path, [1])
        result = await ship_orders_by_id(store, [1], version_kind=BACKFILL)
        assert result["orders_shipped"] == 0
        assert result["stood_down"] == [LINES, ORDERS]
        assert "write chain" in result["skipped"]
        assert owned.only_asked_who_owns()

    @pytest.mark.asyncio
    async def test_the_bucket_comparison_files_info_and_reads_no_order_table(self, owned):
        from core.data_quality import Severity
        from core.mirror_reconciliation import reconcile_orders

        issues = await reconcile_orders(_NoStore())
        assert [(i.check_name, i.table_name, i.severity) for i in issues] == [
            ("mirror_stood_down", ORDERS, Severity.INFO),
            ("mirror_stood_down", LINES, Severity.INFO),
        ]
        assert owned.only_asked_who_owns()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", [
        "backfill_orders", "hourly_orders_ids_diff", "reconcile_orders",
        "ship_orders_by_id",
    ])
    async def test_a_schema_behind_the_code_breaks_before_the_owner_read(
            self, owned, path):
        """The owner rows live in `meta.chain_watermarks` (revision 0032), so
        each path asks `require_revision()` first and a database behind the
        code breaks as `SchemaVersionError` — never as a missing table, and
        never as a stand-down read off a schema nobody verified. Here the owner
        row is present, so a path that read it first would stand down or skip
        instead of surfacing the error; and a path that dropped the revision
        check altogether would do the same. `assert_awaited` alone could see
        neither: the patched check passes in any position."""
        from core import pg
        from core.mirror_reconciliation import reconcile_orders
        from core.pg_backfill import (
            backfill_orders, hourly_orders_ids_diff, ship_orders_by_id,
        )
        from core.pg_order_versions import BACKFILL

        pg.require_revision.side_effect = pg.SchemaVersionError(
            "database at 0031, code wants 0033")
        calls = {
            "backfill_orders": lambda: backfill_orders(_NoStore()),
            "hourly_orders_ids_diff": lambda: hourly_orders_ids_diff(_NoStore()),
            "reconcile_orders": lambda: reconcile_orders(_NoStore()),
            "ship_orders_by_id": lambda: ship_orders_by_id(
                _NoStore(), [1], version_kind=BACKFILL),
        }
        if path == "hourly_orders_ids_diff":
            # The job's never-raises contract: the error is returned.
            result = await calls[path]()
            assert result.get("error", "").startswith("SchemaVersionError"), result
        else:
            with pytest.raises(pg.SchemaVersionError):
                await calls[path]()
        pg.require_revision.assert_awaited_once()
        assert owned.sql == [] and owned.acquired == 0, owned.sql


class TestTheStandDownFindingSaysWhetherTheSyncStillShips:
    """The INFO finding is read by the one person who could act, so its claim
    about the sync's mirror is parsed out of it and checked against what the
    mirror then does with the same recorder. On the local answer the mirror
    has stopped; on the owner rows alone it has not — it asks only the local
    answer — and a finding saying it had would read as nothing to do."""

    @staticmethod
    def _claim(issues) -> str:
        """The one description both order tables carry."""
        assert [(i.check_name, i.table_name) for i in issues] == [
            ("mirror_stood_down", ORDERS), ("mirror_stood_down", LINES)]
        (text,) = {i.description for i in issues}
        stopped = "no longer ships DuckDB's copy" in text
        shipping = "the sync mirror is still shipping" in text
        assert stopped != shipping, f"says neither or both: {text}"
        return text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["flagged", "marker_lost", "rolled_back"])
    async def test_the_claim_is_what_the_sync_mirror_does(
            self, pool, order_chain, tmp_path, state):
        from core.mirror_reconciliation import reconcile_orders

        if state == "flagged":
            order_chain()                       # the local answer names both
        else:
            if state == "marker_lost":
                order_chain(env=lambda: False)  # declared, flag duckdb, no marker
            pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        text = self._claim(await reconcile_orders(_NoStore()))

        pool.sql.clear()
        store = await _store(tmp_path)
        await store.upsert_orders([_order(9)])
        assert pool.wrote(ORDERS) == ("the sync mirror is still shipping" in text), text

        if state == "flagged":
            assert "owner row" not in text
        elif state == "marker_lost":
            assert ("the marker is missing; the sync mirror is still shipping — "
                    "restore data/write-chain-owners or run "
                    "scripts/chain_copy_back.py") in text
        else:
            assert "the marker is missing" not in text
            assert "no chain in this build declares the order tables" in text


class TestTheAdminBackfill:
    def _client(self, flags):
        import time as _time

        from fastapi.testclient import TestClient

        from core.permissions import ADMIN_USER_IDS
        from web.main import app
        from web.routes.api._deps import limiter
        from web.routes.auth import (
            SESSION_COOKIE, create_session_data, session_serializer,
        )

        limiter.reset()
        admin_id = sorted(ADMIN_USER_IDS)[0]

        async def _resolve(session):
            return {"user_id": admin_id, "role": "admin"}

        flags.setattr("web.routes.auth._resolve_session", _resolve)
        client = TestClient(app)
        client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data(
            {"id": str(admin_id), "first_name": "T", "last_name": "U",
             "username": "t", "auth_date": str(int(_time.time()))}, role="admin",
        )))
        return client

    @pytest.fixture
    def backfill(self, flags):
        from unittest.mock import AsyncMock

        run = AsyncMock(return_value={"complete": True})
        flags.setattr("core.pg_backfill.backfill_orders", run)
        flags.setattr("web.routes.api.admin.get_store", AsyncMock(return_value=object()))
        return run

    REFUSAL = (f"{LINES}, {ORDERS} is written by a write chain, not shipped out "
               "of DuckDB")

    def _post(self, flags, background):
        return self._client(flags).post(
            f"/api/mirror/backfill/orders?background={background}")

    @pytest.mark.parametrize("background", ["true", "false"])
    def test_409_before_anything_starts(
            self, flags, pool, order_chain, backfill, background):
        order_chain()
        res = self._post(flags, background)
        assert res.status_code == 409
        assert res.json()["detail"].startswith(self.REFUSAL)
        backfill.assert_not_called()
        _never_reached_postgres(pool)

    @pytest.mark.parametrize("background", ["true", "false"])
    def test_409_on_the_owner_rows_when_the_marker_is_lost(
            self, flags, pool, order_chain, backfill, background):
        """Flag at duckdb, no marker, an owner row in Postgres: the local
        answer is empty, and "started" here was the backfill refusing into a
        log line nobody reads (or a 500 in the foreground)."""
        order_chain(env=lambda: False)
        pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        res = self._post(flags, background)
        assert res.status_code == 409, res.json()
        assert res.json()["detail"].startswith(self.REFUSAL)
        backfill.assert_not_called()
        assert pool.only_asked_who_owns()

    @pytest.mark.parametrize("background", ["true", "false"])
    def test_an_unreadable_owner_row_is_a_503_never_started(
            self, flags, pool, order_chain, backfill, background):
        order_chain(env=lambda: False)
        pool.owner_error = RuntimeError("meta.chain_watermarks unreadable")
        res = self._post(flags, background)
        assert res.status_code == 503, res.json()
        assert res.json().get("status") != "started"
        assert "unreadable" in res.json()["detail"]
        backfill.assert_not_called()

    @pytest.mark.parametrize("background", ["true", "false"])
    def test_a_schema_behind_the_code_is_a_503_before_the_owner_read(
            self, flags, pool, backfill, background):
        from core import pg

        pg.require_revision.side_effect = pg.SchemaVersionError(
            "database at 0031, code wants 0033")
        pool.owner_rows = {ORDERS: "2026-09-20T08:00:00+00:00"}
        res = self._post(flags, background)
        assert res.status_code == 503, res.json()
        assert "SchemaVersionError" in res.json()["detail"]
        assert pool.sql == [] and pool.acquired == 0
        backfill.assert_not_called()

    def test_with_the_mirror_off_it_asks_postgres_nothing(self, flags, pool, backfill):
        """The backfill refuses a switched-off mirror for its own reason; the
        route adds no Postgres read in front of that refusal."""
        from core.pg_landing import MIRROR_ENV

        flags.setenv(MIRROR_ENV, "0")
        self._post(flags, "false")
        _never_reached_postgres(pool)

    def test_without_a_chain_it_runs(self, flags, pool, backfill):
        res = self._post(flags, "false")
        assert res.status_code == 200
        backfill.assert_awaited_once()
        assert pool.only_asked_who_owns()


class TestEveryOrderShipperAsksFirst:
    """Walked, not listed. A function that ships DuckDB's orders to Postgres —
    it calls `write_orders` or `mirror_orders` — must ask
    `order_tables_stood_down` itself. `mirror_orders` is the one exemption: it
    is the wrapper every such caller reaches, and each of those is walked."""

    SHIPPERS = {"write_orders", "mirror_orders"}
    EXEMPT = {("core/pg_landing.py", "mirror_orders")}

    @staticmethod
    def _called(fn) -> set:
        names = set()
        for node in ast.walk(fn):
            if isinstance(node, ast.Call):
                f = node.func
                if isinstance(f, ast.Name):
                    names.add(f.id)
                elif isinstance(f, ast.Attribute):
                    names.add(f.attr)
        return names

    def _sites(self):
        root = CORE.parent
        for folder in ("core", "web", "scripts"):
            for path in sorted((root / folder).rglob("*.py")):
                rel = path.relative_to(root).as_posix()
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for fn in ast.walk(tree):
                    if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    if (rel, fn.name) in self.EXEMPT:
                        continue
                    called = self._called(fn)
                    if called & self.SHIPPERS:
                        yield rel, fn.name, called

    def test_each_one_asks(self):
        missing = [(rel, name) for rel, name, called in self._sites()
                   if "order_tables_stood_down" not in called]
        assert not missing, f"ships orders without asking the registry: {missing}"

    def test_the_walk_is_not_vacuous(self):
        found = {name for _rel, name, _called in self._sites()}
        assert {"upsert_orders", "backfill_orders", "ship_orders_by_id"} <= found

    def test_one_that_takes_the_pool_asks_the_owner_rows_too(self):
        """DN-06: anything already holding a Postgres connection stands down on
        either copy of the latch. The sync's mirror in `upsert_orders` takes
        no pool — the write path, where that read is the one to avoid — and so
        is not held to it."""
        holding = [(rel, name, called) for rel, name, called in self._sites()
                   if "get_pool" in called]
        assert {"backfill_orders", "ship_orders_by_id"} <= {n for _r, n, _c in holding}
        missing = [(rel, name) for rel, name, called in holding
                   if "order_tables_stood_down_or_owned" not in called]
        assert not missing, f"holds a pool but reads only the local latch: {missing}"
