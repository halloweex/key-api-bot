"""The write chains' registry, and the walk that finds every Postgres writer.

Stage 4 moved WRITES chain by chain, and until step 14's S14-13 everything
that shipped a chain's tables out of DuckDB — the mirrors, the backfills, the
hourly ids-diffs and copies — had to ask the registry first, and a walk
(DN-22a/b) failed on any Postgres writer of `bronze.*`/`app.*` that did not.
S14-13 deleted every such shipper (`core.pg_backfill`, `pg_expense_backfill`,
`pg_buyers`, `pg_replication`, `pg_operational`, the mirror's half of
`pg_landing`), and the question they asked (`tables_stood_down*`) with them,
so the tests of asking went too.

What stays: that every module declaring a chain is registered; the walk
itself, and the two things it still holds — the order tables and the archive
have exactly one writer, and the `/sms` and access store helpers write
Postgres alone and ask no switch (S14-08); and that a chain declares both
order tables or neither.
"""
from __future__ import annotations

import ast
import pathlib

import pytest
from tests.helpers.write_walk import (
    CORE, _CAPTURE_WALKED, _Module, _Walk, _executors, _pg_write_targets,
)


def _declares_a_write_chain(path: pathlib.Path) -> bool:
    """Top-level `CHAIN_TABLES = ...` and `def _latch` — parsed, not
    imported, so walking the package has no side effects. (`writes_postgres`
    until step 14's S14-11 deleted it with the flags; every chain still takes
    its latch on its first write.)"""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names.update(t.id for t in targets if isinstance(t, ast.Name))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
    return {"CHAIN_TABLES", "_latch"} <= names


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
        """Every chain that exists today is found, so an empty walk cannot pass
        — chain 7a (DN-25) included, which is the one this walk must see for
        the registry above to be proven rather than assumed."""
        found = {p.stem for p in CORE.glob("*.py") if _declares_a_write_chain(p)}
        assert {"pg_inventory_write", "pg_expenses_write", "pg_goals_write",
                "pg_expense_types_write"} <= found


# ─── The walk: every function that writes bronze.* or app.* in Postgres ─────
#
# Walked, not listed. The walk finds every function in core/, web/ and
# scripts/ that executes a Postgres write naming a `bronze.` or `app.` table,
# or one whose target it cannot read (`INSERT INTO {table}`: a guard that could
# not resolve a name must not read it as "not ours"). Two shapes the DN-22b
# review found are read: a helper that transforms the SQL it is handed before
# executing it (`conn.execute(numbered(sql))`, or handing it on to another such
# helper — `_users_run` → `execute(rendered)`), whose caller's literal names the
# target; and `copy_records_to_table`/`copy_to_table`, which carry no SQL at all
# and are read by their `schema_name` — an unresolved or absent one counts.


@pytest.fixture(scope="module")
def walk():
    return _Walk()


def _chain_modules() -> set:
    from core.write_chains import WRITE_CHAINS

    return {c.__name__.replace(".", "/") + ".py" for c in WRITE_CHAINS}


class TestTheWalk:
    def test_the_walk_is_not_vacuous(self, walk):
        """A walk that found nothing, or stopped resolving what it found,
        would pass every test that reads it. The writers every chain 3–4
        table goes through, and the archive's capture. (At least 27 writers
        were found before step 14's S14-13 deleted the shippers and the copies
        among them.)"""
        writers = walk.writers()
        assert {
            ("core/pg_landing.py", "_write_rows"),
            ("core/pg_landing.py", "_write_order_rows"),
            ("core/pg_buyer_rows.py", "_write_buyer_rows"),
            ("core/pg_order_versions.py", "capture_versions"),
        } <= set(writers), sorted(writers)
        assert len(writers) >= 15, sorted(writers)

    def test_every_registered_chain_writes_postgres(self, walk):
        """Found here, so a chain that stopped writing Postgres — its writer
        moved somewhere the walk does not see — fails rather than passing as
        a registered module with nothing in it."""
        chains = _chain_modules()
        writing = {key[0] for key in walk.writers()}
        assert chains <= writing, chains - writing


def _targets_of(source: str, name: str) -> set:
    """What the walk says `name` writes, in a module made of `source` alone."""
    module = _Module.from_source("core/_walk_probe.py", source)
    modules = {module.name: module}
    module.load_consts(modules)
    return _pg_write_targets(module, module.funcs[name], modules, _executors(modules))


class TestTheWalkSeesWhatTheReviewFound:
    """The DN-22b review appended each of these to `core/pg_buyers.py` and the
    walk passed them: a helper that transforms the statement it is handed
    before executing it — `numbered()` is this codebase's own idiom — and a
    COPY, which carries no statement at all. Fed here as shapes, with the
    direct literal the walk always saw as the control."""

    _NUMBERED = """
        async def _exec_numbered(sql, params):
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute(numbered(sql), *params)

        async def rename_buyer(buyer_id, name):
            await _exec_numbered(
                "UPDATE bronze.buyers SET full_name = ? WHERE id = ?", [name, buyer_id])

        async def read_buyer(buyer_id):
            await _exec_numbered("SELECT full_name FROM bronze.buyers WHERE id = ?", [buyer_id])
    """

    def test_the_control_a_direct_literal(self):
        assert _targets_of("""
            async def rename_buyer(buyer_id, name):
                pool = await get_pool()
                async with pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE bronze.buyers SET full_name = $1 WHERE id = $2", name, buyer_id)
        """, "rename_buyer") == {"bronze.buyers"}

    def test_a_helper_that_transforms_the_statement_executes_it(self):
        assert _targets_of(self._NUMBERED, "rename_buyer") == {"bronze.buyers"}

    def test_a_read_through_the_same_helper_is_not_a_write(self):
        assert _targets_of(self._NUMBERED, "read_buyer") == set()

    def test_a_statement_handed_on_through_two_helpers(self):
        """`_users_run`'s shape: rendered into a local, handed to a helper that
        hands it to asyncpg. The target is a hole the helper fills, so it
        counts as unknown — which is what makes the store's writers visible
        and `_POSTGRES_STORE_HELPERS` necessary. The inner helper is not called
        `execute` here, so only the fixed point can reach it."""
        assert _targets_of("""
            async def _send(sql, params):
                pool = await get_pool()
                async with pool.acquire() as conn:
                    await conn.execute(sql, *params)

            async def _run(self, sql, params):
                rendered = numbered(sql.format(users=TABLE))
                await _send(rendered, params)

            async def approve(self, user_id):
                await self._run("UPDATE {users} SET status = 'approved' "
                                "WHERE user_id = ?", [user_id])
        """, "approve") == {"{?}"}

    def test_self_alone_does_not_make_a_helper_an_executor(self):
        """`self` carries the object, not a statement: a method reading its own
        fixed query is not handed anything."""
        module = _Module.from_source("core/_walk_probe.py", """
            async def count(self):
                async with self.pool.acquire() as conn:
                    await conn.fetchval(self.COUNT_SQL)
        """)
        assert "count" not in _executors({module.name: module})

    @pytest.mark.parametrize("call,expected", [
        ('conn.copy_records_to_table("buyers", schema_name="bronze", records=rows)',
         {"bronze.buyers"}),
        ('conn.copy_to_table("orders", source=path, schema_name="bronze")',
         {"bronze.orders"}),
        ('conn.copy_records_to_table(table_name="gender", schema_name=APP, records=rows)',
         {"app.gender"}),
        ('conn.copy_records_to_table(table, schema_name=schema, records=rows)', {"{?}"}),
        ('conn.copy_records_to_table("buyers", records=rows)', {"{?}"}),
        ('conn.copy_records_to_table("orders", schema_name="silver", records=rows)', set()),
    ])

    def test_a_copy_is_read_by_its_schema(self, call, expected):
        assert _targets_of(f"""
            APP = "app"

            async def ship(rows, table, schema, path):
                pool = await get_pool()
                async with pool.acquire() as conn:
                    await {call}
        """, "ship") == expected


@pytest.fixture
def flags(monkeypatch):
    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS",
                "KS_WRITE_EXPENSE_TYPES"):
        monkeypatch.delenv(env, raising=False)
    return monkeypatch


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

    def acquire(self):
        return _Ctx(_Conn(self))

    def wrote(self, table: str) -> bool:
        return any(table in s and "INSERT INTO" in s for s in self.sql)


@pytest.fixture
def pool(flags):
    """The recording pool behind `core.pg.get_pool`."""
    from unittest.mock import AsyncMock

    recorder = _RecordingPool()
    flags.setattr("core.pg.get_pool", AsyncMock(return_value=recorder))
    flags.setattr("core.pg.require_revision", AsyncMock())
    return recorder


def _chains_splitting_the_order_tables(chains) -> list:
    """The chains that declare one order table and not the other."""
    from core.write_chains import chain_name

    both = {ORDERS, LINES}
    return [chain_name(c) for c in chains
            if both & set(c.CHAIN_TABLES) and not both <= set(c.CHAIN_TABLES)]


class TestOwnershipOfTheOrderTablesPassesAsAUnit:
    """Why the two order tables are one chain's. Not that they ship together —
    `write_orders`' core with `replace_products=False` writes headers alone
    every day, from the 05:15 status refresh and the comment backfills (the
    comment ship until step 14's S14-09) — but that whoever takes the
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


class TestTheOrderWatchesOutliveTheStandDown:
    """The 23.09 question (DN-22a's review), settled in DN-22b (the stand-down
    itself went with the sync's mirror in step 14's S14-09 and S14-13): when a chain
    owns the order tables, the stand-down cuts the sync's mirror off from
    `write_orders` — and `write_orders` is the only thing that moves the
    `bronze.orders` watermark the canary pages on (`mirror_stale`, 8 h) and
    the only thing that writes `app.order_versions`, whose liveness check
    files `order_versions_stalled` (CRITICAL). Either the two watches learn to
    read the stand-down, or the chain's writer keeps feeding both.

    The writer keeps feeding both. The chain-3 writer ships through
    `write_orders`, because nothing else may write the order tables or the
    archive: the capture must land in the transaction that writes the row
    (`core/pg_order_versions.py`), and a second writer of `bronze.orders`
    beside it is exactly what `order_versions_missing` already reports as
    "something is writing bronze.orders without going through write_orders".
    So the watermark keeps moving and the archive keeps being written under
    the chain, and both watches keep meaning what they say. Teaching them the
    stand-down instead would blind the one liveness check on the one table
    nothing can rebuild, at the moment its writer changes hands.

    Pinned here, from the walk: the order tables and the archive each have
    one writer — a chain module included, which the registry walk would
    otherwise excuse as a destination."""

    ORDER_TABLES = {ORDERS, LINES}
    ARCHIVE = "app.order_versions"

    def _writers_of(self, walk, tables) -> set:
        return {key for key, targets in walk.writers().items()
                if targets & set(tables)}

    # Since chain 3's PR-1 the statements live in `_write_order_rows`, on the
    # caller's transaction: `write_orders` runs them for every path that
    # ships DuckDB's orders, and chain 3's writer for its own. Still one
    # writer — the chain module spells no statement against these tables.
    CORE = ("core/pg_landing.py", "_write_order_rows")

    def test_the_order_tables_have_one_writer(self, walk):
        assert self._writers_of(walk, self.ORDER_TABLES) == {self.CORE}

    def test_the_archive_is_written_only_inside_it(self, walk):
        capture = ("core/pg_order_versions.py", "capture_versions")
        assert self._writers_of(walk, {self.ARCHIVE}) == {capture}
        assert walk.callers.get(capture) == {self.CORE}

    @pytest.mark.asyncio
    async def test_it_moves_the_watched_watermark_and_writes_the_archive(self, pool):
        """What a writer going through it keeps feeding: the watermark row the
        canary reads and a version for an order it has not seen."""
        from core.landing_rows import landed_orders
        from core.mirror_reconciliation import WATCHED_MIRRORS
        from core.pg_landing import write_orders
        from core.pg_order_versions import TABLE

        orders, products = landed_orders([_order(1)])
        await write_orders(orders, products, replace_products=True)
        assert ORDERS in WATCHED_MIRRORS
        assert any("INSERT INTO meta.mirror_state" in s for s in pool.sql)
        assert pool.wrote(TABLE)


class TestEveryChainNamesItselfOnce:
    """`CHAIN` is the string the latch, the owner rows, health and the canary
    key on; `chain_name` is what the registry derives from the module. A chain
    whose two disagree is held down by its owner rows while the health block
    and the canary look for it under the other name and see nothing — found
    reviewing chain 4's plan, before a third chain could be the first to do it.
    """

    @pytest.mark.parametrize("chain", __import__(
        "core.write_chains", fromlist=["WRITE_CHAINS"]).WRITE_CHAINS,
        ids=lambda c: c.__name__)

    def test_chain_is_the_module_basename(self, chain):
        from core.write_chains import chain_name
        assert chain.CHAIN == chain_name(chain)


# ─── The analysts' captures (revision 0036) ──────────────────────────────────


@pytest.fixture(scope="module")
def capture_walk():
    return _Walk(_CAPTURE_WALKED)


def _path(module: str) -> str:
    return module.replace(".", "/") + ".py"


def _modules_writing(walk, table: str) -> set:
    return {key[0] for key, targets in walk.writers().items() if table in targets}


def _capture_tables():
    from core.write_chains import CAPTURE_TABLES

    return sorted(CAPTURE_TABLES)


def _key_tables():
    from core.write_chains import KEY_TABLES

    return sorted(KEY_TABLES)


class TestTheCaptureTables:
    """Each capture table has one writer, the module `CAPTURE_TABLES` names
    (the design's §3.3, "Captures stay out of CHAIN_TABLES"); the key tables
    have none in this build. Read off the walk, so a second module writing a
    capture fails here whichever tree it is in.

    `at most its named writer` and `exactly` are two tests on purpose: the
    first fails on a second writer, the second on a writer that stopped."""

    def test_they_are_no_chains_tables(self):
        """Kills: a capture put in a chain's `CHAIN_TABLES` — it would have to
        succeed a DuckDB table in the frozen FATES, and `chain_latch.claim`
        would stamp owner rows for it on every write."""
        from core.write_chains import CAPTURE_TABLES, KEY_TABLES, chain_tables

        assert not (set(CAPTURE_TABLES) | set(KEY_TABLES)) & chain_tables()
        assert not set(CAPTURE_TABLES) & set(KEY_TABLES)

    def test_they_are_revision_0036s_tables(self):
        """Kills: a table created with no writer named, or a writer named for
        a table no revision creates."""
        from core.write_chains import CAPTURE_TABLES, KEY_TABLES
        from tests import migration_replay as replay

        rev = replay.load(replay.VERSIONS / "0036_analytics_captures.py")
        assert set(CAPTURE_TABLES) | set(KEY_TABLES) == set(rev.TABLES)

    @pytest.mark.parametrize("table", _capture_tables())
    def test_a_capture_has_no_writer_but_its_own(self, capture_walk, table):
        from core.write_chains import CAPTURE_TABLES

        assert _modules_writing(capture_walk, table) <= {_path(CAPTURE_TABLES[table])}

    @pytest.mark.parametrize("table", _capture_tables())
    def test_a_capture_has_exactly_its_one_writer(self, capture_walk, table):
        """Kills: a capture whose writer stopped writing it — the call
        removed, or moved into a module the spec does not name."""
        from core.write_chains import CAPTURE_TABLES

        assert _modules_writing(capture_walk, table) == {_path(CAPTURE_TABLES[table])}

    @pytest.mark.parametrize("table", _key_tables())
    def test_the_key_tables_have_no_writer_yet(self, capture_walk, table):
        """D3: the person key's tables, and the buyer key's, are only created
        in phase 1; their writer is the warehouse job, in phase 2. Kills: a
        writer of them landing early, before D3's guards come with it."""
        assert not _modules_writing(capture_walk, table)

    def test_the_helpers_write_no_capture_themselves(self, capture_walk):
        """The capture helpers execute the statement the writer hands them,
        so the walk attributes the write to the writer. Kills: a helper that
        spelled a capture's statement itself — every capture would then read
        as written by `core/analytics_captures.py`."""
        assert "capture_in_transaction" in capture_walk.executors
        assert not [key for key in capture_walk.writers()
                    if key[0] == "core/analytics_captures.py"]

    def test_a_writer_through_the_helper_is_the_writer(self, capture_walk):
        """The shape every in-transaction capture takes: its own constant,
        handed to `capture_in_transaction`. The control for the two tests
        above — the walk sees the table through the helper."""
        module = _Module.from_source("core/_capture_probe.py", """
            from core.analytics_captures import capture_in_transaction

            _COST_SQL = "INSERT INTO app.offer_cost_daily (date, offer_id) SELECT $1, id FROM bronze.offers"

            async def snapshot(conn, today):
                await capture_in_transaction(conn, "app.offer_cost_daily", _COST_SQL, today)
        """)
        modules = dict(capture_walk.modules)
        modules[module.name] = module
        module.load_consts(modules)
        assert _pg_write_targets(module, module.funcs["snapshot"], modules,
                                 capture_walk.executors) == {"app.offer_cost_daily"}
