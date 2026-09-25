"""Once a chain has written Postgres, the flag cannot bring its writes back.

DN-06 of the stage-4 prep plan, owner decision OD-19 (a) of 2026-09-17. Every
consumer routed on `KS_WRITE_*` at the moment of the call, so setting a flag
back after rows had landed in Postgres did not undo anything — it started a
second writer beside the first, which is the state revision 0030 forbids for
`stock_movements` and which would have rolled typed expenses back hourly.

What is pinned here is the local half: the marker file that answers
synchronously, with no event loop and no database, and so still answers on a
boot where Postgres is down. The two copies against each other, and the
end-to-end behaviour of the writers, need a real Postgres and live in
`tests/integration/test_chain_latch.py`.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import json
import pathlib
import textwrap
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from bot import canary as canary_module
from core import (
    chain_latch, pg_expense_types_write, pg_expenses_write, pg_goals_write,
    pg_inventory_write, write_chains,
)

CORE = pathlib.Path(__file__).resolve().parents[2] / "core"


@pytest.fixture
def flags(monkeypatch):
    """No chain's variable set, and nothing latched — the conftest fixture has
    already pointed the marker directory at this test's own tmp_path."""
    for chain in write_chains.WRITE_CHAINS:
        monkeypatch.delenv(chain.WRITE_ENV, raising=False)
    return monkeypatch


class TestTheMarker:
    def test_it_records_when_and_which_chain(self, flags):
        before = datetime.now(timezone.utc)
        stamp = chain_latch.latch("pg_expenses_write", "KS_WRITE_EXPENSES")

        written = json.loads(chain_latch.marker_path("pg_expenses_write").read_text())
        assert written["chain"] == "pg_expenses_write"
        assert written["env"] == "KS_WRITE_EXPENSES"
        assert written["latched_at"] == stamp
        assert datetime.fromisoformat(stamp) >= before

    def test_a_second_call_keeps_the_first_moment(self, flags):
        """The stamp is when ownership passed, not when the latest row was
        written: the comparison reads it against the shipper's `last_ok_at` to
        tell a shipment that predates the handover from one that overwrote it."""
        first = chain_latch.latch("pg_expenses_write")
        path = chain_latch.marker_path("pg_expenses_write")
        mtime = path.stat().st_mtime_ns

        assert chain_latch.latch("pg_expenses_write") == first
        assert path.stat().st_mtime_ns == mtime, "the marker was rewritten"

    def test_a_marker_this_process_did_not_write_is_loaded(self, flags):
        """The case that matters: the container that took the latch is gone and
        this one has to honour it."""
        assert not chain_latch.latched("pg_inventory_write")
        chain_latch.MARKER_DIR.mkdir(parents=True, exist_ok=True)
        (chain_latch.MARKER_DIR / "pg_inventory_write").write_text(
            json.dumps({"chain": "pg_inventory_write", "latched_at": "2026-09-17T08:33:00+00:00"}))

        assert chain_latch.load() == {"pg_inventory_write": "2026-09-17T08:33:00+00:00"}
        assert chain_latch.latched("pg_inventory_write")
        assert chain_latch.latched_at("pg_inventory_write") == "2026-09-17T08:33:00+00:00"

    def test_an_unparseable_marker_still_latches(self, flags):
        """The file's existence is the latch; its contents are the audit trail.
        Reading a corrupt marker as "not latched" is the one failure that
        restarts the second writer."""
        chain_latch.MARKER_DIR.mkdir(parents=True, exist_ok=True)
        (chain_latch.MARKER_DIR / "pg_expenses_write").write_text("{truncated")

        chain_latch.load()

        assert chain_latch.latched("pg_expenses_write")
        assert chain_latch.latched_at("pg_expenses_write")   # the file's own mtime

    def test_configure_modes_loads_it_before_anything_writes(self, flags):
        """`core.runtime_modes.configure_modes()` is the one moment every entry
        point already calls — web's startup before the boot sync, the scheduler,
        and the scripts that reach a writer."""
        from core.runtime_modes import configure_modes

        chain_latch.MARKER_DIR.mkdir(parents=True, exist_ok=True)
        (chain_latch.MARKER_DIR / "pg_expenses_write").write_text(
            json.dumps({"latched_at": "2026-09-17T09:00:00+00:00"}))
        chain_latch._latched = None                 # as a fresh process starts

        configure_modes()

        assert chain_latch.latched("pg_expenses_write")

    def test_release_is_real_and_says_whether_it_removed_anything(self, flags):
        chain_latch.latch("pg_expenses_write")
        assert chain_latch.release("pg_expenses_write") is True
        assert not chain_latch.latched("pg_expenses_write")
        assert chain_latch.release("pg_expenses_write") is False

    def test_the_removal_is_as_durable_as_the_creation(self, flags):
        """`_write_marker` fsyncs the directory so a rename that never reached
        the disk cannot unlatch a chain on the next power cut. The unlink owes
        the same, one step later: DN-08's copy-back releases the latch *after*
        it has copied the rows into DuckDB and put `KS_WRITE_*` back, so a
        directory entry that survives a power cut brings the marker back to a
        chain whose rows are now in both stores — two writers allocating
        `stock_movements` ids, which revision 0030 forbids.
        """
        import os as os_module

        chain_latch.latch("pg_expenses_write")
        synced: list = []
        real_fsync = os_module.fsync

        def recording(fd):
            synced.append(os_module.fstat(fd).st_ino)
            return real_fsync(fd)

        directory = chain_latch.MARKER_DIR.stat().st_ino
        with patch.object(os_module, "fsync", new=recording):
            assert chain_latch.release("pg_expenses_write") is True
        assert directory in synced, "the unlink was never pushed to the disk"

        # A release that removed nothing has nothing to make durable.
        synced.clear()
        with patch.object(os_module, "fsync", new=recording):
            assert chain_latch.release("pg_expenses_write") is False
        assert synced == []

    def test_a_marker_that_cannot_be_written_stops_the_write(self, flags):
        """A row in Postgres that no marker records is how the next boot comes
        to believe DuckDB is still the writer, so the write must not proceed.
        Here the directory's place is taken by a file, which is what a wrong
        mount looks like from inside the container."""
        chain_latch.MARKER_DIR.parent.mkdir(parents=True, exist_ok=True)
        chain_latch.MARKER_DIR.write_text("not a directory")

        with pytest.raises(OSError):
            chain_latch.latch("pg_expenses_write")
        assert not chain_latch.latched("pg_expenses_write")


class TestTheOneAnswerEveryConsumerReads:
    @pytest.mark.parametrize("chain", write_chains.WRITE_CHAINS, ids=write_chains.chain_name)
    def test_latched_outranks_the_flag(self, flags, chain):
        assert chain.writes_postgres() is False
        chain_latch.latch(chain.CHAIN)
        assert chain.writes_postgres() is True, "the flag moved the writes back"
        flags.setenv(chain.WRITE_ENV, "duckdb")
        assert chain.writes_postgres() is True
        assert chain.env_writes_postgres() is False, "the variable is still readable"

    @pytest.mark.parametrize("chain", write_chains.WRITE_CHAINS, ids=write_chains.chain_name)
    def test_latched_outranks_a_value_nobody_can_read(self, flags, chain):
        """A typo must not route a latched chain back to DuckDB either — it is
        the same second writer, arrived at by accident instead of by decision."""
        chain_latch.latch(chain.CHAIN)
        flags.setenv(chain.WRITE_ENV, "postgrse")

        assert chain.writes_postgres() is True
        with pytest.raises(RuntimeError):
            chain.env_writes_postgres()

    def test_the_tables_and_the_sync_keys_stand_down_on_the_latch_alone(self, flags):
        chain_latch.latch("pg_inventory_write")

        assert write_chains.stood_down_tables() >= frozenset(pg_inventory_write.CHAIN_TABLES)
        assert write_chains.stood_down_sync_keys() == frozenset(
            {"last_sync_offers", "last_sync_stocks"})

    def test_health_publishes_both_halves_of_the_disagreement(self, flags):
        from web.routes.api.health import _write_chains

        chain_latch.latch("pg_expenses_write")
        flags.setenv("KS_WRITE_EXPENSES", "postgrse")

        block = _write_chains()["pg_expenses_write"]
        assert block["mode"] == "postgres" and block["latched"] is True
        assert block["mismatch"] is True and block["latched_at"]
        assert "postgrse" in block["error"], "the typo is hidden behind the latch"

    def test_the_published_schema_describes_the_fields_it_publishes(self, flags):
        """`/api/health` is public and `web/schemas.py` is where an operator
        reads what the block means, so the field that says "a rollback somebody
        believes happened has not" cannot be the one with no description."""
        from web.schemas import HealthResponse

        described = HealthResponse.model_fields["write_chains"].description
        published = set().union(*(set(state) for state in
                                  write_chains.chain_modes().values()))
        assert published >= {"latched", "latched_at", "mismatch"}
        for field in sorted(published - {"env", "mode", "error"}):
            assert field in described, f"{field} is published and undescribed"
        assert "chain_copy_back" in described, "the way back is not named"

    def test_the_health_block_answers_without_postgres(self, flags):
        """The other disagreement — marker against owner row — is deliberately
        NOT in `mismatch`: reading the Postgres copy here would make the one
        block that can say "your writes are going to a store you cannot reach"
        fail with that store. The daily comparison owns that comparison."""
        source = textwrap.dedent(inspect.getsource(write_chains.chain_modes))
        tree = ast.parse(source)
        called = {getattr(n.func, "attr", getattr(n.func, "id", ""))
                  for n in ast.walk(tree) if isinstance(n, ast.Call)}
        assert not called & {"read_owners", "get_pool", "acquire", "fetch", "run"}
        assert not [n for n in ast.walk(tree) if isinstance(n, ast.Await)]
        # The import as well as the call: reaching for `core.pg` here is the
        # tell, and a guard that watched only calls let its own mutation past.
        pulled = {f"{n.module}.{a.name}" for n in ast.walk(tree)
                  if isinstance(n, ast.ImportFrom) and n.module for a in n.names}
        assert not [m for m in pulled if m.startswith("core.pg.")], sorted(pulled)

    def test_no_mismatch_while_the_flag_agrees(self, flags):
        chain_latch.latch("pg_expenses_write")
        flags.setenv("KS_WRITE_EXPENSES", "postgres")

        assert write_chains.mismatched_chains() == {}

    def test_the_mismatch_names_the_chain_and_when_it_was_taken(self, flags):
        stamp = chain_latch.latch("pg_expenses_write")
        flags.setenv("KS_WRITE_EXPENSES", "duckdb")

        assert write_chains.mismatched_chains() == {"pg_expenses_write": stamp}


class TestPostgresDownAtBoot:
    """The cost of OD-19 (a), stated as a test rather than left to be found.

    A latched chain whose Postgres is unreachable fails its writes. It must not
    fall back to DuckDB: the expense would land in a store the page does not
    read, and the flag would be believed again on the next boot.
    """

    @pytest_asyncio.fixture
    async def store(self, tmp_path):
        from core.duckdb_store import DuckDBStore

        s = DuckDBStore(db_path=tmp_path / "latched.duckdb")
        await s.connect()
        yield s
        await s.close()

    @pytest.mark.asyncio
    async def test_an_expense_raises_instead_of_landing_in_duckdb(self, flags, store):
        from datetime import date

        from core.runtime_modes import configure_modes

        chain_latch.latch("pg_expenses_write")
        # A restart, modelled honestly: the cache this process built is dropped
        # and the latch has to come back off the disk through the one call every
        # entry point makes. Latching in-process and leaving the cache warm
        # would pass even with the marker read deleted.
        chain_latch._latched = None
        configure_modes()

        flags.setenv("KS_WRITE_EXPENSES", "duckdb")
        unreachable = AsyncMock(side_effect=OSError("connection refused"))

        with patch("core.pg.get_pool", new=unreachable):
            with pytest.raises(OSError):
                await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 100)

        async with store.connection() as conn:
            assert conn.execute("SELECT COUNT(*) FROM manual_expenses").fetchone()[0] == 0


class TestAWriteThatNeverReachesPostgres:
    """It must not latch: the chain still holds the rollback it came with.

    Production on the day DN-06 was written: `KS_WRITE_EXPENSES=postgres`
    since 2026-09-17 08:33 UTC with `app.manual_expenses` at zero rows, so the
    flag could still be flipped back freely. A Postgres restart, a wait on a
    full pool that ended without a connection, or a `SchemaVersionError` from
    `web` coming up ahead of `migrate`,
    plus one submitted expense form in that window, used to spend that — on a
    chain that had written nothing, for ever, with no copy-back built yet.
    """

    @pytest_asyncio.fixture
    async def store(self, tmp_path):
        from core.duckdb_store import DuckDBStore

        s = DuckDBStore(db_path=tmp_path / "unreached.duckdb")
        await s.connect()
        yield s
        await s.close()

    @pytest.mark.parametrize("failing", ["get_pool", "require_revision"])
    @pytest.mark.asyncio
    async def test_an_unreachable_postgres_leaves_the_chain_unlatched(
        self, flags, store, failing,
    ):
        from datetime import date

        flags.setenv("KS_WRITE_EXPENSES", "postgres")
        broken = AsyncMock(side_effect=OSError("connection refused")
                           if failing == "get_pool" else
                           RuntimeError("schema revision 0032, expected 0033"))

        with patch(f"core.pg.{failing}", new=broken):
            with pytest.raises((OSError, RuntimeError)):
                await store.add_expense(date(2026, 9, 17), "marketing", "Facebook Ads", 10)

        assert not chain_latch.latched("pg_expenses_write"), (
            "a write that never reached Postgres latched the chain")
        assert not chain_latch.marker_path("pg_expenses_write").exists()

        # And so the flag is still the whole answer: the rollback survives.
        flags.setenv("KS_WRITE_EXPENSES", "duckdb")
        assert pg_expenses_write.writes_postgres() is False
        async with store.connection() as conn:
            assert conn.execute("SELECT COUNT(*) FROM manual_expenses").fetchone()[0] == 0

    @pytest.mark.asyncio
    async def test_the_dictionary_chain_when_the_pool_has_no_connection_to_give(
        self, flags, store,
    ):
        """The pool is in hand and the revision passed; `acquire` is what fails.

        Chain 6a writes inside the Sunday full sync, whose other steps hold the
        pool's five connections, so its acquire waits there — `get_pool` sets
        no acquire timeout — and a Postgres restart during that wait ends it in
        a reconnect that is refused. The latch used to be taken between
        `_pool()` and `acquire()`, on disk through the whole wait, and this
        latched the chain with nothing written (review of DN-26)."""

        class _Refused:
            """asyncpg's shape: `acquire()` returns a context whose entry is
            what waits for a connection, and where the reconnect it makes for
            a dead one is refused."""

            def acquire(self):
                class _Ctx:
                    async def __aenter__(self_inner):
                        raise ConnectionRefusedError("the reconnect after a restart")

                    async def __aexit__(self_inner, *exc):
                        return False
                return _Ctx()

        flags.setenv("KS_WRITE_EXPENSE_TYPES", "postgres")
        flags.setenv("KS_READ_EXPENSES", "postgres")      # its precondition
        with patch("core.pg.get_pool", new=AsyncMock(return_value=_Refused())), \
                patch("core.pg.require_revision", new=AsyncMock()):
            with pytest.raises(ConnectionRefusedError):
                await store.upsert_expense_types([{"id": 1, "name": "Delivery"}])

        assert not chain_latch.latched("pg_expense_types_write"), (
            "a write that never got a connection latched the chain")
        assert not chain_latch.marker_path("pg_expense_types_write").exists()
        flags.setenv("KS_WRITE_EXPENSE_TYPES", "duckdb")
        assert pg_expense_types_write.writes_postgres() is False

    @pytest.mark.asyncio
    async def test_the_inventory_chain_too(self, flags, store):
        """Chain 1 has not been flipped yet, and the sync tick runs every
        minute — so a deploy race would make its first flip irreversible with
        not one movement written."""
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        unreachable = AsyncMock(side_effect=OSError("connection refused"))

        with patch("core.pg.get_pool", new=unreachable):
            with pytest.raises(OSError):
                await store.upsert_offers([{"id": 1, "product_id": 101, "sku": "S-1"}])

        assert not chain_latch.latched("pg_inventory_write")
        flags.setenv("KS_WRITE_INVENTORY", "duckdb")
        assert pg_inventory_write.writes_postgres() is False


# ─── The guard: no writer may reach Postgres before taking the latch ─────────

# Getting a connection. A public function of a chain module that reaches one
# of these is a writer unless it is named in `_READERS` below — so a writer is
# found by what it holds, not by whether the walk can read its statement.
_CONNECTION = {"get_pool", "_pool", "acquire"}

# The public functions of a chain module that take a connection only to read.
# Named rather than inferred: "reaches a connection" is how a writer is found,
# and these are the exceptions to it. `TestTheWalk` holds each one to writing
# nothing, so a write added to one fails there instead of hiding here.
_READERS = {
    "pg_inventory_write": {"read_snapshot_calendar", "preflight"},
}

# A statement that writes, by how it starts. Upper-cased first; `setval` is a
# write to a sequence however the statement starts.
_WRITE_VERBS = ("INSERT", "UPDATE", "DELETE", "CREATE TEMP", "TRUNCATE")

# What a reader may not reach, whatever its strings say.
_WRITE_CALLS = {"transaction", "execute", "executemany", "_insert", "_upsert",
                "_latch", "claim"}


def _index(source: str):
    """The module's functions by name, and the strings each module-level
    constant holds — what `_reached` follows out of a function body."""
    tree = ast.parse(source)
    funcs = {n.name: n for n in tree.body
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    consts = {}
    for n in tree.body:
        targets = (n.targets if isinstance(n, ast.Assign)
                   else [n.target] if isinstance(n, ast.AnnAssign) and n.value else [])
        for target in targets:
            if isinstance(target, ast.Name):
                consts[target.id] = [c.value for c in ast.walk(n.value)
                                     if isinstance(c, ast.Constant)
                                     and isinstance(c.value, str)]
    return tree, funcs, consts


def _reached(node, funcs, consts):
    """`(call names, strings)` a function reaches: its own body, every private
    function of the module it names — called or passed — transitively, and the
    strings of every module-level constant any of them names.

    The function body alone was the walk until 2026-09-25, and a public writer
    whose statement sat in a private helper or a constant was invisible to all
    four guards and to the integration list built from them (review of the
    latch-inside-acquire change, reproduced with `touch_expense`)."""
    seen, stack = set(), [node]
    calls, strings = set(), []
    while stack:
        fn = stack.pop()
        if fn.name in seen:
            continue
        seen.add(fn.name)
        for n in ast.walk(fn):
            if isinstance(n, ast.Call):
                calls.add(getattr(n.func, "attr", getattr(n.func, "id", "")))
            elif isinstance(n, ast.Constant) and isinstance(n.value, str):
                strings.append(n.value)
            elif isinstance(n, ast.Name):
                if n.id.startswith("_") and n.id in funcs and n.id != fn.name:
                    stack.append(funcs[n.id])
                strings.extend(consts.get(n.id, ()))
    return calls, strings


def _writes(strings) -> bool:
    return any(s.strip().upper().startswith(_WRITE_VERBS) or "SETVAL(" in s.upper()
               for s in strings)


def _writers_in(source: str, readers=frozenset()) -> dict:
    """`_writers` over a module's source — separate so the walk's own tests
    can hand it the shapes it used to miss."""
    tree, funcs, consts = _index(source)
    out = {}
    for node in tree.body:
        if (not isinstance(node, ast.AsyncFunctionDef) or node.name.startswith("_")
                or node.name in readers):
            continue
        calls, strings = _reached(node, funcs, consts)
        if calls & (_CONNECTION | {"_insert", "_upsert"}) or _writes(strings):
            out[node.name] = node
    return out


def _source(module) -> str:
    return pathlib.Path(inspect.getfile(module)).read_text(encoding="utf-8")


def _writers(module) -> dict:
    """`{name: node}` for every public async function in a chain module that
    reaches a connection or a statement that writes — through the module's
    private helpers and constants too — except the readers `_READERS` names.

    Parsed rather than listed, because a sixth inventory writer added without
    the latch is exactly the case this has to fail on. Found by the connection
    first and the statement second: a writer is what holds a connection, and a
    walk that had to *read* the statement missed every writer whose statement
    it could not see."""
    readers = _READERS.get(write_chains.chain_name(module), frozenset())
    return _writers_in(_source(module), readers)


# A statement, not a connection. `get_pool` and `require_revision` are
# deliberately absent: the latch must come AFTER them (see the class below),
# and a set that held them would pin the very order this fixes.
_DB_CALLS = {"execute", "executemany", "fetch", "fetchrow", "fetchval"}

# Getting the connection — everything that can fail before a statement is ever
# sent, and so everything that must not be able to latch a chain for ever.
_CONNECT_CALLS = {"get_pool", "require_revision", "_pool"}


def _inside(node, context: str) -> set:
    """`id()` of every node in the body of an `async with <x>.<context>(...)`
    inside `node` — structure, not line numbers. asyncpg waits on *entering*
    `pool.acquire()`, not on calling it, so `ctx = pool.acquire()`, then the
    latch, then `async with ctx` latches before the connection is handed over
    while every line comparison says it came after."""
    inside = set()
    for w in ast.walk(node):
        if isinstance(w, ast.AsyncWith) and any(
                isinstance(item.context_expr, ast.Call)
                and getattr(item.context_expr.func, "attr", "") == context
                for item in w.items):
            for stmt in w.body:
                inside.update(id(n) for n in ast.walk(stmt))
    return inside


def _calls(node, name: str) -> list:
    return [n for n in ast.walk(node) if isinstance(n, ast.Call)
            and getattr(n.func, "attr", getattr(n.func, "id", "")) == name]


class TestTheWalk:
    """What `_writers` counts is what the four guards below and the live-pool
    list in `tests/integration/test_latch_waits_for_a_connection.py` hold to
    the latch — so a writer it cannot see is a writer nothing checks."""

    # The review's reproduction, verbatim: a public writer in chain 8 whose
    # statement is in a private helper, with no latch and no claim.
    HELPER = textwrap.dedent('''
        async def _write_note(conn, expense_id):
            await conn.execute("UPDATE app.manual_expenses SET note = 'x' WHERE id = $1", expense_id)


        async def touch_expense(expense_id):
            pool = await _pool()
            async with pool.acquire() as conn:
                await _write_note(conn, expense_id)
    ''')

    def test_a_writer_whose_statement_is_in_a_helper_is_found(self):
        source = _source(pg_expenses_write) + self.HELPER
        found = _writers_in(source)
        assert "touch_expense" in found
        node = found["touch_expense"]
        assert not _calls(node, "_latch") and not _calls(node, "claim"), (
            "the shape must be one the latch guards then fail on")

    def test_so_is_one_whose_statement_is_a_module_constant(self):
        source = textwrap.dedent('''
            _SQL = "DELETE FROM app.manual_expenses WHERE id = $1"

            async def drop(conn, expense_id):
                await conn.execute(_SQL, expense_id)
        ''')
        assert set(_writers_in(source)) == {"drop"}

    def test_so_is_one_that_reaches_a_connection_through_a_helper(self):
        source = textwrap.dedent('''
            async def _conn():
                from core.pg import get_pool
                return await get_pool()

            async def anything(x):
                pool = await _conn()
                await pool.execute(build(x))
        ''')
        assert set(_writers_in(source)) == {"anything"}

    @pytest.mark.parametrize("module", write_chains.WRITE_CHAINS, ids=write_chains.chain_name)
    def test_the_readers_it_leaves_out_write_nothing(self, module):
        """A named reader is the one way out of the walk, so each is held to
        reading: no statement that writes and no call that could, through its
        helpers and constants as well."""
        tree, funcs, consts = _index(_source(module))
        for name in _READERS.get(write_chains.chain_name(module), ()):
            node = funcs.get(name)
            assert isinstance(node, ast.AsyncFunctionDef) and not name.startswith("_"), (
                f"{module.__name__}.{name} is named a reader and is not a public "
                "async function of the module")
            calls, strings = _reached(node, funcs, consts)
            assert calls & _CONNECTION, (
                f"{module.__name__}.{name} reaches no connection; it need not be named")
            assert not _writes(strings), f"{module.__name__}.{name} carries a write"
            assert not calls & _WRITE_CALLS, (
                f"{module.__name__}.{name} reaches {sorted(calls & _WRITE_CALLS)}")

    def test_every_reader_named_is_in_a_registered_chain(self):
        chains = {write_chains.chain_name(c) for c in write_chains.WRITE_CHAINS}
        assert set(_READERS) <= chains


class TestEveryWriterLatchesFirst:
    def test_the_walk_finds_the_writers_that_exist(self):
        assert set(_writers(pg_expenses_write)) == {
            "add_expense", "update_expense", "delete_expense"}
        assert set(_writers(pg_inventory_write)) == {
            "upsert_offers", "upsert_stocks", "rebuild_sku_inventory_status",
            "record_sku_inventory_snapshot", "record_inventory_snapshot"}
        # Chain 7a (DN-25): one writer, and its statement is spelled inside it
        # precisely so that this walk sees it — a module-level constant would
        # have left the three guards below passing over an empty set.
        assert set(_writers(pg_goals_write)) == {"set_goal"}
        assert set(_writers(pg_expense_types_write)) == {"upsert_expense_types"}

    def test_every_registered_chain_has_a_writer_the_walk_can_see(self):
        """The guards below are parametrised over `WRITE_CHAINS`; a chain whose
        writers the walk cannot find would pass all three vacuously."""
        for chain in write_chains.WRITE_CHAINS:
            assert _writers(chain), f"{chain.__name__}: the walk found no writer"

    @pytest.mark.parametrize("module", write_chains.WRITE_CHAINS, ids=write_chains.chain_name)
    def test_each_one_calls_the_latch_before_it_touches_postgres(self, module):
        for name, node in _writers(module).items():
            latches = [n.lineno for n in ast.walk(node)
                       if isinstance(n, ast.Call)
                       and getattr(n.func, "id", "") == "_latch"]
            touches = [n.lineno for n in ast.walk(node)
                       if isinstance(n, ast.Call)
                       and getattr(n.func, "attr", getattr(n.func, "id", "")) in _DB_CALLS]
            assert latches, f"{module.__name__}.{name} writes Postgres without latching"
            assert not touches or min(latches) < min(touches), (
                f"{module.__name__}.{name} reaches Postgres before taking the latch")

    @pytest.mark.parametrize("module", write_chains.WRITE_CHAINS, ids=write_chains.chain_name)
    def test_and_none_of_them_latches_before_the_connection(self, module):
        """The window between the two calls is the whole point.

        The latch is permanent — only `scripts/chain_copy_back.py` releases it
        — so taking it for a write that never reaches Postgres spends a
        rollback that is still available. Postgres restarting, the pool being
        exhausted and `require_revision` raising because `web` came up ahead of
        `migrate` are all ordinary events; reproduced against a live database,
        each of them latched the chain with zero rows written.
        """
        for name, node in _writers(module).items():
            latches = [n.lineno for n in ast.walk(node)
                       if isinstance(n, ast.Call)
                       and getattr(n.func, "id", "") == "_latch"]
            connects = [n.lineno for n in ast.walk(node)
                        if isinstance(n, ast.Call)
                        and getattr(n.func, "attr", getattr(n.func, "id", ""))
                        in _CONNECT_CALLS]
            assert connects, f"{module.__name__}.{name} never acquires a pool"
            assert min(latches) > max(connects), (
                f"{module.__name__}.{name} latches before the connection is in "
                "hand, so a write that never reaches Postgres latches for ever")

    @pytest.mark.parametrize("module", write_chains.WRITE_CHAINS, ids=write_chains.chain_name)
    def test_nor_before_the_pool_has_handed_over_a_connection(self, module):
        """Every `_latch()` sits in the body of `async with pool.acquire()`.

        `pool.acquire()` can fail after `_pool()` has succeeded: the connection
        it hands over died — a Postgres restart, a reset that failed on
        release — and the reconnect it makes is refused, or the pool has been
        closed. Waiting is not failing: `core.pg.get_pool` sets no acquire
        timeout, so a full pool makes the writer wait, and a latch taken before
        the acquire sat on disk through the whole wait (`pg_expenses_write._latch`).

        Judged by nesting, not by line: asyncpg waits on entering the context,
        so a latch between `ctx = pool.acquire()` and `async with ctx` comes
        after the call and before the connection. Chains 1, 8 and 7a latched
        before the acquire until 2026-09-25; no chain is exempt now. What it
        costs is proved against a live pool in
        `tests/integration/test_latch_waits_for_a_connection.py`.
        """
        for name, node in _writers(module).items():
            inside = _inside(node, "acquire")
            assert inside, f"{module.__name__}.{name} never enters a connection"
            early = [n.lineno for n in _calls(node, "_latch") if id(n) not in inside]
            assert not early, (
                f"{module.__name__}.{name} latches on line(s) {early}, outside "
                "`async with pool.acquire()`: before the pool has handed over a "
                "connection, so an acquire that fails latches for ever")

    @pytest.mark.parametrize("module", write_chains.WRITE_CHAINS, ids=write_chains.chain_name)
    def test_each_one_claims_the_owner_rows_too(self, module):
        """The audit copy, inside the writing transaction: a write that rolls
        back must not claim what it did not write. A claim outside
        `async with conn.transaction()` autocommits, so it is judged by
        nesting; that the claim and the row share the transaction is proved
        per writer in `tests/integration/test_latch_waits_for_a_connection.py`."""
        for name, node in _writers(module).items():
            claims = _calls(node, "claim")
            assert claims, f"{module.__name__}.{name} takes no owner row"
            inside = _inside(node, "transaction")
            outside = [n.lineno for n in claims if id(n) not in inside]
            assert not outside, (
                f"{module.__name__}.{name} claims on line(s) {outside} outside "
                "its transaction: a write that rolls back would still claim")


class TestTheCanaryJudgesThePublishedBlock:
    def test_it_warns_on_a_mismatch_and_names_when(self):
        from bot import canary

        payload = {"write_chains": {"pg_expenses_write": {
            "env": "KS_WRITE_EXPENSES", "mode": "postgres", "error": None,
            "latched": True, "latched_at": "2026-09-17T08:33:00+00:00",
            "mismatch": True}}}
        (key, message), = canary.check_write_chain_latch(payload)
        assert key == "write_chain_flag_mismatch"
        assert "pg_expenses_write" in message and "2026-09-17T08:33:00+00:00" in message

    def test_a_latched_chain_whose_flag_agrees_is_quiet(self):
        from bot import canary

        payload = {"write_chains": {"pg_expenses_write": {
            "mode": "postgres", "latched": True, "mismatch": False}}}
        assert canary.check_write_chain_latch(payload) == []
        assert canary.check_write_chain_latch({}) == []          # an older web

    def test_it_never_reads_the_markers_itself(self):
        """Both containers mount the same `./data`, so `bot/canary.py` *can*
        open the marker directory — and must not. The question is where web's
        writes are going, web is the process that decides it, and a second
        opinion read out of a directory this container never writes would age
        without anything tracking it. (The docstring said the bot could not see
        the files at all; `docker-compose.yml` mounts `./data:/app/data` into
        both services, so that reason was false and the true one is this.)"""
        source = pathlib.Path(canary_module.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        # `from core import chain_latch` names the module in the alias, not in
        # `n.module`, so both halves are joined — the first draft of this guard
        # looked only at `n.module` and its own mutation walked straight
        # through it.
        imported = {f"{n.module}.{a.name}" for n in ast.walk(tree)
                    if isinstance(n, ast.ImportFrom) and n.module for a in n.names}
        imported |= {a.name for n in ast.walk(tree)
                     if isinstance(n, ast.Import) for a in n.names}
        assert not [m for m in imported
                    if m.startswith(("core.chain_latch", "core.pg"))], sorted(imported)
        assert "write-chain-owners" not in source

    @pytest.mark.asyncio
    async def test_run_canary_warns_rather_than_pages(self):
        """Nothing is failing: the writes go where the rows are. What is wrong
        is that somebody edited a variable expecting a rollback."""
        import httpx
        from datetime import timedelta

        from bot import canary
        from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport

        payload = _healthy_payload()
        payload["write_chains"] = {"pg_expenses_write": {
            "env": "KS_WRITE_EXPENSES", "mode": "postgres", "error": None,
            "latched": True, "latched_at": "2026-09-17T08:33:00+00:00",
            "mismatch": True}}

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(lambda request: httpx.Response(200, json=payload)) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)

        assert result.severity == "warn"
        assert "write_chain_flag_mismatch" in result.failure_keys
        assert "chain_copy_back" in canary.format_alert(result, DASHBOARD)


class TestTheLatchIsWhatTheStoreRoutesOn:
    @pytest.mark.asyncio
    async def test_a_latched_chains_watermark_leaves_duckdb(self, flags, tmp_path):
        """`get_last_sync_time` asks the chain that owns the key, and that
        answer is now the latch. The store's DuckDB path must not be taken."""
        from core.duckdb_store import DuckDBStore

        store = DuckDBStore(db_path="/nonexistent/never-opened.duckdb")
        chain_latch.latch("pg_inventory_write")
        flags.setenv("KS_WRITE_INVENTORY", "duckdb")

        with patch("core.pg_chain_watermarks.get_value",
                   new=AsyncMock(return_value=None)) as read:
            assert await store.get_last_sync_time("stocks") is None
        read.assert_awaited_once_with("last_sync_stocks")


def test_the_module_never_reaches_postgres_to_answer_where_writes_go():
    """`writes_postgres()` is asked on every write and on the boot path, and a
    latch that needed a query could not answer while Postgres was down — which
    is precisely the boot this exists for."""
    source = textwrap.dedent(inspect.getsource(chain_latch.latched))
    source += textwrap.dedent(inspect.getsource(chain_latch.load))
    source += textwrap.dedent(inspect.getsource(chain_latch._cache))
    tree = ast.parse(source)
    called = {getattr(n.func, "attr", getattr(n.func, "id", ""))
              for n in ast.walk(tree) if isinstance(n, ast.Call)}
    assert not called & {"get_pool", "acquire", "fetch", "execute"}
    assert not asyncio.iscoroutinefunction(chain_latch.latched)
