"""Every actor that writes orders holds the scheduler's heavy-job lock while
it does, so no two of them interleave.

The scheduled syncs always did. The manual paths did not, and that is where a
fresher order got overwritten in Postgres by an older snapshot and where
header-only orders were manufactured: a resync, a status refresh or a
backfill chunk interleaving with the minute sync. The levers and the repair
jobs run here beside a test that holds the lock, as another heavy job would
(`tests.helpers.ci_bot_sched_heavy_lock`), over the test Postgres with
KeyCRM faked at its transport; the walk at the end covers the writers
outside `core/` that nobody thought to name.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from core import pg_orders_write
from tests.helpers.alerts_journal_pg import settle
from tests.helpers.ci_bot_sched_heavy_lock import (
    admin_in_this_loop, derivation_stand, fresh_scheduler, parked_behind,
    postgres_by_connection)
from tests.helpers.orders_sync import (
    keycrm_serving_orders, order_payload, orders_held, orders_in_postgres)
from tests.integration.pg_tab_seed import DSN, needs_postgres

OID = 991_401


def _ago(**delta) -> datetime:
    return datetime.now(timezone.utc) - timedelta(**delta)


@needs_postgres
class TestTheManualLeversWaitForTheLock:
    """A lever pulled while another heavy job runs asks KeyCRM nothing until
    the lock is free, then does its work."""

    @pytest.mark.asyncio
    async def test_a_resync(self, monkeypatch):
        """Taken at the route and not again inside the full sync, which the
        weekly job calls already holding it: released, the resync finishes."""
        created = _ago(hours=3)
        async with orders_in_postgres(), postgres_by_connection(DSN):
            keycrm = keycrm_serving_orders(monkeypatch, [order_payload(OID, created=created)])
            lock = fresh_scheduler(monkeypatch)._heavy_job_lock
            async with admin_in_this_loop() as admin:
                await lock.acquire()
                try:
                    resync = asyncio.ensure_future(
                        admin.post("/api/sync/resync", params={"days": 2}))
                    await parked_behind(lock, resync)
                    assert keycrm.requests == []
                finally:
                    lock.release()
                response = await asyncio.wait_for(resync, 60)
            assert response.status_code == 200, response.text
            assert OID in await orders_held()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("background", [True, False],
                             ids=["in-the-background", "waited-for"])
    async def test_a_status_refresh(self, monkeypatch, background):
        created = _ago(hours=3)
        async with orders_in_postgres(), postgres_by_connection(DSN):
            await pg_orders_write.upsert_orders_with_expenses(
                [order_payload(OID, created=created)])
            keycrm = keycrm_serving_orders(monkeypatch, [
                order_payload(OID, created=created, status=19, group=6)])
            lock = fresh_scheduler(monkeypatch)._heavy_job_lock
            async with admin_in_this_loop() as admin:
                await lock.acquire()
                try:
                    refresh = asyncio.ensure_future(admin.post(
                        "/api/sync/refresh-statuses",
                        params={"days": 2, "background": background}))
                    await parked_behind(lock, None if background else refresh)
                    assert keycrm.requests == []
                    assert (await orders_held())[OID]["status"] == 12
                finally:
                    lock.release()
                response = await asyncio.wait_for(refresh, 60)
                await settle()   # a refresh started in the background, to its end
            assert response.status_code == 200, response.text
            assert (await orders_held())[OID]["status"] == 19

    @pytest.mark.asyncio
    async def test_a_warehouse_refresh(self, monkeypatch):
        """Its derivation takes the lock itself, and `asyncio.Lock` is not
        reentrant: the lever must not hold it around the call. Interleaved
        with another job it produced false validation failures."""
        at = datetime(2026, 3, 10, 9, 0, tzinfo=timezone.utc)
        async with derivation_stand(monkeypatch, DSN, [
                (OID, 1, 1, 120, at, None, None, None)]) as pool:
            lock = fresh_scheduler(monkeypatch)._heavy_job_lock
            before = await pool.fetchval("SELECT COALESCE(max(id), 0) FROM meta.derivation_runs")

            async def derived():
                silver = [r["id"] for r in await pool.fetch("SELECT id FROM silver.orders")]
                runs = [r["trigger"] for r in await pool.fetch(
                    "SELECT trigger FROM meta.derivation_runs WHERE id > $1 ORDER BY id",
                    before)]
                return silver, runs

            async with admin_in_this_loop() as admin:
                await lock.acquire()
                try:
                    refresh = asyncio.ensure_future(admin.post("/api/warehouse/refresh"))
                    await parked_behind(lock, refresh)
                    assert await derived() == ([], [])
                finally:
                    lock.release()
                response = await asyncio.wait_for(refresh, 60)
            assert response.status_code == 200, response.text
            assert response.json()["postgres"]["status"] == "success"
            assert await derived() == ([OID], ["manual"])


async def _write_beside_the_lock(monkeypatch, run, *, held, served, prepare=None):
    """Postgres holding `held`, KeyCRM serving `served`; `run(scheduler)`
    started while the test holds the lock. Returns the orders Postgres held
    while it waited, and once it was done."""
    async with orders_in_postgres(), postgres_by_connection(DSN):
        await pg_orders_write.upsert_orders_with_expenses(held)
        keycrm_serving_orders(monkeypatch, served)
        scheduler = fresh_scheduler(monkeypatch)
        if prepare is not None:
            await prepare(scheduler)
        lock = scheduler._heavy_job_lock
        await lock.acquire()
        try:
            job = asyncio.ensure_future(run(scheduler))
            await parked_behind(lock, job)
            while_held = await orders_held()
        finally:
            lock.release()
        await asyncio.wait_for(job, 60)
        return while_held, await orders_held()


@needs_postgres
class TestTheRepairsWriteUnderTheLock:
    """Each job that re-fetches orders by id writes nothing while another
    heavy job holds the lock, and writes once it is free. A repair's write
    beside a sync's put an older payload over a fresher order, and one
    between a Silver commit and its validation read failed a correct
    rebuild."""

    @pytest.mark.asyncio
    async def test_the_half_written_repair(self, monkeypatch):
        created = _ago(days=2)
        bare = dict(order_payload(OID, created=created), products=[])
        while_held, after = await _write_beside_the_lock(
            monkeypatch, lambda s: s._run_halfwritten_repair(),
            held=[bare], served=[order_payload(OID, created=created)])
        assert while_held[OID]["lines"] == []
        assert after[OID]["lines"] == [OID * 1000]

    @pytest.mark.asyncio
    async def test_the_gap_backfill(self, monkeypatch):
        created = _ago(days=400)
        neighbours = [order_payload(OID - 1, created=created),
                      order_payload(OID + 1, created=created)]
        while_held, after = await _write_beside_the_lock(
            monkeypatch, lambda s: s._run_order_gap_backfill(),
            held=neighbours, served=neighbours + [order_payload(OID, created=created)])
        assert OID not in while_held
        assert OID in after

    @pytest.mark.asyncio
    async def test_the_reconciliations_repair(self, monkeypatch):
        """The 05:30 job re-fetches what Postgres lacks. Its verdict's journal
        is not what is under test here, so it is not written."""
        created = _ago(days=10)
        other = order_payload(OID + 5, created=created)

        async def prepare(scheduler):
            from tests.helpers.access_lists import execute

            await execute("UPDATE meta.mirror_state SET backfilled_at = now()"
                          " WHERE table_name = 'bronze.orders'")
            scheduler._persist_postgres_reconciliation = AsyncMock()

        monkeypatch.delenv("KS_CH_URL", raising=False)
        while_held, after = await _write_beside_the_lock(
            monkeypatch, lambda s: s._run_dq_reconciliation(),
            held=[other], served=[other, order_payload(OID, created=created)],
            prepare=prepare)
        assert OID not in while_held
        assert OID in after


def test_every_writer_of_orders_outside_core_answers_for_the_lock():
    """Walks `web/` and `scripts/` for a function that writes the orders — an
    UPDATE against `orders` in its SQL, or a call of chain 3's
    `restore_manager_comments` — and requires it to say how it is serialised
    against the sync.

    Named subjects were tried and are the reason this exists: the two
    `manager_comment` backfills (DN-17) updated DuckDB directly and neither
    held the lock nor reached Postgres, and no test noticed because every
    lock test above names the function it guards. A guard that names its
    subjects only ever guards the ones you were already thinking about —
    the same lesson as `docker rm -v` in the gate scripts.

    **Since step 14's S14-09 neither backfill writes DuckDB**: both restore
    the comments through `pg_orders_write.restore_manager_comments`, chain
    3's, and their DuckDB `UPDATE orders` went with `_orders_in_postgres`.
    The rule carries over unchanged to that call — it is the same write,
    racing the same sync, in the store the sync now writes.

    Two acceptable answers, because the callers are genuinely different:
    `_heavy_job_lock` for anything running inside the web process, and
    `core.pg_orders_write.WEB_MUST_BE_STOPPED` (in `core.pg_backfill` until
    step 14's S14-13) for a CLI, where that lock would be
    a fresh object guarding nothing. The answer may be given by the function
    or by a function of the same module that calls it (the CLI warns in
    `backfill_utm`, its entry point, and writes in the helper it calls).

    **The two are looked for differently, and the difference is the point.**
    The lock is `async with get_scheduler()._heavy_job_lock`, an attribute the
    code evaluates, and that is all it can be. The constant is not a magic
    word — the whole mechanism separating that CLI from a sync tick is an
    operator reading a sentence — so it has to be an **argument to a call the
    function makes**, which is what `logger.warning(...)` is. Two drafts were
    weaker: the first read `ast.get_source_segment`, so a comment three
    paragraphs below the deleted line still said the words; the second
    collected `ast.alias` nodes, so the bare `from core.pg_orders_write import
    WEB_MUST_BE_STOPPED` that a deleted warning leaves behind was accepted as
    proof the operator had been told. Both were found by mutation, the second
    by the review's.
    """
    import ast
    import pathlib
    import re

    repo = pathlib.Path(__file__).resolve().parents[2]
    LOCK, WARNING = "_heavy_job_lock", "WEB_MUST_BE_STOPPED"
    WRITER = "restore_manager_comments"
    updates = re.compile(r"update\s+(?:bronze\.)?orders\s+set", re.IGNORECASE)

    def _evaluated(node):
        """Every identifier the function evaluates. Imports are not evaluation
        — an unused name proves only that somebody once needed it."""
        out = set()
        for n in ast.walk(node):
            if isinstance(n, ast.Attribute):
                out.add(n.attr)
            elif isinstance(n, ast.Name):
                out.add(n.id)
        return out

    def _passed_to_a_call(node):
        """Every identifier handed to some call as an argument or keyword."""
        out = set()
        for n in ast.walk(node):
            if not isinstance(n, ast.Call):
                continue
            for arg in list(n.args) + [k.value for k in n.keywords]:
                for sub in ast.walk(arg):
                    if isinstance(sub, ast.Name):
                        out.add(sub.id)
                    elif isinstance(sub, ast.Attribute):
                        out.add(sub.attr)
        return out

    def _calls(node):
        return {(n.func.id if isinstance(n.func, ast.Name) else getattr(n.func, "attr", ""))
                for n in ast.walk(node) if isinstance(n, ast.Call)}

    found = []
    for root in ("web", "scripts"):
        for path in sorted((repo / root).rglob("*.py")):
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source)
            # Docstrings are excluded deliberately: the prose in these modules
            # discusses updating orders, and a search that could not tell
            # prose from SQL would match the sentence explaining the rule.
            docstrings = set()
            for node in ast.walk(tree):
                if isinstance(node, (ast.Module, ast.ClassDef,
                                     ast.FunctionDef, ast.AsyncFunctionDef)):
                    first = node.body[0] if node.body else None
                    if (isinstance(first, ast.Expr)
                            and isinstance(first.value, ast.Constant)
                            and isinstance(first.value.value, str)):
                        docstrings.add(id(first.value))
            functions = [
                n for n in ast.walk(tree)
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            ]
            writes = [
                node for node in ast.walk(tree)
                if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                    and id(node) not in docstrings
                    and updates.search(" ".join(node.value.split())))
                or (isinstance(node, ast.Call)
                    and WRITER in {getattr(node.func, "attr", None),
                                   getattr(node.func, "id", None)})
            ]
            for node in writes:
                # Every function containing the write, innermost to
                # outermost — a nested helper may be wrapped by the caller
                # that takes the lock — and, to a fixed point, every function
                # of the module that calls one of them.
                enclosing = [
                    f for f in functions
                    if f.lineno <= node.lineno <= (f.end_lineno or f.lineno)
                ]
                assert enclosing, f"{path.name}: an orders write outside any function"
                answering = list(enclosing)
                while True:
                    names = {f.name for f in answering}
                    more = [f for f in functions
                            if f not in answering and _calls(f) & names]
                    if not more:
                        break
                    answering += more
                evaluated = set().union(*(_evaluated(f) for f in answering))
                told = set().union(*(_passed_to_a_call(f) for f in answering))
                found.append(f"{path.name}:{node.lineno}")
                assert LOCK in evaluated or WARNING in told, (
                    f"{path.name}:{node.lineno} writes orders without taking "
                    f"the heavy-job lock, and without passing {WARNING} to "
                    f"anything that would put it in front of an operator"
                )

    assert len(found) >= 2, f"no orders write walked: {found}"
