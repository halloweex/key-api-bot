"""DN-29 against a real Postgres and a temporary DuckDB, through the real boot.

`KS_WRITE_WAREHOUSE=postgres` must hold from the first row the process writes,
and web's boot writes before its scheduler exists: `init_and_sync` runs an
incremental sync over a populated DuckDB and, over an empty one, a full sync
whose last step used to be a full DuckDB rebuild. So these drive
`configure_modes()` → `init_and_sync()` → the scheduler's own jobs, with
KeyCRM stubbed and everything else real — the DuckDB store, the mirror into
Postgres, the derivation's marks and its journal, the revision read on a
connection of the cutover's own.

What is proven, per the plan's DN-29 test list:

- under postgres the boot — both paths — and three sync ticks leave
  `warehouse_dirty` exactly as it was and never call `refresh_warehouse_layers`,
  while `meta.derivation_runs` grows by one a tick;
- back under duckdb a full DuckDB rebuild is owed and the checks are held
  until a full tick validates;
- a missing precondition keeps the mode duckdb and says so on `/api/health`;
- the `warehouse` group is resolved once across two restarts.

What is not: a kill in the middle of a rebuild. That is the gate-stack
rehearsal on the production backups, run on the host, not here.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

IDS = (980001, 980002, 980003)
DERIVED = ("silver.orders", "gold.daily_revenue", "app.customer_profile",
           "silver.order_utm")


def _payload(oid, *, status_id=1, minutes=0):
    """A KeyCRM order two days old; `minutes` moves `updated_at`, and a new
    status or stamp is what makes the sync write it again."""
    ordered = datetime.now(timezone.utc) - timedelta(days=2)
    return {
        "id": oid, "source_id": 1, "status_id": status_id, "grand_total": "120.00",
        "ordered_at": ordered.isoformat(),
        "created_at": ordered.isoformat(),
        "updated_at": (ordered + timedelta(minutes=minutes)).isoformat(),
        "buyer": None, "manager": None,
        "manager_comment": f"utm_source=instagram&utm_campaign=dn29_{oid}",
        "promocode": None,
        "products": [{"id": oid * 1000 + 1, "product_id": 1, "name": "p",
                      "quantity": 1, "price_sold": "120.00"}],
    }


async def _reset(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.order_versions WHERE order_id = ANY($1::int[])", list(IDS))
        await conn.execute("DELETE FROM bronze.order_products WHERE order_id = ANY($1::int[])", list(IDS))
        await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", list(IDS))
        for table in DERIVED:
            await conn.execute(f"TRUNCATE {table}")
        await conn.execute(
            "UPDATE meta.derivation_signal SET requested = 1, built = 0, built_at = NULL"
            " WHERE layer = 'warehouse'")
        await conn.execute("DELETE FROM meta.derivation_runs")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
            [*DERIVED, "meta.derivation_signal", "meta.derivation_runs"])


async def _expenses_history(pool, done):
    """`bronze.expenses`' backfill as the switch will read it: `True` done,
    `False` a row with none, `None` no row at all — a host the landing mirror
    never reached."""
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = 'bronze.expenses'")
        if done is not None:
            await conn.execute(
                "INSERT INTO meta.mirror_state (table_name, backfilled_at)"
                " VALUES ('bronze.expenses', CASE WHEN $1 THEN now() END)", done)


async def _put_back(pool, row):
    """The `bronze.expenses` row as the test found it — other tests gate
    their reads on it."""
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = 'bronze.expenses'")
        if row is not None:
            columns = list(row.keys())
            await conn.execute(
                f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES "
                f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                *row.values())


def _met_env(monkeypatch):
    from core import warehouse_cutover as wc

    env = {
        "KS_PG_DSN": DSN,
        "KS_PG_DERIVE": "own",
        "KS_DQ_PG_WAREHOUSE": "on",
        "KS_UTM_PARSE": "postgres",
        "KS_READ_FALLBACK": "off",
        **{name: "postgres" for name in wc.WAREHOUSE_READERS},
        "KS_READ_COHORTS": "clickhouse",
        # Named and never reached: nothing here runs a ClickHouse job.
        "KS_CH_URL": "http://127.0.0.1:9",
        "KS_PG_SILVER_INTERVAL_S": "0",
        wc.ENV: "postgres",
    }
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("KS_MIRROR_LANDING", raising=False)
    # A build with no door OD-10 would have to decide — this one since
    # 2026-09-30, pinned so a door added later does not hold the switch this
    # file proves; `od10_doors` has tests of its own.
    monkeypatch.setattr(wc, "OD10_DOORS", ())


class _Client:
    """KeyCRM: an empty catalogue, and the orders the test holds now."""

    def __init__(self, world):
        self.world = world

    async def paginate(self, endpoint, params=None, page_size=50):
        if endpoint == "order":
            yield list(self.world.orders)


class World:
    """One temporary DuckDB, the real Postgres, and a way to start a new
    process over them."""

    def __init__(self, store, pool, service, resolve):
        self.store, self.pool, self.service, self.resolve = store, pool, service, resolve
        self.orders = [_payload(oid) for oid in IDS]
        self.refreshed = []

    def restart(self):
        """What a new web process has: nothing configured, nothing settled,
        the scheduler's per-process derivation state forgotten."""
        from core import pg_derivation
        from core import warehouse_cutover as wc
        from core.scheduler import BackgroundScheduler

        for name, value in (("_value", None), ("_mode", None), ("_mode_error", None),
                            ("_unmet", ()), ("_value_unmet", ()), ("_decided_for", None),
                            ("_settled", False),
                            ("_held", False), ("_held_since", None), ("_reclassify_needed", False),
                            ("_writer_record", None), ("_settle_lock", None)):
            setattr(wc, name, value)
        pg_derivation._mode = pg_derivation._mode_error = None
        BackgroundScheduler._pg_derive_ran = False
        BackgroundScheduler._pg_derive_started = None

    async def boot(self):
        """web's `startup_event`, the part that matters here: the modes,
        then the boot sync."""
        from core.runtime_modes import configure_modes
        from core.sync_service import init_and_sync

        configure_modes()
        await init_and_sync(full_sync_days=30)

    async def dirty(self):
        async with self.store.connection() as conn:
            return conn.execute(
                "SELECT value, updated_at FROM sync_metadata WHERE key = 'warehouse_dirty'"
            ).fetchone()

    async def writer(self):
        from core import warehouse_cutover as wc

        async with self.store.connection() as conn:
            row = conn.execute("SELECT value FROM sync_metadata WHERE key = ?",
                               [wc.WRITER_KEY]).fetchone()
        return None if row is None else json.loads(row[0])

    async def runs(self):
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT COUNT(*) FROM meta.derivation_runs")

    async def signal(self):
        async with self.pool.acquire() as conn:
            return tuple(await conn.fetchrow(
                "SELECT requested, built FROM meta.derivation_signal WHERE layer = 'warehouse'"))


@pytest_asyncio.fixture
async def world(monkeypatch, tmp_path):
    from core import pg_derivation, pg_utm_parse, read_fallback
    from core import sync_service as sync_module
    from core.duckdb_store import DuckDBStore
    from core.scheduler import BackgroundScheduler

    _met_env(monkeypatch)
    for module, names in ((pg_derivation, ("_mode", "_mode_error", "_signal_failures")),
                          (pg_utm_parse, ("_mode", "_mode_error")),
                          (read_fallback, ("_mode", "_mode_error", "_misconfigured"))):
        for name in names:
            monkeypatch.setattr(module, name, getattr(module, name))
    for name in ("_pg_derive_ran", "_pg_derive_started", "_pg_silver_last_at",
                 "_pg_layers_pending"):
        monkeypatch.setattr(BackgroundScheduler, name, getattr(BackgroundScheduler, name))
    pg_derivation._signal_failures = 0

    pool = await asyncpg.create_pool(DSN, min_size=2, max_size=8)
    await _reset(pool)
    # Every precondition met includes the expenses history, read for real.
    async with pool.acquire() as conn:
        history = await conn.fetchrow(
            "SELECT * FROM meta.mirror_state WHERE table_name = 'bronze.expenses'")
    await _expenses_history(pool, True)
    store = DuckDBStore(db_path=tmp_path / "dn29.duckdb")
    await store.connect()

    service = sync_module.SyncService(store)
    resolve = AsyncMock(return_value=0)
    w = World(store, pool, service, resolve)
    w.restart()

    service.sync_managers = AsyncMock(return_value=0)
    service.sync_missing_buyers = AsyncMock(return_value=0)
    service.sync_offers = AsyncMock(return_value=0)
    service.sync_stocks = AsyncMock(return_value=0)
    service._should_skip_sync = lambda: (False, "")
    service._fetch_orders_with_date_filter = AsyncMock(side_effect=lambda *_a: list(w.orders))

    # The one method under test for absence: recorded, and still real, so a
    # call that does happen does what it would.
    real_refresh = store.refresh_warehouse_layers

    async def refresh(*args, **kwargs):
        w.refreshed.append(kwargs.get("trigger"))
        return await real_refresh(*args, **kwargs)

    monkeypatch.setattr(store, "refresh_warehouse_layers", refresh)

    async def client():
        return _Client(w)

    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.alerting.raise_alert", AsyncMock(return_value=1)), \
         patch("core.alerting.resolve_group", resolve), \
         patch.object(sync_module, "get_store", AsyncMock(return_value=store)), \
         patch.object(sync_module, "get_sync_service", AsyncMock(return_value=service)), \
         patch.object(sync_module, "get_async_client", new=client), \
         patch.object(sync_module, "init_meilisearch", AsyncMock(return_value=False)), \
         patch("core.duckdb_store.get_store", AsyncMock(return_value=store)):
        yield w
    w.restart()
    await store.close()
    await _reset(pool)
    await _put_back(pool, history)
    await pool.close()


def _warehouse_resolves(resolve):
    """The switch's resolves of the `warehouse` group — the ones carrying its
    note. A DuckDB tick resolves the same group on every clean pass."""
    return [c for c in resolve.await_args_list
            if c.args and c.args[0] == "warehouse" and "note" in c.kwargs]


class TestUnderPostgres:
    @pytest.mark.asyncio
    async def test_the_boot_over_an_empty_duckdb_derives_nothing_in_duckdb(self, world):
        """The full-sync path: its last step used to be a full rebuild."""
        from core import warehouse_cutover as wc

        assert await world.dirty() is None
        await world.boot()

        assert wc.writes_postgres(), wc.status()
        assert world.refreshed == []
        assert await world.dirty() is None, "the boot marked a warehouse nobody derives"
        async with world.store.connection() as conn:
            assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == len(IDS)
            assert conn.execute("SELECT COUNT(*) FROM silver_orders").fetchone()[0] == 0
        requested, built = await world.signal()
        assert requested > built, "the boot's orders reached Postgres unmarked"
        assert (await world.writer())["writer"] == "postgres"

    @pytest.mark.asyncio
    async def test_the_boot_and_three_ticks_leave_duckdb_alone_while_postgres_derives(
        self, world,
    ):
        from core.scheduler import BackgroundScheduler

        # A populated DuckDB, and an id list pending when the switch came.
        await world.store.upsert_orders([_payload(oid) for oid in IDS])
        async with world.store.connection() as conn:
            conn.execute("INSERT OR REPLACE INTO sync_metadata (key, value, updated_at)"
                         " VALUES ('warehouse_dirty', '[980001, 980002]', CURRENT_TIMESTAMP)")
        abandoned = await world.dirty()

        world.orders = [_payload(oid, status_id=2, minutes=1) for oid in IDS]
        await world.boot()
        assert world.service._fetch_orders_with_date_filter.await_count == 1, \
            "the incremental path did not run"
        assert await world.dirty() == abandoned
        assert world.refreshed == []

        scheduler = BackgroundScheduler()
        ticks = (scheduler._run_incremental_sync, world.service.sync_today,
                 scheduler._run_order_status_refresh)
        for n, tick in enumerate(ticks, start=1):
            before = await world.runs()
            world.orders = [_payload(oid, status_id=2 + n, minutes=1 + n) for oid in IDS]
            await tick()
            assert await scheduler._run_warehouse_refresh() == {
                "skipped": True, "reason": "KS_WRITE_WAREHOUSE=postgres"}
            derived = await scheduler._run_pg_derivation()
            assert derived.get("status") == "success", (n, derived)
            assert await world.runs() == before + 1, f"tick {n} left no journal row"
            assert await world.dirty() == abandoned, f"tick {n} touched warehouse_dirty"
            assert world.refreshed == [], f"tick {n} derived in DuckDB"

        async with world.pool.acquire() as conn:
            statuses = await conn.fetch(
                "SELECT id, status_id FROM silver.orders WHERE id = ANY($1::int[])", list(IDS))
        assert {r["status_id"] for r in statuses} == {5}, "Postgres did not derive the last tick"

    @pytest.mark.asyncio
    async def test_the_resolve_runs_once_across_two_restarts(self, world):
        await world.boot()
        for _ in range(2):
            world.restart()
            await world.boot()
        from core import warehouse_cutover as wc

        (call,) = _warehouse_resolves(world.resolve)
        assert call.kwargs["note"] == wc.RESOLVE_NOTE
        assert (await world.writer())["resolved"] is True


class TestTheWayBack:
    @pytest.mark.asyncio
    async def test_a_full_rebuild_is_owed_and_the_checks_are_held_until_it_validates(
        self, world, monkeypatch,
    ):
        from core import warehouse_cutover as wc
        from core.scheduler import BackgroundScheduler

        await world.boot()                      # under postgres: DuckDB frozen
        assert (await world.writer())["writer"] == "postgres"

        world.restart()
        monkeypatch.delenv(wc.ENV)
        world.orders = [_payload(oid, status_id=7, minutes=9) for oid in IDS]
        await world.boot()

        assert not wc.writes_postgres()
        assert (await world.dirty())[0] == "full"
        assert wc.held() and wc.stood_down_duckdb_checks() == wc.STOOD_DOWN_WHEN_POSTGRES
        assert wc.warehouse_checks_stand_down(), "the mirror comparisons would run on stale Silver"
        assert wc.reclassify_needed(), "DuckDB's silver_order_utm is empty"
        assert (await world.writer())["writer"] == "postgres"

        result = await BackgroundScheduler()._run_warehouse_refresh()
        assert result["validation_passed"] is True, result
        assert world.refreshed == ["dirty_flag"]
        assert not wc.held() and wc.stood_down_duckdb_checks() == frozenset()
        assert not wc.reclassify_needed(), "the full tick parsed the verdicts back"
        assert (await world.writer())["writer"] == "duckdb"
        async with world.store.connection() as conn:
            assert conn.execute(
                "SELECT COUNT(*) FROM silver_orders WHERE status_id = 7").fetchone()[0] == len(IDS)

        # And a later start under duckdb owes nothing more.
        world.restart()
        await world.boot()
        assert not wc.held()


class TestAMissingPrecondition:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("done", [False, None], ids=["not_backfilled", "no_row"])
    async def test_expenses_without_their_history_keep_duckdb(self, world, done):
        """`_expenses_run` routes the /expenses history statements on this
        row whatever KS_READ_EXPENSES says, so the switch waits for it — read
        for real, at the start on the cutover's own connection and on the
        status page through the pool."""
        from bot.canary import check_warehouse_preconditions
        from core import warehouse_cutover as wc
        from web.routes.api.health import _warehouse_writer_mode

        await _expenses_history(world.pool, done)
        await world.boot()

        assert wc.mode() == wc.DUCKDB and wc.duckdb_derives()
        block = _warehouse_writer_mode()
        assert block["preconditions_unmet"] == ["expenses_backfilled"]
        (unmet,) = wc.preconditions_unmet()
        assert "backfilled_at is NULL for bronze.expenses" in unmet.detail
        assert [k for k, _ in check_warehouse_preconditions(
            {"warehouse_writer_mode": block})] == ["warehouse_preconditions_unmet"]
        ready = await wc.readiness()
        assert [u["key"] for u in ready["unmet"]] == ["expenses_backfilled"]

        await _expenses_history(world.pool, True)
        ready = await wc.readiness()
        assert ready["unmet"] == [] and ready["mode"] == "duckdb"

    @pytest.mark.asyncio
    async def test_a_revision_this_build_does_not_require_keeps_duckdb(
        self, world, monkeypatch,
    ):
        """The revision is read for real, on the cutover's own connection."""
        import core.pg

        from bot.canary import check_warehouse_preconditions
        from core import warehouse_cutover as wc
        from web.routes.api.health import _warehouse_writer_mode

        monkeypatch.setattr(core.pg, "REQUIRED_REVISION", "9999_not_this_one")
        await world.boot()

        assert wc.mode() == wc.DUCKDB and wc.duckdb_derives()
        block = _warehouse_writer_mode()
        assert block["preconditions_unmet"] == ["pg_revision"]
        (unmet,) = wc.preconditions_unmet()
        assert "0033" in unmet.detail and "9999_not_this_one" in unmet.detail
        assert [k for k, _ in check_warehouse_preconditions(
            {"warehouse_writer_mode": block})] == ["warehouse_preconditions_unmet"]
        # DuckDB derived its boot, as it always has, and nothing was recorded.
        assert world.refreshed == ["full_sync"]
        assert await world.writer() is None
        assert _warehouse_resolves(world.resolve) == []

    @pytest.mark.asyncio
    async def test_a_postgres_that_does_not_answer_keeps_duckdb(self, world, monkeypatch):
        from core import warehouse_cutover as wc

        monkeypatch.setattr(wc, "REVISION_RETRY_DELAYS_S", (0.0, 0.0))
        monkeypatch.setenv("KS_PG_DSN", "postgresql://ks_app:x@127.0.0.1:9/ks")
        from core.runtime_modes import configure_modes

        configure_modes()
        assert wc.mode() == wc.DUCKDB
        (unmet,) = wc.preconditions_unmet()
        assert unmet.key == "pg_revision" and "127.0.0.1" not in unmet.detail

    @pytest.mark.asyncio
    async def test_a_postgres_that_answers_on_the_second_ask_switches(
        self, world, monkeypatch,
    ):
        """After a flip a start that runs as duckdb is the way back. A first
        read refused — Postgres still coming up beside web — is asked again,
        and the second, real read decides."""
        from core import warehouse_cutover as wc

        real = wc._revision_on_its_own_connection
        asks = []

        async def refused_once(dsn):
            asks.append(dsn)
            if len(asks) == 1:
                raise ConnectionRefusedError("still starting")
            return await real(dsn)

        monkeypatch.setattr(wc, "REVISION_RETRY_DELAYS_S", (0.0, 0.0))
        monkeypatch.setattr(wc, "_revision_on_its_own_connection", refused_once)
        from core.runtime_modes import configure_modes

        configure_modes()
        assert len(asks) == 2
        assert wc.mode() == wc.POSTGRES and wc.preconditions_unmet() == ()

    @pytest.mark.asyncio
    async def test_a_connection_lost_during_the_read_is_asked_again(
        self, world, monkeypatch,
    ):
        """Connected, then lost in the middle of the SELECT — a backend
        terminated, a server restarting. That is a read that failed, not a
        database never migrated: it used to be taken for the second, never
        asked again, and after a flip that start was the way back."""
        from core import warehouse_cutover as wc
        from core.runtime_modes import configure_modes

        real = asyncpg.connection.Connection.fetchval
        reads = []

        async def lost_once(self, *args, **kwargs):
            reads.append(args[:1])
            if len(reads) == 1:
                raise asyncpg.ConnectionDoesNotExistError(
                    "connection was closed in the middle of operation")
            return await real(self, *args, **kwargs)

        monkeypatch.setattr(wc, "REVISION_RETRY_DELAYS_S", (0.0, 0.0))
        with patch.object(asyncpg.connection.Connection, "fetchval", lost_once):
            configure_modes()
        # The revision twice; the expenses history once, after the answer.
        history = (wc._EXPENSES_HISTORY_SQL,)
        assert len([r for r in reads if r != history]) == 2, reads
        assert reads[-1] == history and reads.count(history) == 1, reads
        assert wc.mode() == wc.POSTGRES, wc.preconditions_unmet()
