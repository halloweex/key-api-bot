"""Chain 5: the manager classification and stats, against a real Postgres.

What the writer has to survive, each measured on PostgreSQL 17.2 before it was
built (chain 5's design, M1–M11), and each with the mutation it kills:

- a sync never puts the retail seed over a human's classification, and every
  manager lands with a baseline interval in the same transaction;
- a classification closes, replaces and opens in one transaction, and a
  failure in the middle leaves all of it — and the owner rows — as they were;
- two classifications of one manager in flight at once leave one open
  interval, because one advisory lock serialises every writer;
- a backdate behind the manager's latest change is refused before the latch;
- the stats count each order on its Kyiv date whatever the session's zone;
- the replica stands down on the flag and on an owner row alone;
- the copy-back carries both tables back whole, owes DuckDB a full rebuild,
  releases, and the replica then ships what came back;
- the handover says what each table's state is before and after a flip;
- the standing watch's one read reports each seeded shape;
- the dashboard's `{managers}` join and the route follow the chain.

Everything goes through the repository methods every caller reaches. The
four preconditions the chain is held by are facts no environment variable
sets — the goal bridge, step 13, chain 3 — so the fixture holds them met; the
tests in `tests/unit/test_managers_chain.py` prove they hold the chain back.
"""
from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import chain_latch, pg_managers_write

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

CHAIN = pg_managers_write.CHAIN
ENV = pg_managers_write.WRITE_ENV
BASELINE = date(1970, 1, 1)
# Order ids far from anything another test commits.
ORDER_IDS = (970_000_001, 970_000_002, 970_000_003)


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.manager_classifications")
        await conn.execute("DELETE FROM bronze.managers")
        await conn.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])",
                           list(ORDER_IDS))
        await conn.execute("DELETE FROM silver.orders WHERE id = ANY($1::int[])",
                           list(ORDER_IDS))
        # Left behind, an owner row reads in the next module as a chain owning
        # a table with no local marker.
        await conn.execute("DELETE FROM meta.chain_watermarks "
                           "WHERE key LIKE 'owner:%' OR key = 'last_sync_managers'")
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name IN "
                           "('bronze.managers', 'app.manager_classifications')")


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    """A real DuckDB, a live Postgres, the landing mirror on (so the replica
    ships while the chain is off), no chain flag set, and chain 5's
    preconditions held met."""
    from core.duckdb_store import DuckDBStore

    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setenv("KS_MIRROR_LANDING", "on")
    monkeypatch.setattr(pg_managers_write, "unmet_precondition", lambda: None)
    store = DuckDBStore(db_path=tmp_path / "managers.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=2, max_size=6)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
            patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()
    await store.close()


async def _pg_managers(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, is_retail, first_order_date, last_order_date, "
            "order_count FROM bronze.managers ORDER BY id")
    return {r["id"]: dict(r) for r in rows}


async def _pg_intervals(pool, manager_id=None):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT manager_id, valid_from, valid_to, is_retail, set_by, note "
            "FROM app.manager_classifications "
            "WHERE $1::int IS NULL OR manager_id = $1 ORDER BY manager_id, valid_from",
            manager_id)
    return [tuple(r) for r in rows]


async def _duck_intervals(store):
    async with store.connection() as conn:
        return conn.execute(
            "SELECT manager_id, valid_from, valid_to, is_retail, set_by, note "
            "FROM manager_classifications ORDER BY manager_id, valid_from").fetchall()


def _open(intervals, manager_id):
    return [i for i in intervals if i[0] == manager_id and i[2] is None]


class _Conn:
    """A live connection, except that `fail_on` raises where the writer runs
    that statement, and `pause_after` holds the writer after it — so a test
    can inject a failure or a race exactly where it means to."""

    def __init__(self, conn, *, fail_on=None, pause_after=None, paused=None):
        self._conn, self.fail_on = conn, fail_on
        self.pause_after, self.paused = pause_after, paused

    async def execute(self, sql, *args):
        if self.fail_on and sql.startswith(self.fail_on):
            raise asyncpg.DataError("injected")
        out = await self._conn.execute(sql, *args)
        if self.pause_after and sql.startswith(self.pause_after):
            self.paused.set()
            await asyncio.sleep(0.5)
        return out

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _Pool:
    def __init__(self, live, **conn_kwargs):
        self._live, self._kwargs = live, conn_kwargs

    def acquire(self, *args, **kwargs):
        ctx, kw = self._live.acquire(*args, **kwargs), self._kwargs

        class _Ctx:
            async def __aenter__(self_inner):
                return _Conn(await ctx.__aenter__(), **kw)

            async def __aexit__(self_inner, *exc):
                return await ctx.__aexit__(*exc)
        return _Ctx()

    def __getattr__(self, name):
        return getattr(self._live, name)


class TestTheSyncNeverRevertsAHuman:
    @pytest.mark.asyncio
    async def test_a_classification_survives_the_next_sync_and_a_new_manager_lands_classified(
            self, stores):
        """M4. Mutation: `is_retail = EXCLUDED.is_retail` in the conflict
        update, or the baseline seed removed."""
        store, pool, env = stores
        env.setenv(ENV, "postgres")

        await store.upsert_managers([{"id": 34, "name": "Олена"}])
        assert chain_latch.latched(CHAIN)
        await store.set_manager_retail_status(34, True, date(2026, 10, 7), 1, "retail now")
        await store.upsert_managers([{"id": 34, "name": "Олена П."}, {"id": 41, "name": "New"}])

        managers = await _pg_managers(pool)
        assert managers[34]["is_retail"] is True and managers[34]["name"] == "Олена П."
        assert managers[41]["is_retail"] is False
        intervals = await _pg_intervals(pool)
        assert (41, BASELINE, None, False, None,
                "baseline frozen from managers.is_retail") in intervals
        # The manager and its baseline in one transaction, and in it the
        # owner rows were already claimed by the first write.
        async with pool.acquire() as conn:
            pair = await conn.fetch(
                "SELECT xmin::text AS x FROM bronze.managers WHERE id = 41 "
                "UNION ALL SELECT xmin::text FROM app.manager_classifications "
                "WHERE manager_id = 41")
            owners = await conn.fetch("SELECT key FROM meta.chain_watermarks "
                                      "WHERE key LIKE 'owner:%'")
        assert len({r["x"] for r in pair}) == 1
        assert {r["key"] for r in owners} == {"owner:bronze.managers",
                                              "owner:app.manager_classifications"}
        # Nothing reached DuckDB.
        assert await _duck_intervals(store) == []

    @pytest.mark.asyncio
    async def test_a_manager_the_replica_carried_without_a_baseline_gets_one(self, stores):
        """M2: DuckDB seeds only at a boot, so the replica can carry a manager
        with no interval. Mutation: seed only the batch's ids."""
        store, pool, env = stores
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.managers (id, name, is_retail) "
                               "VALUES (17, 'Inherited', TRUE)")
        env.setenv(ENV, "postgres")
        await store.upsert_managers([{"id": 50, "name": "Other"}])
        assert _open(await _pg_intervals(pool), 17) == [
            (17, BASELINE, None, True, None, "baseline frozen from managers.is_retail")]
        before = await _pg_intervals(pool)
        await store.upsert_managers([{"id": 50, "name": "Other"}])
        assert await _pg_intervals(pool) == before, "the seed is not idempotent"


class TestAClassification:
    async def _seeded(self, stores, mid=34):
        store, pool, env = stores
        env.setenv(ENV, "postgres")
        await store.upsert_managers([{"id": mid, "name": "M"}])
        return store, pool

    @pytest.mark.asyncio
    async def test_forward_closes_opens_and_sets_together(self, stores):
        store, pool = await self._seeded(stores)
        await store.set_manager_retail_status(34, True, date(2026, 6, 1), 7, "why")
        assert await _pg_intervals(pool, 34) == [
            (34, BASELINE, date(2026, 6, 1), False, None,
             "baseline frozen from managers.is_retail"),
            (34, date(2026, 6, 1), None, True, 7, "why")]
        assert (await _pg_managers(pool))[34]["is_retail"] is True

    @pytest.mark.asyncio
    async def test_a_failure_before_the_insert_leaves_everything_as_it_was(self, stores):
        """One transaction. Mutation: split the statements into autocommits —
        the manager would be left with no open interval."""
        store, pool, env = stores
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.managers (id, name) VALUES (34, 'M')")
            await conn.execute(
                "INSERT INTO app.manager_classifications (manager_id, is_retail, "
                "valid_from, set_at) VALUES (34, FALSE, '1970-01-01', now())")
        env.setenv(ENV, "postgres")
        before = await _pg_intervals(pool)
        failing = _Pool(pool, fail_on="INSERT INTO app.manager_classifications "
                                      "(manager_id, is_retail, valid_from, valid_to, set_by, "
                                      "set_at,  note, mirrored_at) VALUES")
        with patch("core.pg.get_pool", new=AsyncMock(return_value=failing)):
            with pytest.raises(asyncpg.DataError):
                await store.set_manager_retail_status(34, True, date(2026, 6, 1), 7, None)
        assert await _pg_intervals(pool) == before
        assert (await _pg_managers(pool))[34]["is_retail"] is False
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT count(*) FROM meta.chain_watermarks WHERE key LIKE 'owner:%'") == 0

    @pytest.mark.asyncio
    async def test_the_same_date_twice_is_one_row(self, stores):
        """M1b. Mutation: drop the DELETE — the primary key refuses the second."""
        store, pool = await self._seeded(stores)
        await store.set_manager_retail_status(34, True, date(2026, 6, 1), 7, "a")
        await store.set_manager_retail_status(34, False, date(2026, 6, 1), 7, "b")
        assert await _pg_intervals(pool, 34) == [
            (34, BASELINE, date(2026, 6, 1), False, None,
             "baseline frozen from managers.is_retail"),
            (34, date(2026, 6, 1), None, False, 7, "b")]

    @pytest.mark.asyncio
    async def test_two_in_flight_at_once_leave_one_open_interval(self, stores):
        """M5's interleaving: A holds the baseline it just closed when B
        starts. Without the advisory lock B's close finds nothing open once A
        commits and B opens a second interval (measured: 2 open). Mutation:
        drop the lock."""
        store, pool = await self._seeded(stores)
        paused = asyncio.Event()
        a_pool = _Pool(pool, pause_after="UPDATE app.manager_classifications",
                       paused=paused)
        with patch("core.pg.get_pool", new=AsyncMock(side_effect=[a_pool, pool])):
            a = asyncio.ensure_future(
                store.set_manager_retail_status(34, True, date(2026, 6, 1), 1, "A"))
            await asyncio.wait_for(paused.wait(), 5)
            b = asyncio.ensure_future(
                store.set_manager_retail_status(34, False, date(2026, 6, 2), 2, "B"))
            await asyncio.gather(a, b)
        intervals = await _pg_intervals(pool, 34)
        assert len(_open(intervals, 34)) == 1, intervals
        assert _open(intervals, 34)[0][1:4] == (date(2026, 6, 2), None, False)
        assert (await _pg_managers(pool))[34]["is_retail"] is False

    @pytest.mark.asyncio
    async def test_a_backdate_behind_the_latest_change_is_refused_and_latches_nothing(
            self, stores):
        """OD-C5-1 (a). DuckDB writes two open intervals here (M1). On a
        chain that has never written, the refusal must not latch it."""
        store, pool, env = stores
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.managers (id, name) VALUES (34, 'M')")
            await conn.execute(
                "INSERT INTO app.manager_classifications (manager_id, is_retail, "
                "valid_from, valid_to, set_at) VALUES "
                "(34, FALSE, '1970-01-01', '2026-03-01', now()), "
                "(34, TRUE, '2026-03-01', NULL, now())")
        env.setenv(ENV, "postgres")
        before = await _pg_intervals(pool)
        with pytest.raises(pg_managers_write.BackdateBehindLatest):
            await store.set_manager_retail_status(34, False, date(2026, 1, 1), 1, None)
        assert await _pg_intervals(pool) == before
        assert not chain_latch.marker_path(CHAIN).exists()


class TestTheStats:
    @pytest.mark.asyncio
    async def test_each_order_counts_on_its_kyiv_date_in_a_utc_session(self, stores):
        """M3. Mutation: `ordered_at::date` — a day early for every order
        between 00:00 and 03:00 Kyiv. And DuckDB, under Kyiv, agrees."""
        store, pool, env = stores
        stamps = (datetime(2026, 2, 28, 22, 30, tzinfo=timezone.utc),
                  datetime(2026, 7, 31, 21, 15, tzinfo=timezone.utc))
        async with pool.acquire() as conn:
            assert await conn.fetchval("SHOW timezone") == "UTC"
            await conn.execute("INSERT INTO bronze.managers (id, name) VALUES (7, 'M')")
            for oid, at in zip(ORDER_IDS, stamps):
                await conn.execute(
                    "INSERT INTO bronze.orders (id, source_id, status_id, grand_total, "
                    "ordered_at, manager_id) VALUES ($1, 1, 1, 10, $2, 7)", oid, at)
        env.setenv(ENV, "postgres")
        assert await store.update_manager_stats() >= 1
        row = (await _pg_managers(pool))[7]
        assert (row["first_order_date"], row["last_order_date"], row["order_count"]) == (
            date(2026, 3, 1), date(2026, 8, 1), 2)

        env.setenv(ENV, "duckdb")
        chain_latch.release(CHAIN)
        async with store.connection() as conn:
            conn.execute("SET TimeZone = 'Europe/Kyiv'")
            conn.execute("INSERT INTO managers (id, name) VALUES (7, 'M')")
            for oid, at in zip(ORDER_IDS, stamps):
                conn.execute("INSERT INTO orders (id, source_id, status_id, grand_total, "
                             "ordered_at, manager_id) VALUES (?, 1, 1, 10, ?, 7)", [oid, at])
        with patch("core.pg_replication.replicate_managers", new=AsyncMock()):
            await store.update_manager_stats()
        async with store.connection() as conn:
            duck = conn.execute("SELECT first_order_date, last_order_date, order_count "
                                "FROM managers WHERE id = 7").fetchone()
        assert duck == (date(2026, 3, 1), date(2026, 8, 1), 2)


class TestTheReplicaStandsDown:
    async def _duckdb_holds_one(self, store):
        async with store.connection() as conn:
            conn.execute("INSERT INTO managers (id, name, is_retail) VALUES (9, 'D', TRUE)")

    @pytest.mark.asyncio
    async def test_on_the_flag(self, stores):
        """M8. Mutation: drop DN-22b's question from replicate_managers."""
        from core.pg_replication import replicate_managers

        store, pool, env = stores
        await self._duckdb_holds_one(store)
        env.setenv(ENV, "postgres")
        out = await replicate_managers(store)
        assert out["ok"] is False and "stood down" in out["skipped"]
        assert await _pg_managers(pool) == {}

    @pytest.mark.asyncio
    async def test_on_an_owner_row_alone(self, stores):
        """The marker lost with the flag off: the owner row still holds it."""
        from core.pg_replication import replicate_managers

        store, pool, env = stores
        await self._duckdb_holds_one(store)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                "VALUES ('owner:bronze.managers', '2026-10-07T00:00:00+00:00', now())")
        out = await replicate_managers(store)
        assert out["ok"] is False and "stood down" in out["skipped"]
        assert await _pg_managers(pool) == {}

    @pytest.mark.asyncio
    async def test_and_ships_without_either(self, stores):
        """The control: the same store, no flag, no owner row."""
        from core.pg_replication import replicate_managers

        store, pool, _env = stores
        await self._duckdb_holds_one(store)
        assert (await replicate_managers(store))["ok"] is True
        assert (await _pg_managers(pool))[9]["is_retail"] is True


async def _level(store):
    """DuckDB with managers and their baselines, and Postgres an exact copy of
    it — what a healthy pre-flip production looks like."""
    from core.pg_replication import replicate_managers

    await store.upsert_managers([{"id": 4, "name": "Retail"}, {"id": 34, "name": "Other"}])
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO manager_classifications (manager_id, is_retail, valid_from, note) "
            "SELECT id, is_retail, DATE '1970-01-01', 'baseline frozen from managers.is_retail' "
            "FROM managers")
    assert (await replicate_managers(store))["ok"] is True


class TestTheCopyBack:
    @pytest.mark.asyncio
    async def test_a_round_trip_carries_the_classification_and_owes_a_full_rebuild(self, stores):
        """Mutation: drop the replicated source — `chain_specs` raises — or
        the full-rebuild mark."""
        from core.chain_transfer import copy_back
        from core.pg_replication import replicate_managers

        store, pool, env = stores
        await _level(store)
        env.setenv(ENV, "postgres")
        await store.set_manager_retail_status(34, True, date(2026, 6, 1), 7, "a")
        await store.set_manager_retail_status(34, False, date(2026, 6, 1), 7, "same day")
        await store.set_manager_retail_status(4, False, date(2026, 7, 1), 7, "no longer")
        await store.set_last_sync_time("managers", datetime(2026, 10, 7, 9, tzinfo=timezone.utc))
        pg_before = await _pg_intervals(pool)

        plan = await copy_back(store, pg_managers_write, dry_run=False)

        assert plan["committed"] and plan["released"] and plan["full_rebuild_owed"], plan
        assert [tuple(r) for r in await _duck_intervals(store)] == pg_before
        async with store.connection() as conn:
            assert conn.execute("SELECT is_retail FROM managers WHERE id = 4").fetchone() == (False,)
            keys = dict(conn.execute(
                "SELECT key, value FROM sync_metadata WHERE key IN "
                "('last_sync_managers', 'warehouse_dirty')").fetchall())
        assert keys["warehouse_dirty"] == "full"
        assert keys["last_sync_managers"].startswith("2026-10-07T09:00")
        assert not chain_latch.latched(CHAIN)
        async with pool.acquire() as conn:
            assert await conn.fetchval("SELECT count(*) FROM meta.chain_watermarks "
                                       "WHERE key LIKE 'owner:%'") == 0

        # And the copy it hands back to is the copy it took over from.
        env.setenv(ENV, "duckdb")
        assert (await replicate_managers(store))["ok"] is True
        assert await _pg_intervals(pool) == pg_before


class TestTheHandover:
    @pytest.mark.asyncio
    async def test_before_a_flip_a_baseline_only_duckdb_holds_names_its_shipper(self, stores):
        """A manager synced since web's last start: the copy-back's own
        connect seeds DuckDB a baseline Postgres lacks."""
        from core.chain_transfer import handover_check

        store, pool, _env = stores
        await _level(store)
        async with store.connection() as conn:
            conn.execute("INSERT INTO managers (id, name) VALUES (60, 'N')")
            conn.execute("INSERT INTO manager_classifications (manager_id, is_retail, "
                         "valid_from) VALUES (60, FALSE, DATE '1970-01-01')")
        issues = await handover_check(store, pg_managers_write)
        critical = [i for i in issues if i.severity.value == "CRITICAL"]
        assert {i.table_name for i in critical} == {"bronze.managers",
                                                    "app.manager_classifications"}
        assert all("replicate_managers" in i.description
                   and "manager_stats/trigger" in i.description for i in critical)

    @pytest.mark.asyncio
    async def test_before_a_flip_an_interval_only_postgres_holds_is_critical(self, stores):
        """Mutation: leave it INFO — there is no next full replace after a flip."""
        from core.chain_transfer import handover_check

        store, pool, _env = stores
        await _level(store)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO app.manager_classifications (manager_id, is_retail, "
                "valid_from, set_at) VALUES (34, TRUE, '2026-01-01', now())")
        (issue,) = await handover_check(store, pg_managers_write)
        assert (issue.check_name, issue.severity.value) == ("handover_rows_ahead", "CRITICAL")
        assert issue.table_name == "app.manager_classifications"

    @pytest.mark.asyncio
    async def test_after_the_latch_a_later_duckdb_decision_is_refused(self, stores):
        from core.chain_transfer import handover_check

        store, pool, env = stores
        await _level(store)
        env.setenv(ENV, "postgres")
        await store.set_manager_retail_status(34, True, date(2026, 6, 1), 7, "pg")
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO manager_classifications (manager_id, is_retail, valid_from, "
                "set_at) VALUES (34, FALSE, DATE '2026-06-01', ?)",
                [datetime.now(timezone.utc) + timedelta(hours=1)])
        issues = await handover_check(store, pg_managers_write)
        assert ("handover_rows_newer_in_duckdb", "CRITICAL") in {
            (i.check_name, i.severity.value) for i in issues}


class TestTheStandingWatchReadsTheShape:
    @pytest.mark.asyncio
    async def test_each_seeded_shape_is_reported_once(self, stores):
        """M11's seeds, through `read_facts`. Mutation: any SQL predicate."""
        from core import pg_chain_invariants as inv

        store, pool, _env = stores
        chain_latch.latch(CHAIN)
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO bronze.managers (id, name, is_retail) VALUES "
                "(1,'clean',TRUE),(2,'overlap',FALSE),(3,'gap',FALSE),"
                "(4,'closed-only',TRUE),(5,'none',FALSE),(6,'disagree',FALSE),"
                "(7,'two-open',FALSE),(8,'future',TRUE)")
            await conn.execute(
                "INSERT INTO app.manager_classifications "
                "(manager_id, is_retail, valid_from, valid_to, set_at) VALUES "
                "(1,TRUE,'1970-01-01',NULL,now()),"
                "(2,FALSE,'1970-01-01','2026-03-01',now()),(2,FALSE,'2026-01-01',NULL,now()),"
                "(3,FALSE,'1970-01-01','2026-01-01',now()),(3,FALSE,'2026-02-01',NULL,now()),"
                "(4,TRUE,'1970-01-01','2026-04-01',now()),"
                "(6,TRUE,'1970-01-01',NULL,now()),"
                "(7,FALSE,'1970-01-01','2026-03-01',NULL),(7,FALSE,'2026-01-01',NULL,now()),"
                "(7,TRUE,'2026-03-01',NULL,now()),"
                "(8,FALSE,'1970-01-01','2099-01-01',now()),(8,TRUE,'2099-01-01',NULL,now()),"
                "(99,TRUE,'1970-01-01',NULL,now())")
        facts = await inv.read_facts(pool=pool)
        m = facts.managers
        assert isinstance(m, inv.Managers), facts
        assert (m.unclassified, m.open_wrong, m.broken, m.disagree, m.orphans) == (
            (5,), (4, 7), (2, 3, 7), (6, 7), (99,))
        assert m.nulls.counts == {"set_at": 1} and m.managers == 8
        found = {i.check_name for i in inv.check_chain_invariants(facts)}
        assert {inv.MANAGER_UNCLASSIFIED, inv.MANAGER_OPEN_INTERVAL,
                inv.MANAGER_INTERVALS_BROKEN, inv.MANAGER_RETAIL_DISAGREES,
                inv.COLUMN_NULL} <= found


class TestTheReadsFollow:
    @pytest.mark.asyncio
    async def test_the_dashboard_names_the_manager_postgres_holds(self, stores):
        """`_RETURN_ORDERS_SQL` with KS_READ_DASHBOARD at duckdb: under the
        chain the join reads Postgres. Mutation: drop `reads_the_chain` in
        `_dashboard_run`."""
        from core.repositories.revenue import _RETURN_ORDERS_SQL

        store, pool, env = stores
        env.setenv("KS_READ_DASHBOARD", "duckdb")
        env.setenv(ENV, "postgres")
        await store.upsert_managers([{"id": 34, "name": "Postgres name"}])
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO silver.orders (id, source_id, status_id, grand_total, "
                "manager_id, order_date, is_return, sales_type, is_active_source, "
                "source_name) VALUES ($1, 1, 19, 10, 34, '2026-10-01', TRUE, 'internal', "
                "TRUE, 'Instagram')", ORDER_IDS[0])
        rows = await store._dashboard_run(
            _RETURN_ORDERS_SQL.replace("{where}", "s.id = ?"), [ORDER_IDS[0], 5])
        assert [r[-1] for r in rows] == ["Postgres name"]

    @pytest.mark.asyncio
    async def test_the_route_writes_postgres_marks_the_derivation_and_lists_it(
            self, stores, monkeypatch):
        """End to end, KS_PG_DERIVE=own: the decision in Postgres, the signal
        raised in its transaction, DuckDB untouched, the list from Postgres."""
        import time as _time

        import httpx

        from core import pg_derivation
        from core.permissions import ADMIN_USER_IDS
        from web.main import app
        from web.routes.api._deps import limiter
        from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

        store, pool, env = stores
        env.setenv(ENV, "postgres")
        monkeypatch.setattr(pg_derivation, "_mode", pg_derivation.OWN)
        await store.upsert_managers([{"id": 34, "name": "M"}])
        async with pool.acquire() as conn:
            requested = await conn.fetchval(
                "SELECT requested FROM meta.derivation_signal WHERE layer = 'warehouse'")

        async def get():
            return store
        monkeypatch.setattr("web.routes.api.admin.get_store", get)
        monkeypatch.setattr(store, "get_manager_sales_365d", AsyncMock(return_value=[]))
        limiter.reset()
        admin = sorted(ADMIN_USER_IDS)[0]
        cookie = session_serializer.dumps(create_session_data(
            {"id": str(admin), "first_name": "T", "last_name": "U", "username": "t",
             "auth_date": str(int(_time.time()))}, role="admin"))
        # In this test's own loop: the pool the writer is handed belongs to
        # it, and TestClient would run the app in another.
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url="http://test",
                                         cookies={SESSION_COOKIE: cookie}) as client:
                res = await client.post("/api/managers/34/retail-status?is_retail=true"
                                        "&effective_from=2026-10-07")
                assert res.status_code == 200, res.text
                listed = (await client.get("/api/managers")).json()["managers"]
        finally:
            limiter.reset()
        assert "stood down" in res.json()["replica"]["skipped"]
        assert _open(await _pg_intervals(pool), 34)[0][1:4] == (date(2026, 10, 7), None, True)
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT requested FROM meta.derivation_signal "
                "WHERE layer = 'warehouse'") == requested + 1
        assert await _duck_intervals(store) == []
        assert [(m["id"], m["is_retail"]) for m in listed] == [(34, True)]


# ─── The review of chain 5 (2026-10-08) ─────────────────────────────────────


class TestTheReviewAgainstARealPostgres:
    @pytest.mark.asyncio
    async def test_the_in_lock_seed_keeps_an_inherited_retail_history_retail(self, stores):
        """A manager the replica carried with `is_retail` and no interval, then
        classified non-retail: its past stays what the seed said. Mutation:
        `WHERE FALSE AND m.id = $1` in the classification's seed — the March
        order resolves `internal` (the review's reproduction)."""
        from core.duckdb_store import silver_sales_type_case
        from core.sql_dialect import POSTGRES

        store, pool, env = stores
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.managers (id, name, is_retail) "
                               "VALUES (17, 'Inherited retail', TRUE)")
        env.setenv(ENV, "postgres")
        await store.set_manager_retail_status(17, False, date(2026, 10, 1), 1, "wholesale now")
        assert await _pg_intervals(pool, 17) == [
            (17, BASELINE, date(2026, 10, 1), True, None,
             "baseline frozen from managers.is_retail"),
            (17, date(2026, 10, 1), None, False, 1, "wholesale now")]
        case = silver_sales_type_case(POSTGRES)
        async with pool.acquire() as conn:
            for at, expected in (("2026-03-01 12:00+00", "retail"),
                                 ("2026-10-02 12:00+00", "internal")):
                got = await conn.fetchval(
                    f"SELECT {case} FROM (SELECT 1 AS source_id, 17 AS manager_id, "
                    f"TIMESTAMPTZ '{at}' AS ordered_at) o")
                assert got == expected, (at, got)

    @pytest.mark.asyncio
    async def test_set_at_is_the_callers_clock_never_postgres_now(self, stores):
        """Chain 7a's ONE CLOCK rule: `set_at` is the copy-back's handover
        clock. Mutation: stamp `now()` in the classification's INSERT (or in
        its seed) — the stored value is then today's, not the caller's."""
        store, pool, env = stores
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bronze.managers (id, name) VALUES (34, 'M')")
        env.setenv(ENV, "postgres")
        clock = datetime(2001, 2, 3, 4, 5, 6, tzinfo=timezone.utc)
        await pg_managers_write.set_manager_retail_status(
            34, True, date(2026, 6, 1), 7, "why", set_at=clock)
        async with pool.acquire() as conn:
            stamps = await conn.fetch(
                "SELECT valid_from, set_at FROM app.manager_classifications "
                "WHERE manager_id = 34 ORDER BY valid_from")
        assert [(r["valid_from"], r["set_at"]) for r in stamps] == [
            (BASELINE, clock), (date(2026, 6, 1), clock)]

    @pytest.mark.asyncio
    async def test_the_route_answers_422_for_a_refusal_postgres_never_saw(
            self, stores, monkeypatch):
        """A note carrying NUL is refused before Postgres is asked; the review
        reproduced a 503 "Postgres did not answer" for it. Nothing stored.
        Mutation: drop the route's `ClassificationRefused` mapping."""
        import time as _time

        import httpx

        from core.permissions import ADMIN_USER_IDS
        from web.main import app
        from web.routes.api._deps import limiter
        from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

        store, pool, env = stores
        env.setenv(ENV, "postgres")
        await store.upsert_managers([{"id": 34, "name": "M"}])
        before = await _pg_intervals(pool)

        async def get():
            return store
        monkeypatch.setattr("web.routes.api.admin.get_store", get)
        limiter.reset()
        admin = sorted(ADMIN_USER_IDS)[0]
        cookie = session_serializer.dumps(create_session_data(
            {"id": str(admin), "first_name": "T", "last_name": "U", "username": "t",
             "auth_date": str(int(_time.time()))}, role="admin"))
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url="http://test",
                                         cookies={SESSION_COOKIE: cookie}) as client:
                res = await client.post("/api/managers/34/retail-status?is_retail=true"
                                        "&effective_from=2026-10-07&note=a%00b")
        finally:
            limiter.reset()
        assert res.status_code == 422, res.text
        assert "note carries NUL" in res.json()["detail"]
        assert await _pg_intervals(pool) == before

    @pytest.mark.asyncio
    async def test_the_postgres_list_has_duckdbs_shape(self, stores):
        """`GET /api/managers` reads either store, so one answer must look like
        the other — `synced_at` included. Mutation: `mirrored_at AS
        mirrored_at` in `pg_managers_read`."""
        from core import pg_managers_read

        store, pool, env = stores
        with patch("core.pg_replication.replicate_managers", new=AsyncMock()):
            await store.upsert_managers([{"id": 34, "name": "M"}])
        duck = await store.get_all_managers()
        env.setenv(ENV, "postgres")
        await store.upsert_managers([{"id": 34, "name": "M"}])
        pg = await pg_managers_read.fetch_all_managers()
        assert [set(r) for r in pg] == [set(r) for r in duck]
        assert pg[0]["synced_at"] is not None

    @pytest.mark.asyncio
    async def test_the_stats_never_go_backwards_while_a_chain_owns_the_orders(
            self, stores, monkeypatch):
        """The ways back run chain 5's before chain 3's. With chain 5 back on
        DuckDB and the orders still a chain's, DuckDB's orders are frozen;
        recounted from them, Postgres went from 2026-10-06 and 500 orders to
        2026-09-01 and 1 (the review's reproduction). Mutation: drop the guard
        in `DuckDBStore.update_manager_stats`."""
        import types

        from core import write_chains
        from core.pg_replication import replicate_managers

        store, pool, _env = stores
        async with store.connection() as conn:
            conn.execute("INSERT INTO managers (id, name) VALUES (34, 'M')")
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
                "created_at, updated_at, manager_id) VALUES (?, 1, 1, 100, "
                "TIMESTAMPTZ '2026-09-01 10:00:00+00', TIMESTAMPTZ '2026-09-01 10:00:00+00', "
                "TIMESTAMPTZ '2026-09-01 10:00:00+00', 34)", [ORDER_IDS[0]])
            conn.execute("UPDATE managers SET last_order_date = DATE '2026-10-06', "
                         "order_count = 500 WHERE id = 34")
        assert (await replicate_managers(store))["ok"] is True
        orders_chain = types.ModuleType("core.pg_chain_orders_write")
        orders_chain.WRITE_ENV = "KS_WRITE_CHAIN_ORDERS"
        orders_chain.CHAIN_TABLES = ("bronze.orders", "bronze.order_products")
        orders_chain.env_writes_postgres = lambda: True
        monkeypatch.setattr(write_chains, "WRITE_CHAINS",
                            write_chains.WRITE_CHAINS + (orders_chain,))

        assert await store.update_manager_stats() == 0
        row = (await _pg_managers(pool))[34]
        assert (row["last_order_date"], row["order_count"]) == (date(2026, 10, 6), 500)
