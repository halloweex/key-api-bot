"""The UTM parse inside Postgres, against a real Postgres (DN-18, step 9b).

`core/pg_utm_parse.py` parses `silver.order_utm` out of `bronze.orders` with
the parser DuckDB uses. Every claim it makes is about what a server does to
real rows — which orders a predicate selects, what a transaction leaves behind
when it fails, which of two sessions waits — so it is proved on one.

The last class is the one the module exists for: DuckDB's
`refresh_utm_silver_layer` and this parse, handed the same orders — every
comment in the golden fixtures — must store the same thing, every column,
`parsed_at` included, at zero tolerance. Then again after the edits the sync
makes, and once more for the full parse against DuckDB's reclassify.

HOW A SCENARIO IS ISOLATED
Most scenarios need the whole of `bronze.orders` and `silver.order_utm` to
themselves, so they run inside one transaction that empties both and is rolled
back; the parse's own transaction becomes a savepoint inside it, through a
one-connection pool. The serialisation scenarios cannot — a second session must
see what the first committed — so they commit rows under ids of their own and
remove them afterwards.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` and CI supply one.
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import mirror_reconciliation as mr
from core import pg_utm_parse as parse
from core.duckdb_store import DuckDBStore
from core.pg_order_utm import UTM_COLUMNS, UTM_TABLE
from tests.unit.test_utm_classify_golden import PARSE_GOLDEN

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

# Microseconds on purpose: a driver or a cast that dropped them would pass a
# comparison of whole seconds.
UPDATED = datetime(2026, 9, 7, 9, 0, 0, 123457, tzinfo=timezone.utc)
COMMENT = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: spring"
EDITED = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: autumn"
assert len(COMMENT) == len(EDITED)


class _OneConnectionPool:
    """`pool.acquire()` that hands out the scenario's connection, so everything
    the parse and the failure recorder write lands inside the transaction that
    is rolled back."""

    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self, **_kw):
        yield self._conn


@pytest.fixture(autouse=True)
def fresh_layer_lock():
    """A lock of this test's own. `asyncio.Lock` binds to the first loop that
    ever contends on it, and every test here runs on a loop of its own."""
    with patch("core.pg_silver.PG_LAYER_LOCK", asyncio.Lock()):
        yield


@pytest_asyncio.fixture
async def conn():
    c = await asyncpg.connect(DSN)
    tx = c.transaction()
    await tx.start()
    try:
        # Bounded: emptying a table waits on any lock another session holds,
        # and a test must not hang on somebody else's session.
        await c.execute("SET LOCAL lock_timeout = '5s'")
        await c.execute("SET LOCAL statement_timeout = '60s'")
        await c.execute(f"DELETE FROM {UTM_TABLE}")
        await c.execute("DELETE FROM bronze.orders")
        await c.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", UTM_TABLE)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=_OneConnectionPool(c))), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield c
    finally:
        await tx.rollback()
        await c.close()


async def order(conn, oid, *, comment=COMMENT, updated_at=UPDATED):
    await conn.execute(
        """
        INSERT INTO bronze.orders
               (id, source_id, status_id, grand_total, updated_at, manager_comment)
        VALUES ($1, 4, 1, 100, $2, $3)
        ON CONFLICT (id) DO UPDATE SET updated_at = EXCLUDED.updated_at,
                                       manager_comment = EXCLUDED.manager_comment
        """, oid, updated_at, comment)


async def rows(conn, where="TRUE"):
    records = await conn.fetch(
        f"SELECT {', '.join(UTM_COLUMNS)} FROM {UTM_TABLE} WHERE {where} ORDER BY order_id")
    return {r["order_id"]: tuple(r) for r in records}


async def watermark(conn):
    return await conn.fetchrow(
        "SELECT last_ok_at, failures_since_ok, last_error, last_rows "
        "FROM meta.mirror_state WHERE table_name = $1", UTM_TABLE)


def raising(*_a, **_kw):
    raise RuntimeError("bad comment")


# ── the incremental parse ─────────────────────────────────────────────────────

class TestTheIncrementalParse:
    @pytest.mark.asyncio
    async def test_a_new_order_with_a_utm_comment_gets_a_row(self, conn):
        await order(conn, 101)

        result = await parse.parse_incremental()

        assert result["parsed"] == 1 and result["rows"] == 1
        stored = (await rows(conn))[101]
        assert stored == (101, "fbads", "cpc", "spring", None, None, None,
                          None, None, None, None, "paid_confirmed", "facebook",
                          UPDATED)

    @pytest.mark.asyncio
    async def test_an_equal_length_campaign_edit_with_updated_at_bumped_is_reparsed(self, conn):
        """The case the orders fingerprint cannot see — a text edit of the same
        length — and the one `updated_at` exists to carry."""
        await order(conn, 102)
        await parse.parse_incremental()

        bumped = UPDATED + timedelta(minutes=5)
        await order(conn, 102, comment=EDITED, updated_at=bumped)
        result = await parse.parse_incremental()

        assert result["parsed"] == 1
        stored = (await rows(conn))[102]
        assert stored[UTM_COLUMNS.index("utm_campaign")] == "autumn"
        assert stored[UTM_COLUMNS.index("parsed_at")] == bumped

    @pytest.mark.asyncio
    async def test_a_reparse_clears_a_field_the_comment_no_longer_has(self, conn):
        """Every column is replaced, as DuckDB's INSERT OR REPLACE replaces it:
        an upsert that set only the non-null fields would keep a campaign the
        comment has dropped."""
        await order(conn, 103)
        await parse.parse_incremental()

        await order(conn, 103, comment="UTM: utm_source: ig; utm_medium: social",
                    updated_at=UPDATED + timedelta(minutes=5))
        await parse.parse_incremental()

        stored = (await rows(conn))[103]
        assert stored[UTM_COLUMNS.index("utm_campaign")] is None
        assert stored[UTM_COLUMNS.index("platform")] == "instagram"

    @pytest.mark.asyncio
    async def test_an_order_with_no_comment_is_never_given_a_row(self, conn):
        await order(conn, 104, comment=None)
        await order(conn, 105, comment="")

        assert (await parse.parse_incremental())["parsed"] == 0
        assert await rows(conn) == {}

    @pytest.mark.asyncio
    async def test_a_comment_with_no_tracking_is_a_row_of_nulls(self, conn):
        """The NULLs are what let the readers' COALESCE fall back to the
        source; `('unknown', 'unattributed')` here would overrule it."""
        await order(conn, 106, comment="Передзвонити після 18:00")
        await parse.parse_incremental()

        assert (await rows(conn))[106] == (106, *(None,) * 12, UPDATED)

    @pytest.mark.asyncio
    async def test_zero_rows_still_stamp_the_watermark(self, conn):
        """A quiet night is not a dead parser. The watermark measures that the
        parse ran, so the ~90-minute canary limit does not page every night.

        From nothing first: inside this test's one transaction `now()` does
        not move, so a stamp that merely survived from an earlier run would
        look exactly like a new one. A first run with nothing to parse has no
        earlier stamp to hide behind."""
        assert await watermark(conn) is None
        assert (await parse.parse_incremental())["parsed"] == 0
        empty = await watermark(conn)
        assert empty is not None, "a zero-row run left no watermark"
        assert empty["last_ok_at"] is not None and empty["failures_since_ok"] == 0
        assert empty["last_rows"] == 0

        await order(conn, 107)
        await parse.parse_incremental()
        first = await watermark(conn)

        assert (await parse.parse_incremental())["parsed"] == 0
        second = await watermark(conn)
        assert second["failures_since_ok"] == 0
        assert second["last_ok_at"] is not None
        # `last_rows` is the table, not the delta: a quiet run must not read as
        # an empty table.
        assert first["last_rows"] == second["last_rows"] == 1

    @pytest.mark.asyncio
    async def test_a_zero_row_success_clears_an_earlier_failure(self, conn):
        await order(conn, 108)
        with patch("core.pg_utm_parse.utm_columns", side_effect=raising):
            with pytest.raises(RuntimeError):
                await parse.parse_incremental()
        assert (await watermark(conn))["failures_since_ok"] == 1

        await conn.execute("DELETE FROM bronze.orders WHERE id = 108")
        await parse.parse_incremental()
        wm = await watermark(conn)
        assert wm["failures_since_ok"] == 0 and wm["last_error"] is None

    @pytest.mark.asyncio
    async def test_a_raising_parser_is_recorded_and_the_rows_stay(self, conn):
        await order(conn, 109)
        await parse.parse_incremental()
        before = await rows(conn)
        ok_at = (await watermark(conn))["last_ok_at"]

        await order(conn, 109, comment=EDITED, updated_at=UPDATED + timedelta(minutes=5))
        await order(conn, 110)
        with patch("core.pg_utm_parse.utm_columns", side_effect=raising):
            with pytest.raises(RuntimeError, match="bad comment"):
                await parse.parse_incremental()

        assert await rows(conn) == before
        wm = await watermark(conn)
        assert wm["failures_since_ok"] == 1
        assert "bad comment" in wm["last_error"]
        assert wm["last_ok_at"] == ok_at

    @pytest.mark.asyncio
    async def test_no_updated_at_is_stamped_with_the_transaction_clock(self, conn):
        """DuckDB's `COALESCE(?, CURRENT_TIMESTAMP)`: the order gets its verdict,
        and `NULL > parsed_at` never selects it again."""
        await order(conn, 111, updated_at=None)
        await parse.parse_incremental()

        stamped = (await rows(conn))[111][UTM_COLUMNS.index("parsed_at")]
        assert stamped == await conn.fetchval("SELECT now()")
        assert (await parse.parse_incremental())["parsed"] == 0

    @pytest.mark.asyncio
    async def test_the_completeness_check_owes_nothing_after_it(self, conn):
        """DN-16's check is this parse's predicate read the other way round, so
        `parsed_at = updated_at` must satisfy it exactly — through asyncpg,
        with no offset between the two stamps."""
        long_ago = datetime.now(timezone.utc) - timedelta(days=1)
        await order(conn, 112)
        await order(conn, 113, comment="Передзвонити після 18:00")
        await conn.execute("UPDATE bronze.orders SET mirrored_at = $1", long_ago)
        await parse.parse_incremental()

        row = await mr.order_utm_completeness_row(
            conn, now=datetime.now(timezone.utc),
            grace_minutes=mr.SILVER_GRACE_MINUTES, max_samples=10)
        assert mr.order_utm_completeness_findings(
            row, grace_minutes=mr.SILVER_GRACE_MINUTES) == []


# ── the full parse ────────────────────────────────────────────────────────────

async def _ten_parsed(conn):
    for oid in range(201, 211):
        await order(conn, oid)
    result = await parse.parse_full()
    assert result["rows"] == 10, result
    return await rows(conn)


class TestTheFullParse:
    @pytest.mark.asyncio
    async def test_it_reparses_what_the_incremental_never_would(self, conn):
        """A stale verdict whose `parsed_at` equals `updated_at` — a rule change
        the incremental cannot see — is rewritten, and a row whose order has no
        comment any more is gone: the DuckDB reclassify's result."""
        await _ten_parsed(conn)
        await conn.execute(
            f"UPDATE {UTM_TABLE} SET traffic_type = 'unknown', platform = 'other' "
            "WHERE order_id = 201")
        await conn.execute("UPDATE bronze.orders SET manager_comment = NULL WHERE id = 210")
        assert (await parse.parse_incremental())["parsed"] == 0

        result = await parse.parse_full()

        # 9 of 10 is exactly the floor, and the floor admits it.
        assert result == {**result, "rows": 9, "replaced": 10}
        stored = await rows(conn)
        assert set(stored) == set(range(201, 210))
        assert stored[201][-3:-1] == ("paid_confirmed", "facebook")

    @pytest.mark.asyncio
    async def test_a_full_parse_over_a_truncated_bronze_is_refused_and_the_table_is_intact(self, conn):
        before = await _ten_parsed(conn)
        ok_at = (await watermark(conn))["last_ok_at"]
        await conn.execute("DELETE FROM bronze.orders WHERE id > 201")

        result = await parse.parse_full()

        assert result["parsed_rows"] == 1 and result["current_rows"] == 10
        assert result["refused"].startswith("refused:")
        assert await rows(conn) == before
        wm = await watermark(conn)
        assert wm["failures_since_ok"] == 1
        assert "yields 1 rows" in wm["last_error"] and "10" in wm["last_error"]
        assert wm["last_ok_at"] == ok_at and wm["last_rows"] == 10

    @pytest.mark.asyncio
    async def test_a_refusal_is_kept_by_the_log_not_by_the_watermark(self, conn, caplog):
        """The watermark row is the incremental's liveness stamp, so the next
        successful incremental — the next derivation under DN-19, up to the
        60-minute heartbeat away — clears a refusal written there. What keeps it is the return value and the
        ERROR log. This pins both halves of `parse_full`'s account of where a
        refusal lives: if the watermark starts keeping it, the docstring
        changes with the test."""
        await _ten_parsed(conn)
        await conn.execute("DELETE FROM bronze.orders WHERE id > 201")

        with caplog.at_level(logging.ERROR, logger=parse.__name__):
            result = await parse.parse_full()

        assert result["refused"].startswith("refused:")
        logged = [r.getMessage() for r in caplog.records
                  if r.name == parse.__name__ and r.levelno == logging.ERROR]
        assert any(result["refused"] in m for m in logged), logged
        assert (await watermark(conn))["failures_since_ok"] == 1

        assert (await parse.parse_incremental())["parsed"] == 0
        wm = await watermark(conn)
        assert wm["failures_since_ok"] == 0 and wm["last_error"] is None

    @pytest.mark.asyncio
    async def test_force_replaces_a_shrink_and_says_so(self, conn):
        await _ten_parsed(conn)
        await conn.execute("DELETE FROM bronze.orders WHERE id > 201")

        result = await parse.parse_full(force=True)

        assert result["rows"] == 1 and result["forced"] is True
        assert set(await rows(conn)) == {201}
        assert (await watermark(conn))["failures_since_ok"] == 0

    @pytest.mark.asyncio
    async def test_a_raising_parser_is_recorded_and_the_rows_stay(self, conn):
        before = await _ten_parsed(conn)
        with patch("core.pg_utm_parse.utm_columns", side_effect=raising):
            with pytest.raises(RuntimeError, match="bad comment"):
                await parse.parse_full()

        assert await rows(conn) == before
        wm = await watermark(conn)
        assert wm["failures_since_ok"] == 1 and "bad comment" in wm["last_error"]

    @pytest.mark.asyncio
    async def test_a_write_that_fails_after_the_delete_leaves_the_table(self, conn):
        """DELETE, INSERT and the watermark are one transaction. A failure
        after the DELETE — here, once every row is already inserted — must
        leave the verdicts a reader had, not an empty table."""
        before = await _ten_parsed(conn)
        real = parse._write_chunked

        async def then_fail(c, sql, batch):
            await real(c, sql, batch)
            raise RuntimeError("disk full")

        await conn.execute(f"UPDATE {UTM_TABLE} SET utm_term = 'kept' WHERE order_id = 205")
        before[205] = (await rows(conn))[205]
        with patch("core.pg_utm_parse._write_chunked", new=then_fail):
            with pytest.raises(RuntimeError, match="disk full"):
                await parse.parse_full()

        assert await rows(conn) == before
        assert (await watermark(conn))["failures_since_ok"] == 1

    @pytest.mark.asyncio
    async def test_it_waits_for_the_layer_lock_and_gives_up_bounded(self, conn):
        """In-process, a full parse is one of several actors over the Postgres
        layer. Three of its future callers are HTTP handlers, so the wait is
        bounded, and a parse that did not run says so in the watermark."""
        from core.pg_silver import PG_LAYER_LOCK

        before = await _ten_parsed(conn)
        await conn.execute("UPDATE bronze.orders SET manager_comment = $1", EDITED)
        with patch("core.pg_utm_parse.LOCK_WAIT_S", 0.05):
            async with PG_LAYER_LOCK:                  # held by somebody else
                with pytest.raises(TimeoutError):
                    await parse.parse_full()

        assert await rows(conn) == before
        wm = await watermark(conn)
        assert wm["failures_since_ok"] == 1 and "PG_LAYER_LOCK" in wm["last_error"]
        assert not PG_LAYER_LOCK.locked()


# ── serialisation, across sessions ────────────────────────────────────────────

IDS = tuple(range(972001, 972006))


@pytest_asyncio.fixture
async def committed():
    """Committed rows under ids of this module's own, on a real pool: a second
    session must see what the first committed. The table is emptied whole —
    the full parse replaces it whole — and the ids removed afterwards."""
    pool = await asyncpg.create_pool(DSN, min_size=2, max_size=6)

    async def clean():
        async with pool.acquire() as c:
            await c.execute(f"DELETE FROM {UTM_TABLE}")
            await c.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", list(IDS))
            await c.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", UTM_TABLE)

    await clean()
    try:
        async with pool.acquire() as c:
            for oid in IDS[:-1]:
                await order(c, oid)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
             patch("core.pg.require_revision", new=AsyncMock()):
            yield pool
    finally:
        await clean()
        await pool.close()


async def _held_by_another_session(pool):
    """A session-level hold on the parse's key, as a `docker exec` CLI's parse
    in another process would hold it."""
    other = await asyncpg.connect(DSN)
    await other.execute("SELECT pg_advisory_lock($1)", parse.ADVISORY_LOCK_KEY)
    return other


_OURS = f"order_id = ANY('{{{','.join(map(str, IDS))}}}'::int[])"


async def _committed_rows(pool):
    async with pool.acquire() as c:
        return await rows(c, _OURS)


class TestTwoParsesSerialise:
    @pytest.mark.parametrize("shape", ["full", "incremental"])
    @pytest.mark.asyncio
    async def test_a_parse_waits_for_another_process_and_reads_what_it_left(
            self, committed, shape):
        """Reading after the lock is the point: the order committed while this
        parse waited is in what it writes."""
        pool = committed
        run = parse.parse_full if shape == "full" else parse.parse_incremental
        other = await _held_by_another_session(pool)
        try:
            task = asyncio.create_task(run())
            await asyncio.sleep(0.5)
            assert not task.done(), "the parse did not wait for the advisory lock"
            assert await _committed_rows(pool) == {}

            async with pool.acquire() as c:
                await order(c, IDS[-1])
            await other.execute("SELECT pg_advisory_unlock($1)", parse.ADVISORY_LOCK_KEY)
            await asyncio.wait_for(task, timeout=30)
        finally:
            await other.close()

        assert set(await _committed_rows(pool)) == set(IDS)

    @pytest.mark.asyncio
    async def test_a_full_parse_that_waited_counts_the_table_it_replaces(self, committed):
        """The shrink guard compares against the table this parse replaces, so
        the count comes after the key. Counted before the wait, it would judge a
        table another process has since replaced: here that refuses a
        replacement the table as it stands admits. (TRUNCATE's ACCESS
        EXCLUSIVE used to catch this by accident, and DELETE takes none.)"""
        pool = committed
        await parse.parse_full()
        other = await _held_by_another_session(pool)
        try:
            task = asyncio.create_task(parse.parse_full())
            await asyncio.sleep(0.5)
            assert not task.done(), "the parse did not wait for the advisory lock"

            # What another process's forced full parse leaves behind.
            gone = list(IDS[1:-1])
            await other.execute("DELETE FROM bronze.orders WHERE id = ANY($1::int[])", gone)
            await other.execute(f"DELETE FROM {UTM_TABLE} WHERE order_id = ANY($1::int[])", gone)
            standing = await other.fetchval(parse.COUNT_SQL)
            await other.execute("SELECT pg_advisory_unlock($1)", parse.ADVISORY_LOCK_KEY)
            result = await asyncio.wait_for(task, timeout=30)
        finally:
            await other.close()

        assert "refused" not in result, result
        assert result["replaced"] == standing

    @pytest.mark.asyncio
    async def test_two_concurrent_full_parses_serialise(self, committed):
        """One through `parse_full`, one as another process runs it — with a
        `PG_LAYER_LOCK` of its own, which is what `_parse_full_locked` is from
        here. Only the advisory lock stands between them, and the parse inside
        it is slowed so an overlap cannot be missed."""
        pool = committed
        active, peak = 0, 0
        guard = threading.Lock()
        real = parse.parse_rows

        def slow(records):
            nonlocal active, peak
            with guard:
                active += 1
                peak = max(peak, active)
            try:
                threading.Event().wait(0.4)
                return real(records)
            finally:
                with guard:
                    active -= 1

        with patch("core.pg_utm_parse.parse_rows", new=slow):
            first, second = await asyncio.wait_for(asyncio.gather(
                parse.parse_full(), parse._parse_full_locked(None, force=False),
            ), timeout=60)

        assert peak == 1, "two full parses ran at once"
        assert first["rows"] == second["rows"] >= len(IDS) - 1
        assert set(await _committed_rows(pool)) == set(IDS[:-1])
        async with pool.acquire() as c:
            assert (await watermark(c))["failures_since_ok"] == 0


class TestLockWaitsAreBounded:
    @pytest.mark.parametrize("shape", ["full", "incremental"])
    @pytest.mark.asyncio
    async def test_a_key_held_elsewhere_fails_the_parse_within_the_bound(
            self, committed, shape):
        """Under DN-19 the incremental waits while the derivation holds
        `PG_LAYER_LOCK`, and the sync waits behind that. A stuck CLI or an idle
        psql holding the key must cost one failed parse, not the derivation.

        Half a second on purpose: a bound rendered in whole seconds becomes
        `'0s'`, which Postgres reads as no timeout, and the parse would wait
        for as long as the other session cared to hold the key."""
        pool = committed
        run = parse.parse_full if shape == "full" else parse.parse_incremental
        other = await _held_by_another_session(pool)
        try:
            started = time.monotonic()
            with patch("core.pg_utm_parse.PG_LOCK_WAIT_S", 0.5):
                with pytest.raises(asyncpg.exceptions.LockNotAvailableError):
                    await asyncio.wait_for(run(), timeout=15)
            waited = time.monotonic() - started
        finally:
            await other.close()

        assert 0.4 <= waited < 10, waited
        assert await _committed_rows(pool) == {}
        async with pool.acquire() as c:
            wm = await watermark(c)
        assert wm["failures_since_ok"] == 1
        assert "LockNotAvailableError" in wm["last_error"]


    @pytest.mark.asyncio
    async def test_on_the_production_pool_shape_the_lock_names_itself(self, committed):
        """Production's pool carries `command_timeout` (`KS_PG_TIMEOUT`). With
        the in-Postgres bound below it, Postgres ends the wait and the
        watermark names the lock; above it, asyncpg would cancel first and
        record a bare `TimeoutError`."""
        other = await _held_by_another_session(committed)
        prod_shaped = await asyncpg.create_pool(DSN, min_size=1, max_size=2, command_timeout=2)
        try:
            with patch("core.pg_utm_parse.PG_LOCK_WAIT_S", 0.5):
                with pytest.raises(asyncpg.exceptions.LockNotAvailableError):
                    await asyncio.wait_for(parse.parse_incremental(prod_shaped), timeout=15)
        finally:
            await prod_shaped.close()
            await other.close()
        async with committed.acquire() as c:
            wm = await watermark(c)
        assert "LockNotAvailableError" in wm["last_error"], wm["last_error"]


class TestTheIncrementalIsOneTransaction:
    @pytest.mark.asyncio
    async def test_a_failure_between_chunks_leaves_nothing_behind(self, committed):
        """The advisory lock, the read, every upsert chunk and the OK stamp
        commit together or not at all. The `conn` fixture cannot show it —
        there the parse's transaction is a savepoint inside the scenario's —
        so this runs on the committed pool and looks from a second session."""
        pool = committed
        await parse.parse_incremental()
        before = await _committed_rows(pool)
        async with pool.acquire() as c:
            ok_at = (await watermark(c))["last_ok_at"]
            await order(c, IDS[0], comment=EDITED, updated_at=UPDATED + timedelta(minutes=5))
            await order(c, IDS[-1])
        real = parse._write_chunked

        async def first_chunk_then_fail(c, sql, batch):
            assert len(batch) > parse.CHUNK, "the batch must span several chunks"
            await real(c, sql, batch[:parse.CHUNK])
            raise ConnectionResetError("dropped between chunks")

        with patch("core.pg_utm_parse.CHUNK", 1), \
             patch("core.pg_utm_parse._write_chunked", new=first_chunk_then_fail):
            with pytest.raises(ConnectionResetError):
                await parse.parse_incremental()

        assert await _committed_rows(pool) == before
        async with pool.acquire() as c:
            wm = await watermark(c)
        assert wm["failures_since_ok"] == 1 and wm["last_ok_at"] == ok_at


class TestReadersDuringAFullParse:
    """A full parse run from a CLI holds a `PG_LAYER_LOCK` of its own, so
    nothing in this process orders it against a reader. Here no reader takes
    `PG_LAYER_LOCK` at all, which is that case."""

    @pytest.mark.asyncio
    async def test_a_snapshot_taken_before_it_still_reads_the_old_verdicts(self, committed):
        """`core/pg_warehouse_dq.py` reads attribution out of one REPEATABLE
        READ snapshot. A TRUNCATE committed after that snapshot was taken
        would read as an empty table — 0 % website attribution, filed against
        a table that was only being replaced."""
        pool = committed
        await parse.parse_full()
        before = await _committed_rows(pool)
        reader = await asyncpg.connect(DSN)
        try:
            tx = reader.transaction(isolation="repeatable_read", readonly=True)
            await tx.start()
            await reader.fetchval("SELECT count(*) FROM bronze.orders")   # the snapshot
            async with pool.acquire() as c:
                await c.execute(
                    "UPDATE bronze.orders SET manager_comment = $1 WHERE id = ANY($2::int[])",
                    EDITED, list(IDS))

            result = await asyncio.wait_for(parse.parse_full(), timeout=30)
            assert "refused" not in result, result

            assert await rows(reader, _OURS) == before
            await tx.rollback()
        finally:
            await reader.close()

        after = await _committed_rows(pool)
        assert set(after) == set(before)
        assert {r[UTM_COLUMNS.index("utm_campaign")] for r in after.values()} == {"autumn"}

    @pytest.mark.asyncio
    async def test_an_open_read_of_the_table_does_not_hold_it_up(self, committed):
        """A `/traffic` read holds ACCESS SHARE until its transaction ends. A
        TRUNCATE would wait for it, and every read arriving after would queue
        behind the TRUNCATE, for up to `LOCK_WAIT_S`."""
        pool = committed
        await parse.parse_full()
        reader = await asyncpg.connect(DSN)
        try:
            tx = reader.transaction()
            await tx.start()
            await reader.fetchval(f"SELECT count(*) FROM {UTM_TABLE}")

            with patch("core.pg_utm_parse.LOCK_WAIT_S", 1):
                result = await asyncio.wait_for(parse.parse_full(), timeout=15)

            assert "refused" not in result, result
            await tx.rollback()
        finally:
            await reader.close()


# ── the two engines agree ─────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def duck(tmp_path):
    store = DuckDBStore(db_path=tmp_path / "pg_utm_parse.duckdb")
    await store.connect()
    try:
        yield store
    finally:
        await store.close()


class _Both:
    """One order header written to both stores, as the sync writes it: the same
    aware datetime handed to each driver."""

    def __init__(self, store, conn):
        self.store, self.conn = store, conn

    async def order(self, oid, comment, updated_at):
        async with self.store.connection() as dk:
            dk.execute(
                "INSERT OR REPLACE INTO orders (id, source_id, status_id, grand_total, "
                "ordered_at, updated_at, buyer_id, manager_id, manager_comment) "
                "VALUES (?, 4, 1, 100, ?, ?, 10, NULL, ?)",
                [oid, updated_at, updated_at, comment],
            )
        await order(self.conn, oid, comment=comment, updated_at=updated_at)

    async def duckdb_rows(self):
        async with self.store.connection() as dk:
            got = dk.execute(
                f"SELECT {', '.join(UTM_COLUMNS)} FROM silver_order_utm ORDER BY order_id"
            ).fetchall()
        return {r[0]: _utc(r) for r in got}

    async def postgres_rows(self):
        return {k: _utc(v) for k, v in (await rows(self.conn)).items()}


def _utc(row):
    """Values, not renderings: both drivers hand back an aware datetime, in
    whatever zone the session names, and equal instants compare equal only
    once they are in one zone."""
    *head, parsed_at = row
    return (*head, parsed_at.astimezone(timezone.utc))


class TestTheTwoEnginesAgree:
    """Zero tolerance, every column. A disagreement anywhere is a second
    classifier, which is the one thing step 9 must not build."""

    @pytest.mark.asyncio
    async def test_over_the_golden_fixtures_and_the_sync_edits_after_them(self, duck, conn):
        both = _Both(duck, conn)
        stamps = {}
        for oid, (comment, _parsed, _row) in enumerate(PARSE_GOLDEN, start=1):
            stamps[oid] = UPDATED + timedelta(minutes=oid, microseconds=oid)
            await both.order(oid, comment, stamps[oid])
        no_comment = len(PARSE_GOLDEN) + 1
        await both.order(no_comment, None, UPDATED)

        # Round 1: everything is new.
        duck_ids = await duck.refresh_utm_silver_layer()
        pg = await parse.parse_incremental()
        expected = {oid for oid, (c, _, _) in enumerate(PARSE_GOLDEN, start=1) if c}
        assert duck_ids == expected and pg["parsed"] == len(expected)
        assert await both.postgres_rows() == await both.duckdb_rows()
        assert len(await both.postgres_rows()) == len(expected)

        # Round 2: what the sync does to orders afterwards, applied to both.
        by_comment = {c: oid for oid, (c, _, _) in enumerate(PARSE_GOLDEN, start=1)}
        later = UPDATED + timedelta(days=1, microseconds=7)
        summer = by_comment["UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: summer"]
        await both.order(summer, "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: winter",
                         later)                                     # equal length, bumped
        pixel = by_comment["_fbp: fb.1.1700000000.42"]
        await both.order(pixel, "_fbp: fb.1.1700000000.43", stamps[pixel])   # not bumped
        klaviyo = by_comment["UTM: utm_source: klaviyo; utm_medium: email; utm_campaign: welcome_flow"]
        await both.order(klaviyo, "", later)                        # emptied, bumped
        ig = by_comment["UTM: utm_id: 12345; utm_source: ig; utm_medium: social; foo: bar"]
        await both.order(ig, PARSE_GOLDEN[ig - 1][0], later)        # same text, bumped
        await both.order(no_comment + 1, "ttp: 2abcDEF; нове замовлення", later)  # new

        duck_ids = await duck.refresh_utm_silver_layer()
        pg = await parse.parse_incremental()
        assert duck_ids == {summer, ig, no_comment + 1}
        assert pg["parsed"] == len(duck_ids)
        after = await both.postgres_rows()
        assert after == await both.duckdb_rows()
        assert after[summer][UTM_COLUMNS.index("utm_campaign")] == "winter"
        assert after[pixel][UTM_COLUMNS.index("fbp")] == "fb.1.1700000000.42"
        assert klaviyo in after

        # Round 3: the full parse against DuckDB's reclassify — DELETE, then
        # the parse, which then selects every commented order.
        async with duck.connection() as dk:
            dk.execute("DELETE FROM silver_order_utm")
        await duck.refresh_utm_silver_layer()
        pg = await parse.parse_full()
        assert "refused" not in pg, pg
        full = await both.postgres_rows()
        assert full == await both.duckdb_rows()
        assert full[pixel][UTM_COLUMNS.index("fbp")] == "fb.1.1700000000.43"
        assert klaviyo not in full
