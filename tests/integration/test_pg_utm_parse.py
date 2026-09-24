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
import os
import threading
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
        parse ran, so the ~90-minute canary limit does not page every night."""
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
    async def test_a_write_that_fails_after_the_truncate_leaves_the_table(self, conn):
        """TRUNCATE, INSERT and the watermark are one transaction. A failure
        after the TRUNCATE — here, once every row is already inserted — must
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


async def _committed_rows(pool):
    async with pool.acquire() as c:
        return await rows(c, f"order_id = ANY('{{{','.join(map(str, IDS))}}}'::int[])")


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
