"""The forecast vintages' capture (revision 0036), against a real PostgreSQL.

`pg_forecast_write.store_predictions` replaces a range of the stored forecast
with DELETE + INSERT. Before the DELETE it copies the rows it is about to
take into `app.revenue_prediction_vintages`, under a SAVEPOINT of its own
(`analytics_captures.capture_in_transaction`). What has to hold, each named by
the mutation it kills:

- the rows kept are the ones replaced, as they were shown: their values, the
  training that made them (`made_at` = their `created_at`), the moment they
  stopped being shown (`captured_at`); nothing outside the range or of another
  sales type, and the capture is stamped — zero rows included;
- a row stored with no `created_at` (revision 0025 allows it) is kept with a
  NULL `made_at`, and the forecast is written (the design's T8);
- a capture that fails on the server rolls back alone: the forecast is
  replaced and claimed as it would have been, the failure is stamped and the
  watch files it — and without the savepoint that failure would have aborted
  the forecast (the review's case, as a control);
- a replacement that fails takes its capture with it;
- the captures switched off: the forecast replaced, nothing kept, no stamp;
- two trainings at once each keep what the other stored.

Everything is written under sales types no other test uses and deleted again;
the owner rows the writer claims are deleted only if this test made them.
Skipped without `KS_PG_DSN`.
"""
from __future__ import annotations

import asyncio
import os
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import analytics_captures as cap
from core import chain_latch, pg_forecast_write as fw

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

TAG = "test-capture-vintages"          # VARCHAR(32), the sales type under test
OTHER = "test-capture-vintages-b"      # another sales type, same days
VINTAGES = fw.VINTAGES_TABLE
OWNER_KEYS = [chain_latch.owner_key(t) for t in fw.CHAIN_TABLES]
# Revision 2's NOT NULL `made_at`, put back for one test as a constraint that
# only new rows must meet.
REVIEWS_CASE = "test_capture_vintages_made_at_required"

M1 = {"mae": 1.0, "mape": 2.0, "wape": 3.0}
M2 = {"mae": 120.5, "mape": 31.2, "wape": 27.66}
MONDAY = datetime(2026, 11, 16, 0, 30, tzinfo=timezone.utc)       # 03:30 Kyiv
THURSDAY = datetime(2026, 11, 19, 0, 30, tzinfo=timezone.utc)


def _days(first: date, n: int, revenue) -> list:
    return [{"date": first + timedelta(days=i), "predicted_revenue": revenue(i)}
            for i in range(n)]


async def _clean(conn):
    await conn.execute(f"ALTER TABLE {VINTAGES} DROP CONSTRAINT IF EXISTS {REVIEWS_CASE}")
    for table in (VINTAGES, "app.revenue_predictions"):
        await conn.execute(
            f"DELETE FROM {table} WHERE sales_type = ANY($1::text[])", [TAG, OTHER])
    await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1",
                       cap.state_row(VINTAGES))


@pytest_asyncio.fixture
async def pool(monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.delenv(cap.SWITCH_ENV, raising=False)
    created = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    async with created.acquire() as conn:
        await _clean(conn)
        owned = {r["key"] for r in await conn.fetch(
            "SELECT key FROM meta.chain_watermarks WHERE key = ANY($1::text[])",
            OWNER_KEYS)}
    with patch("core.pg.get_pool", new=AsyncMock(return_value=created)):
        yield created
    async with created.acquire() as conn:
        await _clean(conn)
        # Left behind, an owner row reads as a chain owning a table with no
        # local marker — a CRITICAL this fixture would have made.
        await conn.execute(
            "DELETE FROM meta.chain_watermarks WHERE key = ANY($1::text[]) "
            "AND NOT key = ANY($2::text[])", OWNER_KEYS, list(owned))
    await created.close()


async def _seed(pool, rows):
    """Stored forecast rows as a training (or the hourly copy) left them."""
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO app.revenue_predictions (prediction_date, sales_type, "
            "predicted_revenue, model_mae, model_mape, model_wape, created_at) "
            "VALUES ($1, $2, $3, 1, 1, 27, $4)", rows)


async def _predictions(pool, sales_type=TAG):
    async with pool.acquire() as conn:
        return {r["prediction_date"]: (r["predicted_revenue"], r["created_at"])
                for r in await conn.fetch(
                    "SELECT prediction_date, predicted_revenue, created_at "
                    "FROM app.revenue_predictions WHERE sales_type = $1", sales_type)}


async def _vintages(pool):
    async with pool.acquire() as conn:
        return await conn.fetch(
            f"SELECT made_at, captured_at, prediction_date, sales_type, "
            f"predicted_revenue, model_mae, model_mape, model_wape FROM {VINTAGES} "
            f"WHERE sales_type = ANY($1::text[]) ORDER BY id", [TAG, OTHER])


async def _state(pool):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT last_ok_at, failures_since_ok, last_error, last_rows "
            "FROM meta.mirror_state WHERE table_name = $1", cap.state_row(VINTAGES))


async def _findings(pool):
    facts = await cap.read_facts(pool=pool)
    assert facts.unwatched is None and not facts.switched_off
    return {i.check_name: i for i in cap.judge(facts) if i.table_name == VINTAGES}


class TestTheReplacedRowsAreKept:
    @pytest.mark.asyncio
    async def test_as_they_were_shown_and_only_they(self, pool):
        """Kills: the copy after the DELETE (nothing kept) or of the new rows,
        a predicate wider or narrower than the DELETE's (the range's edges,
        another sales type), `made_at` from anything but `created_at`, and a
        run that copied nothing left unstamped."""
        await fw.store_predictions(
            _days(date(2026, 11, 16), 11, lambda i: 500.0 + i), TAG, M1, MONDAY)
        assert await _vintages(pool) == [], "the first training replaced nothing"
        state = await _state(pool)
        assert state["last_ok_at"] is not None and state["last_rows"] == 0
        await fw.store_predictions(
            _days(date(2026, 11, 19), 5, lambda i: 7.0), OTHER, M1, MONDAY)

        async with pool.acquire() as conn:
            before = await conn.fetchval("SELECT now()")
        await fw.store_predictions(
            _days(date(2026, 11, 19), 5, lambda i: 1000.0 + i), TAG, M2, THURSDAY)
        async with pool.acquire() as conn:
            after = await conn.fetchval("SELECT now()")

        kept = await _vintages(pool)
        assert [(r["prediction_date"], r["sales_type"], r["predicted_revenue"],
                 r["model_mae"], r["model_mape"], r["model_wape"], r["made_at"])
                for r in kept] == [
            (date(2026, 11, 19) + timedelta(days=i), TAG, Decimal(f"{503 + i}.00"),
             Decimal("1.00"), Decimal("2.00"), Decimal("3.00"), MONDAY)
            for i in range(5)]
        assert all(before <= r["captured_at"] <= after for r in kept)

        shown = await _predictions(pool)
        assert len(shown) == 11
        for i in range(3):        # before the range: Monday's, untouched
            assert shown[date(2026, 11, 16) + timedelta(days=i)] == (
                Decimal(f"{500 + i}.00"), MONDAY)
        for i in range(5):        # the range: Thursday's
            assert shown[date(2026, 11, 19) + timedelta(days=i)] == (
                Decimal(f"{1000 + i}.00"), THURSDAY)
        for i in range(5, 8):     # past its end: Monday's, untouched
            assert shown[date(2026, 11, 19) + timedelta(days=i)][1] == MONDAY

        state = await _state(pool)
        assert state["failures_since_ok"] == 0 and state["last_rows"] == 5
        assert await _findings(pool) == {}

    @pytest.mark.asyncio
    async def test_t8_a_row_never_stamped_is_kept_and_the_forecast_written(self, pool):
        """The design's T8 (iii): `created_at` NULL, as revision 0025 allows.
        Kills: a `made_at` declared NOT NULL again, or a capture that drops
        the rows it cannot date."""
        await _seed(pool, [(date(2026, 11, 20), TAG, 100000, None),
                           (date(2026, 11, 21), TAG, 110000, None)])

        assert await fw.store_predictions(
            _days(date(2026, 11, 20), 2, lambda i: 200000.0 + 10000 * i),
            TAG, M2, THURSDAY) == 2

        assert await _predictions(pool) == {
            date(2026, 11, 20): (Decimal("200000.00"), THURSDAY),
            date(2026, 11, 21): (Decimal("210000.00"), THURSDAY)}
        kept = await _vintages(pool)
        assert [(r["prediction_date"], r["predicted_revenue"], r["made_at"]) for r in kept] == [
            (date(2026, 11, 20), Decimal("100000.00"), None),
            (date(2026, 11, 21), Decimal("110000.00"), None)]
        assert all(r["captured_at"] is not None for r in kept)
        assert (await _state(pool))["last_rows"] == 2


class TestACaptureNeverFailsTheForecast:
    @pytest.mark.asyncio
    async def test_a_capture_the_server_refuses_rolls_back_alone(self, pool):
        """The review's case on the real table: a capture the server refuses
        inside the forecast's transaction. Kills: no savepoint (the forecast
        is lost with it — the control shows the failure is the server's and
        aborts a transaction), a raise into the writer, the claim rolled back
        with the capture, or a failure the watch does not see."""
        await _seed(pool, [(date(2026, 11, 20), TAG, 100000, None),
                           (date(2026, 11, 21), TAG, 110000, None)])
        async with pool.acquire() as conn:
            await conn.execute(
                f"ALTER TABLE {VINTAGES} ADD CONSTRAINT {REVIEWS_CASE} "
                f"CHECK (made_at IS NOT NULL) NOT VALID")
            # The control: the same statement with no savepoint around it.
            with pytest.raises(asyncpg.InFailedSQLTransactionError):
                async with conn.transaction():
                    with pytest.raises(asyncpg.CheckViolationError):
                        await conn.execute(fw._VINTAGES_CAPTURE_SQL, TAG,
                                           date(2026, 11, 20), date(2026, 11, 21))
                    await conn.execute("SELECT 1")

        assert await fw.store_predictions(
            _days(date(2026, 11, 20), 2, lambda i: 300000.0), TAG, M2, THURSDAY) == 2

        assert await _predictions(pool) == {
            date(2026, 11, 20): (Decimal("300000.00"), THURSDAY),
            date(2026, 11, 21): (Decimal("300000.00"), THURSDAY)}
        assert await _vintages(pool) == []
        async with pool.acquire() as conn:
            assert {r["key"] for r in await conn.fetch(
                "SELECT key FROM meta.chain_watermarks WHERE key = ANY($1::text[])",
                OWNER_KEYS)} == set(OWNER_KEYS), "the writer's transaction committed"
        state = await _state(pool)
        assert state["failures_since_ok"] == 1
        assert state["last_error"].startswith("CheckViolationError")
        found = await _findings(pool)
        assert set(found) == {cap.condition(cap.FAILED, VINTAGES)}
        assert found[cap.condition(cap.FAILED, VINTAGES)].severity.value == "WARN"

        # The next training's capture resumes, and clears the count: the gap
        # is the failed run's rows and nothing after them.
        async with pool.acquire() as conn:
            await conn.execute(f"ALTER TABLE {VINTAGES} DROP CONSTRAINT {REVIEWS_CASE}")
        await fw.store_predictions(
            _days(date(2026, 11, 20), 2, lambda i: 400000.0), TAG, M2,
            THURSDAY + timedelta(days=4))
        assert [(r["predicted_revenue"], r["made_at"]) for r in await _vintages(pool)] == [
            (Decimal("300000.00"), THURSDAY)] * 2
        assert (await _state(pool))["failures_since_ok"] == 0

    @pytest.mark.asyncio
    async def test_a_replacement_that_fails_takes_its_capture_with_it(self, pool):
        """The copy is the replacement's: it commits only with it. Kills: a
        capture that commits on its own — a failed training would then leave
        a vintage of a forecast still shown, and kept twice once it goes."""
        await _seed(pool, [(date(2026, 11, 20), TAG, 100000, MONDAY)])
        failing = _FailingPool(pool, "INSERT INTO app.revenue_predictions ")

        with patch.object(fw, "_pool", AsyncMock(return_value=failing)):
            with pytest.raises(_Injected):
                await fw.store_predictions(
                    _days(date(2026, 11, 20), 1, lambda i: 1.0), TAG, M2, THURSDAY)

        assert failing.hit
        assert await _predictions(pool) == {
            date(2026, 11, 20): (Decimal("100000.00"), MONDAY)}
        assert await _vintages(pool) == []
        assert await _state(pool) is None

    @pytest.mark.asyncio
    async def test_switched_off_the_forecast_is_replaced_and_nothing_kept(
            self, pool, monkeypatch):
        """Kills: a switch that leaves the capture running, stamps anyway, or
        stops the forecast with it."""
        monkeypatch.setenv(cap.SWITCH_ENV, "off")
        await _seed(pool, [(date(2026, 11, 20), TAG, 100000, MONDAY)])

        assert await fw.store_predictions(
            _days(date(2026, 11, 20), 1, lambda i: 2.0), TAG, M2, THURSDAY) == 1

        assert await _predictions(pool) == {
            date(2026, 11, 20): (Decimal("2.00"), THURSDAY)}
        assert await _vintages(pool) == []
        assert await _state(pool) is None


class TestTwoTrainingsAtOnce:
    @pytest.mark.asyncio
    async def test_each_keeps_what_the_other_stored(self, pool):
        """Under the chain's advisory lock the second training copies what
        the first stored. Kills: the copy taken before the lock — both would
        copy the seeded rows, and the first's would never be kept."""
        first = date(2026, 11, 20)
        for _ in range(3):
            async with pool.acquire() as conn:
                await _clean(conn)
            await _seed(pool, [(first + timedelta(days=i), TAG, 1, MONDAY)
                               for i in range(5)])
            await asyncio.gather(
                fw.store_predictions(_days(first, 5, lambda i: 2.0), TAG, M2, THURSDAY),
                fw.store_predictions(_days(first, 5, lambda i: 3.0), TAG, M2, THURSDAY))

            (shown,) = {v for v, _c in (await _predictions(pool)).values()}
            other = Decimal("5.00") - shown
            assert Counter(r["predicted_revenue"] for r in await _vintages(pool)) == {
                Decimal("1.00"): 5, other: 5}


class _Injected(Exception):
    pass


class _FailingConn:
    """The writer's connection, raising on the first statement carrying
    `needle` — inside the writer's own transaction, after its capture."""

    def __init__(self, conn, needle, pool):
        self._conn, self._needle, self._pool = conn, needle, pool

    def _check(self, sql):
        if self._needle in sql:
            self._pool.hit = True
            raise _Injected(self._needle)

    async def execute(self, sql, *a, **k):
        self._check(sql)
        return await self._conn.execute(sql, *a, **k)

    async def executemany(self, sql, *a, **k):
        self._check(sql)
        return await self._conn.executemany(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _FailingPool:
    def __init__(self, pool, needle):
        self._pool, self._needle, self.hit = pool, needle, False

    def acquire(self):
        outer = self

        class _Ctx:
            async def __aenter__(self):
                self._ctx = outer._pool.acquire()
                conn = await self._ctx.__aenter__()
                return _FailingConn(conn, outer._needle, outer)

            async def __aexit__(self, *exc):
                return await self._ctx.__aexit__(*exc)
        return _Ctx()
