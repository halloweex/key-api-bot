"""Revision 0036's goal capture, through chain 7a's writer, on a real PostgreSQL.

`app.revenue_goals` is one row per period, rewritten in place, so the goal in
force last month is gone the moment somebody types this month's. Every write
now appends the row it stored to `app.revenue_goal_history`, inside the
goal's own transaction under a SAVEPOINT of its own
(`core/pg_goals_write.py`, "AND EVERY GOAL IS KEPT"). What is pinned, each
named by the mutation it kills:

- each kind lands as what it was — a typed goal `set`, a reset
  `reset_to_auto`, a goal stored with `is_custom=False` `recalculated` — and
  every write is a version, the same number typed twice included;
- the version is the stored row read back: Postgres' rounding, and
  `updated_at` as `set_at`, so a period's latest version is the live row;
- the version commits with its goal and rolls back with it, ok stamp
  included;
- a capture that fails rolls back alone: the goal is written, the latch's
  owner row claimed, earlier versions untouched, the failure stamped and
  filed by the watch as a WARN — and the next good write clears it;
- `KS_ANALYTICS_CAPTURES=off` writes the goal and nothing else;
- the goal in force is kept before a write overwrites it when the history
  does not end with it — the goal stored before revision 0036, one written
  while the captures were off, one whose version failed — and only then;
- a capture blocked on a lock holds the goal's transaction for its bound,
  not for the pool's `command_timeout`.

Everything that can go through the repository does — `GoalsMixin.set_goal`
and `reset_goal_to_auto` are what `POST` and `DELETE /api/goals` call.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import analytics_captures as cap
from core import chain_latch, pg_goals_write

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

GOALS = "app.revenue_goals"
HISTORY = pg_goals_write.CAPTURE_TABLE
STAMP = cap.state_row(HISTORY)
CHAIN = pg_goals_write.CHAIN
COLUMNS = "period_type, goal_amount, is_custom, calculated_goal, growth_factor"


async def _clean(pool):
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {GOALS}")
        await conn.execute(f"DELETE FROM {HISTORY} WHERE period_type = ANY($1::text[])",
                           list(pg_goals_write.PERIOD_TYPES))
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", STAMP)
        # The latch's owner rows (`tests/integration/test_goals_writer.py`'s
        # reason: left behind they read as a chain with no local marker).
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")


@pytest_asyncio.fixture
async def stores(monkeypatch):
    """The repository the routes call, a live Postgres, the captures on."""
    from core.store import AnalyticsStore

    monkeypatch.delenv(pg_goals_write.WRITE_ENV, raising=False)
    monkeypatch.delenv(cap.SWITCH_ENV, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = AnalyticsStore()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    await _clean(pool)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)), \
         patch("core.pg.require_revision", new=AsyncMock()):
        yield store, pool, monkeypatch
    await _clean(pool)
    await pool.close()


async def _versions(pool, period_type):
    async with pool.acquire() as conn:
        return await conn.fetch(
            f"SELECT {COLUMNS}, change_kind, set_at FROM {HISTORY} "
            "WHERE period_type = $1 ORDER BY id", period_type)


async def _live(pool, period_type):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            f"SELECT {COLUMNS}, updated_at FROM {GOALS} WHERE period_type = $1",
            period_type)


async def _stamp(pool):
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT last_ok_at, failures_since_ok, last_error, last_rows "
            "FROM meta.mirror_state WHERE table_name = $1", STAMP)


def _values(row):
    return tuple(row[c.strip()] for c in COLUMNS.split(","))


def _at(hour: int) -> datetime:
    return datetime(2026, 11, 13, hour, 0, tzinfo=timezone.utc)


class TestEveryKindIsKeptAsWhatItWas:
    @pytest.mark.asyncio
    async def test_a_typed_goal_is_set_and_is_the_live_row(self, stores):
        """`POST /api/goals`' path. Kills: a version built from the caller's
        values rather than the stored row, a `set_at` on another clock than
        the goal's `updated_at`, and a capture that does not stamp."""
        store, pool, _env = stores

        await store.set_goal("monthly", 1_234_500.0, growth_factor=1.2)

        versions = await _versions(pool, "monthly")
        live = await _live(pool, "monthly")
        assert [v["change_kind"] for v in versions] == ["set"]
        assert _values(versions[0]) == _values(live)
        assert versions[0]["is_custom"] is True
        assert versions[0]["set_at"] == live["updated_at"]
        stamp = await _stamp(pool)
        assert stamp["last_ok_at"] is not None and stamp["last_rows"] == 1
        assert stamp["failures_since_ok"] == 0 and stamp["last_error"] is None

    @pytest.mark.asyncio
    async def test_a_reset_is_reset_to_auto_after_the_goal_it_replaced(self, stores):
        """`DELETE /api/goals/{period}`' path, through `reset_goal_to_auto`.
        Kills: the reset recorded as a recalculation, and the overwritten goal
        lost — the one thing `ON CONFLICT (period_type)` cannot keep."""
        store, pool, _env = stores

        await store.set_goal("daily", 99_000.0)
        out = await store.reset_goal_to_auto("daily")

        versions = await _versions(pool, "daily")
        assert [v["change_kind"] for v in versions] == ["set", "reset_to_auto"]
        typed, reset = versions
        assert (typed["goal_amount"], typed["is_custom"]) == (Decimal("99000.00"), True)
        assert reset["is_custom"] is False
        assert float(reset["goal_amount"]) == out["amount"]
        assert _values(reset) == _values(await _live(pool, "daily"))

    @pytest.mark.asyncio
    async def test_a_goal_stored_by_the_service_is_recalculated(self, stores):
        """No caller stores an automatic goal today; the writer's rule for one
        is pinned all the same. Kills: every non-custom write read as a reset."""
        _store, pool, _env = stores

        await pg_goals_write.set_goal("weekly", 300_000.0, False, 250_000.0, 1.10, _at(8))

        assert [v["change_kind"] for v in await _versions(pool, "weekly")] == ["recalculated"]

    @pytest.mark.asyncio
    async def test_every_write_is_a_version_the_same_number_included(self, stores):
        """Every goal ever set: a human typing the same number again is a
        second act, with its own `set_at`. Kills: a capture that skips a write
        whose content matches the last version."""
        _store, pool, _env = stores

        for hour in (8, 9):
            await pg_goals_write.set_goal("weekly", 300_000.0, True, 250_000.0, 1.10, _at(hour))

        versions = await _versions(pool, "weekly")
        assert [v["set_at"] for v in versions] == [_at(8), _at(9)]
        assert _values(versions[0]) == _values(versions[1])
        assert (await _stamp(pool))["last_rows"] == 1

    @pytest.mark.asyncio
    async def test_the_version_is_what_postgres_kept(self, stores):
        """Read back, not passed through: the numbers after NUMERIC's
        rounding. Kills: a capture of the caller's floats."""
        _store, pool, _env = stores

        await pg_goals_write.set_goal(
            "monthly", 1_000_000.004, True, 999_999.996, 1.234, _at(8))

        (version,) = await _versions(pool, "monthly")
        assert (version["goal_amount"], version["calculated_goal"],
                version["growth_factor"]) == (
            Decimal("1000000.00"), Decimal("1000000.00"), Decimal("1.23"))

    @pytest.mark.asyncio
    async def test_the_periods_are_kept_apart(self, stores):
        """A version is the row of its own period. Kills: a capture that reads
        back every period's row on each write."""
        store, pool, _env = stores

        for period, amount in (("daily", 60_000.0), ("weekly", 400_000.0),
                               ("monthly", 1_500_000.0)):
            await store.set_goal(period, amount)

        for period, amount in (("daily", "60000.00"), ("weekly", "400000.00"),
                               ("monthly", "1500000.00")):
            assert [v["goal_amount"] for v in await _versions(pool, period)] == [
                Decimal(amount)]


class _RollsBackAtTheEnd:
    """The writer's outermost transaction, rolled back where it would commit."""

    def __init__(self, tx):
        self._tx = tx

    async def __aenter__(self):
        await self._tx.start()
        return self._tx

    async def __aexit__(self, *exc):
        await self._tx.rollback()
        if exc[0] is None:
            raise _RolledBack()
        return False


class _RolledBack(Exception):
    pass


class _Conn:
    """The live connection, except that its outermost transaction rolls back;
    a savepoint inside it (the capture's) is left alone."""

    def __init__(self, conn):
        self._conn = conn

    def transaction(self, *args, **kwargs):
        tx = self._conn.transaction(*args, **kwargs)
        return tx if self._conn.is_in_transaction() else _RollsBackAtTheEnd(tx)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _Pool:
    def __init__(self, live):
        self._live = live

    def acquire(self, *args, **kwargs):
        return _Acquired(self._live.acquire(*args, **kwargs))


class _Acquired:
    def __init__(self, ctx):
        self._ctx = ctx

    async def __aenter__(self):
        return _Conn(await self._ctx.__aenter__())

    async def __aexit__(self, *exc):
        return await self._ctx.__aexit__(*exc)


class TestTheVersionLandsWithItsGoal:
    @pytest.mark.asyncio
    async def test_a_goal_that_rolls_back_leaves_no_version_and_no_stamp(self, stores):
        """The capture is inside the goal's transaction, not beside it.
        Kills: a capture on a connection or a transaction of its own — the
        history would then hold a goal that was never set."""
        _store, pool, _env = stores

        with patch("core.pg.get_pool", new=AsyncMock(return_value=_Pool(pool))):
            with pytest.raises(_RolledBack):
                await pg_goals_write.set_goal(
                    "monthly", 1_000_000.0, True, 900_000.0, 1.1, _at(8))

        assert await _live(pool, "monthly") is None
        assert await _versions(pool, "monthly") == []
        assert await _stamp(pool) is None


class TestACaptureThatFailsNeverFailsTheGoal:
    @pytest.mark.asyncio
    async def test_the_goal_is_written_and_the_failure_is_a_warn(self, stores, monkeypatch):
        """The server refuses the version (a NULL `set_at`, the shape of the
        design's T8). Kills: no savepoint — the goal would roll back with the
        capture — a raise into the writer, an earlier version touched, a
        failure left unstamped, and a finding that pages."""
        from core.data_quality import Severity

        _store, pool, _env = stores
        await pg_goals_write.set_goal("monthly", 1_000_000.0, True, 900_000.0, 1.1, _at(8))

        good = pg_goals_write._GOAL_HISTORY_SQL
        broken = good.replace("$2, updated_at", "$2, NULL::timestamptz")
        assert broken != good
        monkeypatch.setattr(pg_goals_write, "_GOAL_HISTORY_SQL", broken)
        row = await pg_goals_write.set_goal(
            "monthly", 2_000_000.0, True, 900_000.0, 1.1, _at(9))

        assert row[1] == Decimal("2000000.00")
        live = await _live(pool, "monthly")
        assert (live["goal_amount"], live["updated_at"]) == (Decimal("2000000.00"), _at(9))
        assert set(await chain_latch.read_owners(pool)) == set(pg_goals_write.CHAIN_TABLES)
        assert [v["goal_amount"] for v in await _versions(pool, "monthly")] == [
            Decimal("1000000.00")]
        stamp = await _stamp(pool)
        assert stamp["failures_since_ok"] == 1
        assert stamp["last_error"].startswith("NotNullViolationError")

        findings = [i for i in cap.judge(await cap.read_facts(pool=pool))
                    if i.table_name == HISTORY]
        assert [(i.check_name, i.severity) for i in findings] == [
            (cap.condition(cap.FAILED, HISTORY), Severity.WARN)]

        monkeypatch.setattr(pg_goals_write, "_GOAL_HISTORY_SQL", good)
        await pg_goals_write.set_goal("monthly", 3_000_000.0, True, 900_000.0, 1.1, _at(10))

        # The goal whose version failed is kept by the next write, as the goal
        # in force, with its own `updated_at`.
        versions = await _versions(pool, "monthly")
        assert [(v["goal_amount"], v["change_kind"], v["set_at"]) for v in versions] == [
            (Decimal("1000000.00"), "set", _at(8)),
            (Decimal("2000000.00"), "set", _at(9)),
            (Decimal("3000000.00"), "set", _at(10))]
        stamp = await _stamp(pool)
        assert stamp["failures_since_ok"] == 0 and stamp["last_error"] is None
        assert not [i for i in cap.judge(await cap.read_facts(pool=pool))
                    if i.table_name == HISTORY]


class TestTheSwitch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", ["off", "0", "false"])
    async def test_off_writes_the_goal_and_nothing_else(self, stores, value):
        """Kills: an in-transaction capture with no off switch (the design
        review's finding 20)."""
        store, pool, env = stores
        env.setenv(cap.SWITCH_ENV, value)

        await store.set_goal("monthly", 1_234_500.0)
        await store.reset_goal_to_auto("daily")

        assert (await _live(pool, "monthly"))["goal_amount"] == Decimal("1234500.00")
        assert await _live(pool, "daily") is not None
        for period in pg_goals_write.PERIOD_TYPES:
            assert await _versions(pool, period) == []
        assert await _stamp(pool) is None

    @pytest.mark.asyncio
    async def test_on_again_the_next_write_keeps_the_goal_in_force_and_itself(self, stores):
        """The goal written while the switch was off was in force until this
        write. Kills: the history skipping it."""
        store, pool, env = stores
        env.setenv(cap.SWITCH_ENV, "off")
        await store.set_goal("monthly", 1_000_000.0)
        env.delenv(cap.SWITCH_ENV)
        await store.set_goal("monthly", 1_100_000.0)

        assert [v["goal_amount"] for v in await _versions(pool, "monthly")] == [
            Decimal("1000000.00"), Decimal("1100000.00")]


async def _store_before_0036(pool, period, amount, is_custom, updated_at):
    """A goal as an image at 0035 stores it: the row, and no version."""
    async with pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO {GOALS} (period_type, goal_amount, is_custom, "
            "calculated_goal, growth_factor, updated_at, mirrored_at) "
            "VALUES ($1, $2, $3, $2, 1.10, $4, now()) "
            "ON CONFLICT (period_type) DO UPDATE SET goal_amount = EXCLUDED.goal_amount, "
            "is_custom = EXCLUDED.is_custom, updated_at = EXCLUDED.updated_at",
            period, amount, is_custom, updated_at)


class TestTheGoalInForceIsKeptBeforeItIsOverwritten:
    @pytest.mark.asyncio
    async def test_a_goal_stored_before_0036_survives_the_first_write(self, stores):
        """The review's case: the goal in force since 1 October, stored by the
        image at 0035, is overwritten on 14 November. Kills: the capture that
        reads only the row it wrote — the one goal the history exists to keep
        would be gone."""
        _store, pool, _env = stores
        await _store_before_0036(pool, "monthly", Decimal("2500000"), True, _at(1))

        await pg_goals_write.set_goal("monthly", 3_000_000.0, True, 2_400_000.0, 1.10, _at(9))

        versions = await _versions(pool, "monthly")
        assert [(v["goal_amount"], v["change_kind"], v["set_at"]) for v in versions] == [
            (Decimal("2500000.00"), "set", _at(1)),
            (Decimal("3000000.00"), "set", _at(9))]
        assert _values(versions[-1]) == _values(await _live(pool, "monthly"))
        stamp = await _stamp(pool)
        assert stamp["failures_since_ok"] == 0

    @pytest.mark.asyncio
    async def test_an_automatic_goal_in_force_says_reset_to_auto(self, stores):
        _store, pool, _env = stores
        await _store_before_0036(pool, "daily", Decimal("80000"), False, _at(2))

        await pg_goals_write.set_goal("daily", 90_000.0, True, 80_000.0, 1.10, _at(9))

        assert [v["change_kind"] for v in await _versions(pool, "daily")] == [
            "reset_to_auto", "set"]

    @pytest.mark.asyncio
    async def test_a_history_that_ends_with_the_goal_in_force_gains_one_row(self, stores):
        """Kills: the goal in force appended again on every write."""
        _store, pool, _env = stores
        for hour in (8, 9, 10):
            await pg_goals_write.set_goal("weekly", 300_000.0 + hour, True, 250_000.0,
                                          1.10, _at(hour))

        assert [v["set_at"] for v in await _versions(pool, "weekly")] == [
            _at(8), _at(9), _at(10)]

    @pytest.mark.asyncio
    async def test_a_goal_in_force_that_cannot_be_kept_holds_its_failure(
            self, stores, monkeypatch):
        """The new goal's version is not attempted after it: its ok stamp
        would clear the failure. The next write keeps both, the goal in force
        being the one this write stored. Kills: the failure cleared within the
        same write."""
        _store, pool, _env = stores
        await _store_before_0036(pool, "monthly", Decimal("2500000"), True, _at(1))
        good = pg_goals_write._GOAL_IN_FORCE_SQL
        monkeypatch.setattr(pg_goals_write, "_GOAL_IN_FORCE_SQL",
                            good.replace("COALESCE(g.updated_at, now())", "NULL::timestamptz"))

        await pg_goals_write.set_goal("monthly", 3_000_000.0, True, 2_400_000.0, 1.10, _at(9))

        assert (await _live(pool, "monthly"))["goal_amount"] == Decimal("3000000.00")
        assert await _versions(pool, "monthly") == []
        assert (await _stamp(pool))["failures_since_ok"] == 1

        monkeypatch.setattr(pg_goals_write, "_GOAL_IN_FORCE_SQL", good)
        await pg_goals_write.set_goal("monthly", 3_500_000.0, True, 2_400_000.0, 1.10, _at(10))

        assert [(v["goal_amount"], v["set_at"]) for v in await _versions(pool, "monthly")] == [
            (Decimal("3000000.00"), _at(9)), (Decimal("3500000.00"), _at(10))]
        assert (await _stamp(pool))["failures_since_ok"] == 0


class TestABlockedCaptureHoldsTheGoalBriefly:
    @pytest.mark.asyncio
    async def test_a_lock_on_the_history_costs_the_bound_and_the_goal_commits(
            self, stores, monkeypatch):
        """A person's CREATE INDEX on the history holds a SHARE lock: the
        capture waits its bound, rolls back alone, and the goal commits — its
        row lock held that long, not for `command_timeout`. The pool here has
        none, so without the bound this waits for the holder. Kills: the
        capture's statements run without a timeout of their own."""
        import asyncio
        import time

        _store, pool, _env = stores
        monkeypatch.setattr(cap, "CAPTURE_TIMEOUT_S", 0.5)
        holder = await asyncpg.connect(DSN)
        tx = holder.transaction()
        await tx.start()
        try:
            await holder.execute(f"LOCK TABLE {HISTORY} IN SHARE MODE")
            started = time.monotonic()
            await asyncio.wait_for(pg_goals_write.set_goal(
                "monthly", 1_000_000.0, True, 900_000.0, 1.1, _at(8)), timeout=10)
            took = time.monotonic() - started
        finally:
            await tx.rollback()
            await holder.close()

        assert took < 1.5
        assert (await _live(pool, "monthly"))["goal_amount"] == Decimal("1000000.00")
        assert await _versions(pool, "monthly") == []
        stamp = await _stamp(pool)
        assert stamp["failures_since_ok"] == 1
        assert stamp["last_error"].startswith("TimeoutError")
