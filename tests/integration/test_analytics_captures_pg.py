"""Revision 0036 and the capture helpers against a real PostgreSQL.

The suite's database is at the head. Nothing here moves it for good: the
revision's statements and Alembic's own rendering of the downgrade and the
upgrade run inside a transaction that is always rolled back. What the helpers
commit, they delete again; the `capture.*` stamps are cleared before and after.

What is pinned, each named by the mutation it kills:

- 0036 is applied, applies twice, and `ks_readonly` reads its tables as it
  reads the rest of `app.*` and writes none of them;
- the downgrade to 0035 moves the stamp and nothing else — every table and
  every row stays, and `require_revision("0035_batch_e_chains")`, which is
  what `rollback-step14` asks, passes — and the upgrade after it is a no-op
  (owner decision D2);
- a capture that raises inside a writer's transaction rolls back alone: the
  writer's rows commit, the capture's do not, and the failure is stamped;
- the watch reads those stamps back and files the finding;
- a never-stamped capture's grace is read off the journal's integrity runs,
  in the stamps' transaction, so a restart moves nothing; a journal the read
  is refused by costs that answer alone (a savepoint), and the capture is
  filed stalled as before.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import analytics_captures as cap
from core import pg
from tests import migration_replay as replay

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

REPO = Path(__file__).resolve().parents[2]
REV = replay.load(replay.VERSIONS / "0036_analytics_captures.py")
BEFORE = "0035_batch_e_chains"
HEAD = "0036_analytics_captures"

# One row each, the least every table's NOT NULLs accept.
ONE_ROW_EACH = {
    "app.analytics_buyer_key": "INSERT INTO app.analytics_buyer_key (buyer_id) VALUES (-1)",
    "app.analytics_person_key": (
        "INSERT INTO app.analytics_person_key (buyer_id, person_key, rule) "
        "VALUES (-1, 1, 'phone9')"),
    "app.analytics_phone_excluded": (
        "INSERT INTO app.analytics_phone_excluded (phone9, records, reason) "
        "VALUES ('000000000', 4, 'shared')"),
    "app.analytics_person_merges": (
        "INSERT INTO app.analytics_person_merges (from_person_key, to_person_key) VALUES (1, 2)"),
    "app.offer_cost_daily": (
        "INSERT INTO app.offer_cost_daily (date, offer_id) VALUES ('2026-11-13', -1)"),
    "app.catalogue_versions": (
        "INSERT INTO app.catalogue_versions (product_id, known_from, row_hash) "
        "VALUES (-1, now(), 1)"),
    "app.revenue_prediction_vintages": (
        "INSERT INTO app.revenue_prediction_vintages (prediction_date, sales_type) "
        "VALUES ('2026-11-20', 'retail')"),
    "app.revenue_goal_history": (
        "INSERT INTO app.revenue_goal_history (period_type, goal_amount, change_kind) "
        "VALUES ('monthly', 1, 'set')"),
    "app.order_channel_verdicts": (
        "INSERT INTO app.order_channel_verdicts "
        "(order_id, known_from, rule_version, change_kind, row_hash) "
        "VALUES (-1, now(), 'test', 'first', 1)"),
    "app.expense_deletions": (
        "INSERT INTO app.expense_deletions (expense_id, order_id, amount) VALUES (-1, -1, 1)"),
    "app.ga4_funnel": (
        "INSERT INTO app.ga4_funnel (grain, period_start, period_end, country_scope, "
        "breakdown, breakdown_value, step, active_users) "
        "VALUES ('day', '2026-11-12', '2026-11-12', 'ua', 'total', '', 1, 10)"),
}


def _alembic_sql(*args: str) -> str:
    """What `alembic <args> --sql` would execute, without its BEGIN/COMMIT, so
    it can run inside the test's own transaction."""
    out = subprocess.run(
        [sys.executable, "-m", "alembic", *args, "--sql"],
        cwd=REPO, capture_output=True, text=True, timeout=120,
        env={"PATH": "/usr/bin:/bin", "HOME": "/tmp"},
    )
    assert out.returncode == 0, out.stderr[-2000:]
    lines = [line for line in out.stdout.splitlines()
             if line.strip() not in ("BEGIN;", "COMMIT;")]
    return "\n".join(lines)


async def _stamp(conn) -> str:
    return await conn.fetchval("SELECT version_num FROM meta.alembic_version")


async def _counts(conn) -> dict:
    return {t: await conn.fetchval(f"SELECT count(*) FROM {t}") for t in REV.TABLES}


class TestTheRevisionOnAServer:
    @pytest.mark.asyncio
    async def test_the_eleven_tables_are_there_at_the_head(self):
        conn = await asyncpg.connect(DSN)
        try:
            assert await _stamp(conn) == HEAD == pg.REQUIRED_REVISION
            missing = [t for t in REV.TABLES
                       if await conn.fetchval("SELECT to_regclass($1)", t) is None]
            assert not missing
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_ks_readonly_reads_them_and_writes_none(self):
        """The default privileges in `postgres/initdb/30-app.sql`, not a GRANT
        in the revision. Kills: a table ks_readonly cannot read, or one it
        could write — `scripts/utm_reclassify_dryrun.py` refuses a login with
        any write right."""
        conn = await asyncpg.connect(DSN)
        try:
            for t in REV.TABLES:
                assert await conn.fetchval(
                    "SELECT has_table_privilege('ks_readonly', $1, 'SELECT')", t), t
                for right in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                    assert not await conn.fetchval(
                        "SELECT has_table_privilege('ks_readonly', $1, $2)", t, right), (t, right)
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_it_applies_twice(self):
        """Every statement again on a database that has them, then Alembic's
        own rendering of the step. Kills: an IF NOT EXISTS missing — the
        re-upgrade after a downgrade would fail on the first table."""
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            for statement in REV.STATEMENTS:
                await conn.execute(statement)
            await conn.execute(_alembic_sql("upgrade", f"{BEFORE}:{HEAD}"))
            assert await _stamp(conn) == HEAD
        finally:
            await tx.rollback()
            await conn.close()

    @pytest.mark.asyncio
    async def test_the_downgrade_moves_the_stamp_and_keeps_every_row(self):
        """D2, as Alembic renders it and the server runs it: down to 0035, the
        stamp is 0035, `rollback-step14`'s check passes, every table is there
        with its row; up again, the stamp is 0036 and nothing else moved.
        Kills: a DROP in the downgrade, or a re-upgrade that is not a no-op."""
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            for sql in ONE_ROW_EACH.values():
                await conn.execute(sql)
            before = await _counts(conn)
            assert all(n >= 1 for n in before.values())

            await conn.execute(_alembic_sql("downgrade", f"{HEAD}:{BEFORE}"))
            assert await _stamp(conn) == BEFORE
            await pg.require_revision(BEFORE, conn=conn)
            assert await _counts(conn) == before

            await conn.execute(_alembic_sql("upgrade", f"{BEFORE}:{HEAD}"))
            assert await _stamp(conn) == HEAD
            assert await _counts(conn) == before
        finally:
            await tx.rollback()
            await conn.close()


# ─── The helpers ──────────────────────────────────────────────────────────────

TAG = "test-analytics-capture"
GOALS = "app.revenue_goal_history"
VERSIONS = "app.catalogue_versions"
GOAL_SQL = ("INSERT INTO app.revenue_goal_history (period_type, goal_amount, change_kind) "
            "VALUES ($1, $2, 'set')")


async def _clean(conn):
    await conn.execute("DELETE FROM meta.mirror_state WHERE table_name LIKE 'capture.%'")
    await conn.execute("DELETE FROM app.revenue_goal_history WHERE period_type = $1", TAG)
    await conn.execute("DELETE FROM app.catalogue_versions WHERE product_id = -7")
    await conn.execute("DELETE FROM app.revenue_prediction_vintages WHERE sales_type = $1", TAG)
    await conn.execute("DELETE FROM app.revenue_predictions WHERE sales_type = $1", TAG)


@pytest_asyncio.fixture
async def pool(monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.delenv(cap.SWITCH_ENV, raising=False)
    created = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    async with created.acquire() as conn:
        await _clean(conn)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=created)):
        yield created
    async with created.acquire() as conn:
        await _clean(conn)
    await created.close()


async def _state(conn, table):
    return await conn.fetchrow(
        "SELECT last_ok_at, failures_since_ok, last_error, last_rows "
        "FROM meta.mirror_state WHERE table_name = $1", cap.state_row(table))


class TestInTransaction:
    @pytest.mark.asyncio
    async def test_a_capture_that_raises_leaves_the_writers_rows_committed(self, pool):
        """The review's case: the capture fails inside the writer's
        transaction. Kills: no savepoint (the writer's rows roll back with it),
        a raise into the writer, or a failure left unstamped."""
        async with pool.acquire() as conn:
            await conn.execute("CREATE TEMP TABLE writer_rows (n INTEGER)")
            async with conn.transaction():
                await conn.execute("INSERT INTO writer_rows VALUES (1)")
                kept = await cap.capture_in_transaction(conn, GOALS, GOAL_SQL, TAG, None)
                await conn.execute("INSERT INTO writer_rows VALUES (2)")
            assert kept is False
            assert await conn.fetchval("SELECT count(*) FROM writer_rows") == 2
            assert await conn.fetchval(
                "SELECT count(*) FROM app.revenue_goal_history WHERE period_type = $1", TAG) == 0
            state = await _state(conn, GOALS)
            assert state["failures_since_ok"] == 1
            assert state["last_error"].startswith("NotNullViolationError")
            await conn.execute("DROP TABLE writer_rows")

    @pytest.mark.asyncio
    async def test_a_kept_capture_commits_and_rolls_back_with_its_writer(self, pool):
        """The rows and the ok stamp are the writer's transaction's: they
        commit with it and vanish with it. Kills: a capture or a stamp that
        commits on its own."""
        async with pool.acquire() as conn:
            with pytest.raises(RuntimeError):
                async with conn.transaction():
                    assert await cap.capture_in_transaction(conn, GOALS, GOAL_SQL, TAG, 1) is True
                    raise RuntimeError("the writer fails after the capture")
            assert await _state(conn, GOALS) is None

            async with conn.transaction():
                assert await cap.capture_in_transaction(conn, GOALS, GOAL_SQL, TAG, 2) is True
            assert await conn.fetchval(
                "SELECT goal_amount FROM app.revenue_goal_history WHERE period_type = $1",
                TAG) == 2
            state = await _state(conn, GOALS)
            assert state["failures_since_ok"] == 0 and state["last_error"] is None
            assert state["last_rows"] == 1 and state["last_ok_at"] is not None

    @pytest.mark.asyncio
    async def test_a_success_after_a_failure_resets_the_count(self, pool):
        async with pool.acquire() as conn:
            async with conn.transaction():
                await cap.capture_in_transaction(conn, GOALS, GOAL_SQL, TAG, None)
            async with conn.transaction():
                await cap.capture_in_transaction(conn, GOALS, GOAL_SQL, TAG, None)
            assert (await _state(conn, GOALS))["failures_since_ok"] == 2
            async with conn.transaction():
                await cap.capture_in_transaction(conn, GOALS, GOAL_SQL, TAG, 3)
            assert (await _state(conn, GOALS))["failures_since_ok"] == 0

    @pytest.mark.asyncio
    async def test_the_switch_off_writes_nothing_at_all(self, pool, monkeypatch):
        monkeypatch.setenv(cap.SWITCH_ENV, "off")
        async with pool.acquire() as conn:
            async with conn.transaction():
                assert await cap.capture_in_transaction(conn, GOALS, GOAL_SQL, TAG, 1) is False
            assert await conn.fetchval(
                "SELECT count(*) FROM app.revenue_goal_history WHERE period_type = $1", TAG) == 0
            assert await _state(conn, GOALS) is None

    @pytest.mark.asyncio
    async def test_t8_an_unstamped_forecast_is_captured_and_replaced(self, pool):
        """The design's T8 on the real table: `app.revenue_predictions.created_at`
        is nullable (0025), so `made_at` is too, and the forecast's
        replacement and its vintage both commit."""
        capture = ("INSERT INTO app.revenue_prediction_vintages "
                   "(made_at, prediction_date, sales_type, predicted_revenue) "
                   "SELECT created_at, prediction_date, sales_type, predicted_revenue "
                   "FROM app.revenue_predictions WHERE sales_type = $1")
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO app.revenue_predictions "
                "(prediction_date, sales_type, predicted_revenue, created_at) "
                "VALUES ('2099-12-30', $1, 100000, NULL)", TAG)
            async with conn.transaction():
                assert await cap.capture_in_transaction(
                    conn, "app.revenue_prediction_vintages", capture, TAG) is True
                await conn.execute("DELETE FROM app.revenue_predictions WHERE sales_type = $1", TAG)
                await conn.execute(
                    "INSERT INTO app.revenue_predictions "
                    "(prediction_date, sales_type, predicted_revenue, created_at) "
                    "VALUES ('2099-12-30', $1, 200000, now())", TAG)
            assert await conn.fetchval(
                "SELECT predicted_revenue FROM app.revenue_predictions WHERE sales_type = $1",
                TAG) == 200000
            row = await conn.fetchrow(
                "SELECT made_at, captured_at, predicted_revenue "
                "FROM app.revenue_prediction_vintages WHERE sales_type = $1", TAG)
            assert row["made_at"] is None and row["captured_at"] is not None
            assert row["predicted_revenue"] == 100000


class TestOwnTransaction:
    @pytest.mark.asyncio
    async def test_a_refused_self_check_keeps_nothing_and_says_so(self, pool):
        async def capture(conn):
            await conn.execute(
                "INSERT INTO app.catalogue_versions (product_id, known_from, row_hash) "
                "VALUES (-7, now(), 1)")
            raise cap.CaptureRefused("would retire 600 of 1000 products")

        assert await cap.run_own_transaction(VERSIONS, capture) is False
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT count(*) FROM app.catalogue_versions WHERE product_id = -7") == 0
            assert (await _state(conn, VERSIONS))["last_error"].startswith(cap.SELF_CHECK_MARK)

    @pytest.mark.asyncio
    async def test_a_kept_capture_is_stamped_with_its_rows(self, pool):
        async def capture(conn):
            await conn.execute(
                "INSERT INTO app.catalogue_versions (product_id, known_from, row_hash) "
                "VALUES (-7, now(), 1)")
            return 1

        assert await cap.run_own_transaction(VERSIONS, capture) is True
        async with pool.acquire() as conn:
            state = await _state(conn, VERSIONS)
            assert state["last_rows"] == 1 and state["failures_since_ok"] == 0


class TestTheWatchReadsTheStamps:
    @pytest.mark.asyncio
    async def test_a_failure_stamped_is_a_finding_read_back(self, pool):
        """End to end: a failing capture's stamp, read by the integrity job's
        reader, judged a WARN named for its table. Kills: the reader asking
        the wrong rows, or the watch not seeing a stamp the helper wrote."""
        async with pool.acquire() as conn:
            async with conn.transaction():
                await cap.capture_in_transaction(conn, GOALS, GOAL_SQL, TAG, None)
        facts = await cap.read_facts(pool=pool)
        assert facts.unwatched is None and facts.now is not None
        assert facts.stamps[GOALS].failures_since_ok == 1
        issues = {i.check_name: i for i in cap.judge(facts)}
        assert issues[f"capture_failed:{GOALS}"].severity.value == "WARN"
        # Nothing else stamped: every capture with a cadence reads as never
        # stamped, found so for the first time (the journal holds no run that
        # filed it), so inside its grace; and none of them is CRITICAL.
        assert facts.journal_unread is None and facts.missing_since is not None
        unstarted = {f"capture_unstarted:{t}" for t, s in cap.CAPTURES.items()
                     if s.max_age is not None}
        assert unstarted <= set(issues)
        assert {issues[n].severity.value for n in unstarted} == {"INFO"}
        assert all(i.severity.value != "CRITICAL" for i in issues.values())

    @pytest.mark.asyncio
    async def test_with_the_switch_off_the_stamps_are_read_and_held(self, pool, monkeypatch):
        """The switch off judges no stamp, but reads them, and holds what
        stands: a failure stamped before it went off is not read as healed.
        Kills: an early return that reads nothing, which also left the GA4
        backlog unjudged while off."""
        async with pool.acquire() as conn:
            async with conn.transaction():
                await cap.capture_in_transaction(conn, GOALS, GOAL_SQL, TAG, None)
        monkeypatch.setenv(cap.SWITCH_ENV, "off")
        facts = await cap.read_facts(pool=pool)
        assert facts.switched_off and facts.unwatched is None and facts.now is not None
        assert facts.stamps[GOALS].failures_since_ok == 1
        (off,) = [i for i in cap.judge(facts) if i.check_name == cap.SWITCHED_OFF]
        assert off.severity.value == "WARN"
        assert f"capture_failed:{GOALS}" in off.unverified


_JOURNAL = ("app.data_quality_issues", "app.data_quality_diffs", "app.data_quality_runs")


@pytest_asyncio.fixture
async def journal(pool):
    """The journal emptied before and after, as the journal's own
    integration tests leave it, and a writer of integrity runs into it."""
    async def wipe():
        async with pool.acquire() as conn:
            for table in _JOURNAL:
                await conn.execute(f"DELETE FROM {table}")
    await wipe()

    async def run(run_id, hours_ago, *names, error=None, layer="integrity"):
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO app.data_quality_runs (run_id, started_at, ended_at, as_of, "
                "window_start, window_end, layer, status, error_message) "
                "SELECT $1, now() - make_interval(secs => $2), now() - make_interval(secs => $2), "
                "now(), current_date, current_date, $3, $4, $5",
                run_id, hours_ago * 3600.0, layer, "FAILED" if error else "WARN", error)
            for name in names:
                await conn.execute(
                    "INSERT INTO app.data_quality_issues (run_id, check_name, table_name, "
                    "severity, count) VALUES ($1, $2, $3, 'WARN', 1)",
                    run_id, name, name.split(":", 1)[-1])
    yield run
    await wipe()


VINTAGES = "app.revenue_prediction_vintages"
GA4 = "app.ga4_funnel"


class TestTheGraceIsReadFromTheJournal:
    """`read_facts` reads when each never-stamped capture was first found so
    out of the journal, beside the stamps. Kills: a grace measured from the
    process (a deploy would restart it), or a journal read whose failure
    takes the stamps with it."""

    @pytest.mark.asyncio
    async def test_a_streak_filed_before_this_rule_is_past_its_grace(self, pool, journal):
        """The runs since revision 0036 filed a never-stamped GA4 capture as
        stalled; their streak began five days ago, past its 30 h."""
        await journal(1, 120, f"capture_stalled:{GA4}")
        await journal(2, 60, f"capture_stalled:{GA4}")
        await journal(3, 6, f"capture_stalled:{GA4}")
        facts = await cap.read_facts(pool=pool)
        assert facts.journal_unread is None and facts.missing_since is not None
        assert facts.now is not None
        age = facts.now - facts.missing_since[GA4]
        assert timedelta(hours=119) < age < timedelta(hours=121)
        issues = {i.check_name: i for i in cap.judge(facts)}
        assert issues[f"capture_stalled:{GA4}"].severity.value == "WARN"
        assert f"capture_unstarted:{GA4}" not in issues

    @pytest.mark.asyncio
    async def test_inside_its_grace_it_is_info_and_another_process_agrees(self, pool, journal):
        """The vintages found missing for 30 h of their 99: INFO, and a new
        pool — a restart, a deploy — reads the same start."""
        await journal(1, 30, f"capture_unstarted:{VINTAGES}")
        await journal(2, 6, f"capture_unstarted:{VINTAGES}", f"capture_unstarted:{GA4}")
        facts = await cap.read_facts(pool=pool)
        assert facts.missing_since is not None and facts.now is not None
        assert timedelta(hours=29) < facts.now - facts.missing_since[VINTAGES] < timedelta(hours=31)
        issues = {i.check_name: i for i in cap.judge(facts)}
        assert issues[f"capture_unstarted:{VINTAGES}"].severity.value == "INFO"
        restarted = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
        try:
            again = await cap.read_facts(pool=restarted)
        finally:
            await restarted.close()
        assert again.missing_since == facts.missing_since

    @pytest.mark.asyncio
    async def test_failed_runs_and_other_layers_are_not_read_and_a_gap_breaks(self, pool, journal):
        """Successful integrity runs only. A failed run between two that
        filed it neither breaks the streak nor extends it; a successful run
        that did not file it does break it."""
        await journal(1, 50, f"capture_unstarted:{VINTAGES}")
        await journal(2, 40)                                        # broke it
        await journal(3, 30, f"capture_unstarted:{VINTAGES}")
        await journal(4, 20, error="RuntimeError: blind")           # not read
        await journal(5, 10, f"capture_unstarted:{VINTAGES}")
        await journal(6, 5, layer="mirror_landing")                 # not read
        facts = await cap.read_facts(pool=pool)
        assert facts.missing_since is not None and facts.now is not None
        assert timedelta(hours=29) < facts.now - facts.missing_since[VINTAGES] < timedelta(hours=31)

    @pytest.mark.asyncio
    async def test_a_journal_refusing_the_read_is_no_grace(self, pool, journal, monkeypatch):
        """Postgres refuses the journal's statement inside the stamps'
        transaction: the savepoint takes the refusal, the stamps are still
        read, and the never-stamped captures are WARN as before."""
        from core import data_quality

        monkeypatch.setattr(data_quality, "RECENT_FINDINGS_SQL",
                            "SELECT started_at, names FROM app.no_such_journal")
        facts = await cap.read_facts(pool=pool)
        assert facts.unwatched is None and facts.now is not None
        assert facts.missing_since is None
        assert facts.journal_unread and "no_such_journal" in facts.journal_unread
        issues = {i.check_name: i for i in cap.judge(facts)}
        assert issues[f"capture_stalled:{VINTAGES}"].severity.value == "WARN"
        assert "could not be read" in issues[f"capture_stalled:{VINTAGES}"].description

    @pytest.mark.asyncio
    async def test_with_every_capture_stamped_the_journal_is_not_asked(
            self, pool, journal, monkeypatch):
        from core import dq_journal

        async with pool.acquire() as conn:
            for table in cap.CAPTURES:
                await conn.execute(
                    "INSERT INTO meta.mirror_state (table_name, last_ok_at) VALUES ($1, now()) "
                    "ON CONFLICT (table_name) DO UPDATE SET last_ok_at = now()",
                    cap.state_row(table))

        async def refuse(*a, **k):
            raise AssertionError("the journal was asked with every capture stamped")

        monkeypatch.setattr(dq_journal, "recent_findings", refuse)
        facts = await cap.read_facts(pool=pool)
        assert facts.missing_since == {} and facts.journal_unread is None
        assert cap.judge(facts) == []


class TestABlockedCaptureHoldsItsWriterBriefly:
    """A capture inside a writer's transaction that waits on a lock — a
    CREATE INDEX, REINDEX or VACUUM FULL on its table, its stamp row held —
    holds the writer, and every lock the writer took, as long as it waits.
    Bounded by `CAPTURE_TIMEOUT_S` per statement; shortened here so the test
    is quick. The pool has no `command_timeout`, so without the bound these
    would wait until the holder lets go."""

    BOUND = 0.5

    async def _while_held(self, pool, monkeypatch, hold_sql):
        import asyncio
        import time

        monkeypatch.setattr(cap, "CAPTURE_TIMEOUT_S", self.BOUND)
        holder = await asyncpg.connect(DSN)
        tx = holder.transaction()
        await tx.start()
        try:
            await holder.execute(hold_sql)
            async with pool.acquire() as conn:
                await conn.execute("CREATE TEMP TABLE writer_rows (n INTEGER)")
                started = time.monotonic()

                async def writer():
                    async with conn.transaction():
                        await conn.execute("INSERT INTO writer_rows VALUES (1)")
                        kept = await cap.capture_in_transaction(conn, GOALS, GOAL_SQL, TAG, 1)
                        await conn.execute("INSERT INTO writer_rows VALUES (2)")
                    return kept

                kept = await asyncio.wait_for(writer(), timeout=10)
                took = time.monotonic() - started
                rows = await conn.fetchval("SELECT count(*) FROM writer_rows")
                await conn.execute("DROP TABLE writer_rows")
        finally:
            await tx.rollback()
            await holder.close()
        return kept, took, rows

    @pytest.mark.asyncio
    async def test_a_lock_on_the_capture_table(self, pool, monkeypatch):
        kept, took, rows = await self._while_held(
            pool, monkeypatch, "LOCK TABLE app.revenue_goal_history IN SHARE MODE")
        assert kept is False and rows == 2
        assert took < 3 * self.BOUND
        async with pool.acquire() as conn:
            state = await _state(conn, GOALS)
            assert state["failures_since_ok"] == 1
            assert state["last_error"].startswith("TimeoutError")
            assert await conn.fetchval(
                "SELECT count(*) FROM app.revenue_goal_history WHERE period_type = $1", TAG) == 0

    @pytest.mark.asyncio
    async def test_its_stamp_row_held(self, pool, monkeypatch):
        """The ok stamp waits one bound, the failure stamp another: twice the
        bound at most, never KS_PG_TIMEOUT twice."""
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO meta.mirror_state (table_name, last_attempted_at, "
                "failures_since_ok) VALUES ($1, now(), 0)", cap.state_row(GOALS))
        kept, took, rows = await self._while_held(
            pool, monkeypatch,
            f"SELECT 1 FROM meta.mirror_state WHERE table_name = "
            f"'{cap.state_row(GOALS)}' FOR UPDATE")
        assert kept is False and rows == 2
        assert took < 4 * self.BOUND
