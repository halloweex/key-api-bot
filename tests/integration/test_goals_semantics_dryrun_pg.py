"""The goals dry run's Postgres half against a real PostgreSQL (review of 7b-2).

The flip reads Postgres `silver.orders`, and the backup half compares DuckDB
with DuckDB. So the gate also reads, out of Postgres' copy of the quality
journal, the latest `mirror_landing` run's verdict on `silver.orders` —
`reconcile_silver`, the one comparison that sets the two Silvers against each
other at one instant. Proved here on the real schema:

  * the statements read what `judge_postgres` is given — the journal copy's
    mark, the latest run of the layer (not an older one, not another layer's)
    and its findings against `silver.orders` alone — inside a READ ONLY
    transaction the server assigns no transaction id;
  * the door refuses `ks_app`, which can write, before a row is read;
  * as production runs it — `ks_readonly`, the real `main()` in a fresh
    interpreter under the audit hook — the only connection is the read-only
    login's, `KS_PG_DSN` poisoned beside it, nothing written; a finding
    against `silver.orders` turns a clean backup into exit 1.

Seeded rows use run ids from 9 100 000 000 001, above anything the rest of the
suite writes, and the journal copy's mark is put back as it was found.
"""
from __future__ import annotations

import ast
import asyncio
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from scripts import goals_semantics_dryrun as dryrun  # noqa: E402
from scripts import utm_reclassify_dryrun as door  # noqa: E402
from tests.unit.test_utm_reclassify_dryrun import POISONED_PG_DSN  # noqa: E402
from tests.write_audit import run_main_audited  # noqa: E402

DSN = os.getenv("KS_PG_DSN")
READONLY_DSN = os.getenv("KS_PG_READONLY_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

# The same sentence the reclassify dry run's skips carry, which CI's "no store
# test may have skipped" step greps for: in CI these run or the job fails.
PROVISIONED = ("needs a live PostgreSQL provisioned as .github/workflows/ci.yml "
               "provisions it")

OLDER_RUN = 9_100_000_000_001
RUN = OLDER_RUN + 1
OTHER_LAYER_RUN = OLDER_RUN + 2
JOURNAL = "app.data_quality_runs"


async def _clean(conn):
    await conn.execute("DELETE FROM app.data_quality_issues WHERE run_id >= $1", OLDER_RUN)
    await conn.execute("DELETE FROM app.data_quality_runs WHERE run_id >= $1", OLDER_RUN)


@pytest_asyncio.fixture
async def journal():
    """This morning's run clean on Silver with a finding elsewhere, an older
    run with a finding on Silver, and a newer run of another layer — and the
    journal copy shipped ten minutes ago."""
    conn = await asyncpg.connect(DSN)
    try:
        saved = await conn.fetchrow(
            "SELECT * FROM meta.mirror_state WHERE table_name = $1", JOURNAL)
        await _clean(conn)
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", JOURNAL)
        await conn.execute(
            "INSERT INTO meta.mirror_state (table_name, last_attempted_at, last_ok_at, "
            "failures_since_ok, last_rows) VALUES ($1, now() - interval '10 minutes', "
            "now() - interval '10 minutes', 0, 1)", JOURNAL)
        runs = [
            (OLDER_RUN, "26 hours", "mirror_landing"),
            (RUN, "2 hours", "mirror_landing"),
            (OTHER_LAYER_RUN, "1 hour", "integrity"),
        ]
        for run_id, ago, layer in runs:
            await conn.execute(
                "INSERT INTO app.data_quality_runs (run_id, started_at, ended_at, as_of, "
                "window_start, window_end, layer, status) VALUES ($1, "
                f"now() - interval '{ago}', now() - interval '{ago}', now(), "
                "current_date, current_date, $2, 'WARN')", run_id, layer)
        for run_id, check, table in (
            (OLDER_RUN, "mirror_missing_rows", "silver.orders"),
            (RUN, "mirror_buckets_disagree", "app.order_versions"),
            (OTHER_LAYER_RUN, "pk_uniqueness_silver_orders", "silver.orders"),
        ):
            await conn.execute(
                "INSERT INTO app.data_quality_issues (run_id, check_name, table_name, "
                "severity, count) VALUES ($1, $2, $3, 'CRITICAL', 1)",
                run_id, check, table)
        yield conn
    finally:
        await _clean(conn)
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", JOURNAL)
        if saved is not None:
            await conn.execute(
                "INSERT INTO meta.mirror_state (" + ", ".join(saved.keys()) + ") VALUES ("
                + ", ".join(f"${i}" for i in range(1, len(saved) + 1)) + ")",
                *saved.values())
        await conn.close()


async def _session(dsn=DSN):
    """The script's own session settings, behind the door it would refuse
    `ks_app` at — the statements are what is under test here."""
    return await asyncpg.connect(dsn, server_settings={
        **door.SESSION, "application_name": dryrun.APPLICATION_NAME})


class TestTheStatements:
    @pytest.mark.asyncio
    async def test_the_latest_run_of_the_layer_clean_on_silver(self, journal):
        conn = await _session()
        try:
            state = await dryrun.read_postgres_state(conn)
        finally:
            await conn.close()
        assert state["run"]["run_id"] == RUN
        assert state["findings"] == []          # the finding elsewhere is not read
        assert 9 * 60 <= (state["taken_at"] - state["journal_ok_at"]).total_seconds() <= 11 * 60
        verdict = dryrun.judge_postgres(state, landing_on=True)
        assert verdict.clean, verdict.reasons

    @pytest.mark.asyncio
    async def test_a_finding_against_silver_is_read_and_is_not_proved(self, journal):
        await journal.execute(
            "INSERT INTO app.data_quality_issues (run_id, check_name, table_name, "
            "severity, count) VALUES ($1, 'mirror_row_values', 'silver.orders', "
            "'CRITICAL', 7)", RUN)
        conn = await _session()
        try:
            state = await dryrun.read_postgres_state(conn)
        finally:
            await conn.close()
        assert state["findings"] == [("mirror_row_values", "CRITICAL", 7)]
        verdict = dryrun.judge_postgres(state, landing_on=True)
        assert not verdict.clean
        assert "mirror_row_values (CRITICAL, 7)" in verdict.reasons[0]

    @staticmethod
    def _writing(conn, *, declared: bool):
        """`conn` with a write slipped in before the first read, on a session
        without the read-only default. `declared=False` also takes the
        READ ONLY declaration away, so only the transaction-id check is left."""

        class _Writes:
            def transaction(self, **kw):
                return conn.transaction(**(kw if declared else {}))

            async def fetchval(self, sql, *a):
                if sql.startswith("SELECT statement_timestamp"):
                    await conn.execute(
                        "UPDATE meta.mirror_state SET last_rows = last_rows + 1 "
                        "WHERE table_name = $1", JOURNAL)
                return await conn.fetchval(sql, *a)

            def __getattr__(self, name):
                return getattr(conn, name)

        return _Writes()

    @pytest.mark.asyncio
    async def test_the_declared_read_only_transaction_refuses_a_write(self, journal):
        conn = await asyncpg.connect(DSN)
        try:
            with pytest.raises(asyncpg.exceptions.ReadOnlySQLTransactionError):
                await dryrun.read_postgres_state(self._writing(conn, declared=True))
        finally:
            await conn.close()
        assert await journal.fetchval(
            "SELECT last_rows FROM meta.mirror_state WHERE table_name = $1", JOURNAL) == 1

    @pytest.mark.asyncio
    async def test_without_the_declaration_the_transaction_id_catches_it(self, journal):
        """The server's word that nothing wrote: a transaction id is assigned
        on the first write and only then, so the write is caught and rolled
        back. Mutation: drop `assert_wrote_nothing` and the row moves."""
        conn = await asyncpg.connect(DSN)
        try:
            with pytest.raises(door.WroteSomething):
                await dryrun.read_postgres_state(self._writing(conn, declared=False))
        finally:
            await conn.close()
        assert await journal.fetchval(
            "SELECT last_rows FROM meta.mirror_state WHERE table_name = $1", JOURNAL) == 1

    @pytest.mark.asyncio
    async def test_the_door_refuses_a_login_that_can_write(self, journal):
        with pytest.raises(dryrun.Refused, match="this login can write"):
            await dryrun.postgres_half(DSN)


@pytest.mark.skipif(not READONLY_DSN,
                    reason=f"{PROVISIONED}: a ks_readonly login at KS_PG_READONLY_DSN")
class TestAsKsReadonlyUnderAudit:
    def _backup(self, tmp_path):
        from tests.unit.test_goals_semantics_dryrun import _backup

        return _backup(tmp_path)

    def _run(self, tmp_path, backup):
        return run_main_audited(
            "scripts.goals_semantics_dryrun",
            ["--backup", str(backup), "--today", "2026-10-01"],
            env={"KS_PG_READONLY_DSN": READONLY_DSN, "KS_PG_DSN": POISONED_PG_DSN,
                 "KS_READ_GOALS": "postgres"},
            cwd=tmp_path)

    def test_clean_on_both_halves_is_exit_0(self, journal, tmp_path):
        """The front door as production walks it. Every connection is to the
        read-only login's port — `KS_PG_DSN` is poisoned with another, and
        `KS_READ_GOALS=postgres` is set, so a goal read routed off the copy
        would show — and nothing is written."""
        backup = self._backup(tmp_path)
        run = self._run(tmp_path, backup)
        assert run.code == 0, run.stderr + run.stdout
        assert "reconcile_silver filed nothing against silver.orders" in run.stdout
        assert run.writes == [] and run.mutations == [] and run.commands == []
        ports = {ast.literal_eval(c)[1] for c in run.connects}
        assert ports == {urlsplit(READONLY_DSN).port}, run.connects

    def test_postgres_silver_not_proved_is_exit_1(self, journal, tmp_path):
        asyncio.run(_insert_silver_finding())
        backup = self._backup(tmp_path)
        run = self._run(tmp_path, backup)
        assert run.code == 1, run.stderr + run.stdout
        assert "NOT PROVED" in run.stdout and "mirror_missing_rows" in run.stdout


async def _insert_silver_finding():
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(
            "INSERT INTO app.data_quality_issues (run_id, check_name, table_name, "
            "severity, count) VALUES ($1, 'mirror_missing_rows', 'silver.orders', "
            "'CRITICAL', 3)", RUN)
    finally:
        await conn.close()
