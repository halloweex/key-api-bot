"""The stage 4 soak checks against a real Postgres (DN-03).

Each file under `deploy/stage4_soak/` is one query that returns its own verdict.
What has to be true of them is not that they parse — it is that each one says
FAIL on the state it exists to catch, PASS on the state next to it, and UNKNOWN
where the evidence it reads cannot be trusted. That needs seeded rows, a clock
the test controls and a real server.

HOW A SCENARIO IS ISOLATED
Every scenario runs in one transaction that is rolled back: seed, then
`SET TRANSACTION READ ONLY`, then the check. Nothing seeded survives the test —
and the check itself runs in a read-only transaction, which proves the same
thing `deploy/stage4_soak.sh` relies on in production: it cannot write.

THE CLOCK
The files read `soak.now` when it is set and `now()` otherwise; the script
never sets it. Here it is a fixed Wednesday noon in Kyiv in 2030, far from any
row another test committed, so a heavy-job window or a stray `mirrored_at`
cannot make a verdict depend on when CI happened to run.

`psql` variables (`:'inventory_on'` and friends) mean nothing to asyncpg, so
`render` substitutes them the way `psql -v` would.
"""
from __future__ import annotations

import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
needs_pg = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

REPO = Path(__file__).resolve().parents[2]
SQL_DIR = REPO / "deploy" / "stage4_soak"
FILES = sorted(SQL_DIR.glob("*.sql"))
VERDICTS = {"PASS", "FAIL", "UNKNOWN"}
VARIABLES = {"inventory_on": "0", "inventory_flip_at": "", "dq_pg_warehouse_on": "0"}
RUN_AS_OWNER = "-- soak:run-as ks_app"

KYIV = ZoneInfo("Europe/Kyiv")
# A Wednesday, noon in Kyiv: inside no heavy-lock window, after the 07:30 run.
NOW = datetime(2030, 6, 5, 12, 0, tzinfo=KYIV)
ORDER_IDS = [990_000_001, 990_000_002, 990_000_003]
DQ_RUN_IDS = [990_000_101, 990_000_102, 990_000_103]


def _uncommented(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def _code(sql: str) -> str:
    """The SQL with comments and string literals removed."""
    return re.sub(r"'(?:[^']|'')*'", "''", _uncommented(sql))


def _variables(sql: str) -> set[str]:
    """psql's quoted interpolations, `:'name'`, outside comments."""
    return set(re.findall(r":'(\w+)'", _uncommented(sql)))


def render(name: str, **variables: str) -> str:
    """The file as `psql -v name=value` would hand it to the server."""
    sql = (SQL_DIR / name).read_text(encoding="utf-8")
    unknown = _variables(sql) - set(VARIABLES)
    assert not unknown, f"{name} uses undocumented psql variables {sorted(unknown)}"
    for key, value in {**VARIABLES, **variables}.items():
        sql = sql.replace(f":'{key}'", "'" + value.replace("'", "''") + "'")
    return sql


# ── facts about the files, no database needed ─────────────────────────────────

class TestTheFiles:
    def test_there_are_checks(self):
        assert len(FILES) >= 20, [f.name for f in FILES]

    @pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
    def test_one_statement_each(self, path):
        code = _code(path.read_text(encoding="utf-8")).strip()
        assert code.endswith(";") and code.count(";") == 1, (
            f"{path.name} must be exactly one statement: psql runs the file whole, "
            f"and the report reads one result set per file")

    @pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
    def test_nothing_that_writes(self, path):
        """The runtime guard is the read-only transaction; this says why a
        file failed before a server is involved."""
        code = _code(path.read_text(encoding="utf-8"))
        verbs = re.findall(
            r"\b(insert|update|delete|truncate|alter|create|drop|grant|revoke|copy|"
            r"merge|nextval|setval|set_config|lock)\b", code, flags=re.I)
        assert not verbs, f"{path.name} contains {sorted(set(verbs))}"

    @pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
    def test_only_sequence_reads_run_as_the_owner(self, path):
        """`ks_app` is for what `ks_readonly` cannot read, and nothing else."""
        text = path.read_text(encoding="utf-8")
        reads_sequence = bool(re.search(r"\b\w+_id_seq\b", _code(text)))
        assert (text.splitlines()[0] == RUN_AS_OWNER) == reads_sequence, path.name

    @pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
    def test_only_documented_variables(self, path):
        render(path.name)

    def test_the_history_check_measures_against_the_canarys_limit(self):
        """20 asks whether the canary would have paged, so its limit is the
        canary's: a copy that drifted would certify a history the page judges
        differently."""
        from bot.canary import DQ_MAX_AGE_S

        sql = _uncommented(
            (SQL_DIR / "20_reconciliation_pg_history.sql").read_text(encoding="utf-8"))
        found = re.findall(r"interval\s+'(\d+)\s+hours'\s+AS\s+canary_max_age", sql)
        assert len(found) == 1, found
        assert int(found[0]) * 3600 == DQ_MAX_AGE_S["reconciliation_pg"]


# ── against a migrated Postgres ───────────────────────────────────────────────

@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    yield p
    await p.close()


@asynccontextmanager
async def scenario(pool):
    """One transaction, always rolled back."""
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            yield conn
        finally:
            await tr.rollback()


async def check(conn, name: str, *, now: datetime | None = NOW, **variables: str):
    if now is not None:
        await conn.execute("SELECT set_config('soak.now', $1, true)", now.isoformat())
    await conn.execute("SET TRANSACTION READ ONLY")
    rows = await conn.fetch(render(name, **variables))
    assert rows, f"{name} returned no row"
    return [dict(r) for r in rows]


async def verdict(conn, name: str, **kw) -> tuple[str, str]:
    rows = await check(conn, name, **kw)
    assert len(rows) == 1, rows
    return rows[0]["verdict"], rows[0]["detail"]


def ago(**delta) -> datetime:
    return NOW - timedelta(**delta)


async def signal(conn, *, requested, built, requested_at, built_at):
    await conn.execute(
        """
        INSERT INTO meta.derivation_signal (layer, requested, built, requested_at, built_at)
        VALUES ('warehouse', $1, $2, $3, $4)
        ON CONFLICT (layer) DO UPDATE SET requested = EXCLUDED.requested,
            built = EXCLUDED.built, requested_at = EXCLUDED.requested_at,
            built_at = EXCLUDED.built_at
        """, requested, built, requested_at, built_at)


async def no_runs(conn):
    await conn.execute("DELETE FROM meta.derivation_runs")


async def run(conn, *, trigger, started_at, seen=7, error=None, passed=True, seconds=3):
    """One journal row. Insert in chronological order: the checks order by id."""
    await conn.execute(
        """
        INSERT INTO meta.derivation_runs
               (layer, trigger, started_at, ended_at, requested_seen,
                validation_passed, validation, error)
        VALUES ('warehouse', $1, $2, $3, $4, $5, $6::jsonb, $7)
        """,
        trigger, started_at, started_at + timedelta(seconds=seconds), seen,
        None if error else passed,
        None if error else '{"bronze_orders": 10, "row_count_match": true}',
        error)


async def mirror_state(conn, table, *, ok_at, attempted_at=None, failures=0, error=None, rows=None):
    await conn.execute(
        """
        INSERT INTO meta.mirror_state
               (table_name, last_attempted_at, last_ok_at, failures_since_ok, last_error, last_rows)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (table_name) DO UPDATE SET
            last_attempted_at = EXCLUDED.last_attempted_at, last_ok_at = EXCLUDED.last_ok_at,
            failures_since_ok = EXCLUDED.failures_since_ok, last_error = EXCLUDED.last_error,
            last_rows = EXCLUDED.last_rows
        """, table, attempted_at or ok_at, ok_at, failures, error, rows)


async def bronze_order(conn, oid, *, mirrored_at):
    await conn.execute(
        """
        INSERT INTO bronze.orders (id, source_id, status_id, grand_total, mirrored_at)
        VALUES ($1, 1, 1, 100, $2)
        """, oid, mirrored_at)


async def dq_run(conn, run_id, *, layer, started_at, error=None):
    await conn.execute(
        """
        INSERT INTO app.data_quality_runs
               (run_id, started_at, ended_at, as_of, window_start, window_end, layer,
                status, error_message)
        VALUES ($1, $2, $2, $2, $3, $3, $4, $5, $6)
        """, run_id, started_at, started_at.date(), layer,
        "FAILED" if error else "OK", error)


async def dq_issue(conn, run_id, check_name, *, table="silver.orders", count=1):
    await conn.execute(
        """
        INSERT INTO app.data_quality_issues (run_id, check_name, table_name, severity, count)
        VALUES ($1, $2, $3, 'WARN', $4)
        """, run_id, check_name, table, count)


@needs_pg
class TestEveryCheckRuns:
    VARIANTS = (
        {"inventory_on": "0", "dq_pg_warehouse_on": "0"},
        {"inventory_on": "1", "dq_pg_warehouse_on": "1",
         "inventory_flip_at": "2030-06-01 10:00+03"},
        {"inventory_on": "1", "dq_pg_warehouse_on": "1"},
        {"inventory_on": "invalid", "dq_pg_warehouse_on": "invalid"},
        {"inventory_on": "unknown", "dq_pg_warehouse_on": "unknown"},
    )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
    @pytest.mark.parametrize("clock", ["fixed", "real"])
    async def test_returns_verdict_rows_read_only(self, pool, path, clock):
        """On whatever the database holds, under every flag the script can pass,
        with the real clock as well as the fixed one."""
        uses_variables = _variables(path.read_text(encoding="utf-8"))
        for variables in (self.VARIANTS if uses_variables else self.VARIANTS[:1]):
            async with scenario(pool) as conn:
                rows = await check(conn, path.name, now=NOW if clock == "fixed" else None,
                                   **variables)
            for row in rows:
                assert list(row) == ["check", "verdict", "detail"], row
                assert row["verdict"] in VERDICTS, row
                assert row["check"] and isinstance(row["check"], str), row
                assert row["detail"] and "\n" not in row["detail"], row

    @pytest.mark.asyncio
    async def test_check_names_are_unique(self, pool):
        names = []
        for path in FILES:
            async with scenario(pool) as conn:
                names += [r["check"] for r in await check(conn, path.name)]
        assert len(names) == len(set(names)), names

    @pytest.mark.asyncio
    async def test_inventory_checks_are_not_applicable_while_the_chain_is_off(self, pool):
        for path in FILES:
            if not path.name[3:].startswith("i"):
                continue
            async with scenario(pool) as conn:
                v, detail = await verdict(conn, path.name, inventory_on="0")
            assert (v, detail.startswith("not applicable")) == ("PASS", True), (path.name, detail)


@needs_pg
class TestD1OwedRebuild:
    FILE = "02_d1_owed_rebuild.sql"

    @pytest.mark.asyncio
    async def test_owed_and_unanswered_for_fifteen_minutes_fails(self, pool):
        async with scenario(pool) as conn:
            await signal(conn, requested=5, built=3,
                         requested_at=ago(minutes=20), built_at=ago(minutes=30))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail

    @pytest.mark.asyncio
    async def test_settled_passes(self, pool):
        async with scenario(pool) as conn:
            await signal(conn, requested=5, built=5,
                         requested_at=ago(minutes=20), built_at=ago(minutes=5))
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_no_build_for_an_hour_fails_even_with_nothing_owed(self, pool):
        async with scenario(pool) as conn:
            await signal(conn, requested=5, built=5,
                         requested_at=ago(hours=2), built_at=ago(minutes=70))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail

    @pytest.mark.asyncio
    async def test_the_first_order_after_a_quiet_hour_is_not_a_stall(self, pool):
        """Built 50 min ago, then one order a minute ago: owed at once, and
        built within a tick. Judged from `built_at` this read as FAIL."""
        async with scenario(pool) as conn:
            await signal(conn, requested=6, built=5,
                         requested_at=ago(minutes=1), built_at=ago(minutes=50))
            await bronze_order(conn, ORDER_IDS[0], mirrored_at=ago(minutes=1))
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_owed_during_training_is_unknown(self, pool):
        monday_training = datetime(2030, 6, 3, 3, 45, tzinfo=KYIV)
        async with scenario(pool) as conn:
            await signal(conn, requested=5, built=3,
                         requested_at=monday_training - timedelta(minutes=20),
                         built_at=monday_training - timedelta(minutes=30))
            v, detail = await verdict(conn, self.FILE, now=monday_training)
        assert v == "UNKNOWN" and "model training" in detail, detail


@needs_pg
class TestD2Journal:
    FILE = "03_d2_derivation_journal.sql"

    @pytest.mark.asyncio
    async def test_a_clean_day_passes(self, pool):
        async with scenario(pool) as conn:
            await no_runs(conn)
            await run(conn, trigger="heartbeat", started_at=ago(hours=2))
            await run(conn, trigger="signal", started_at=ago(hours=1))
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_an_error_in_the_last_day_fails(self, pool):
        async with scenario(pool) as conn:
            await no_runs(conn)
            await run(conn, trigger="signal", started_at=ago(hours=2),
                      error="silver.orders: PostgresError: boom")
            await run(conn, trigger="heartbeat", started_at=ago(hours=1))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL" and "boom" in detail, detail

    @pytest.mark.asyncio
    async def test_an_error_older_than_a_day_does_not(self, pool):
        async with scenario(pool) as conn:
            await no_runs(conn)
            await run(conn, trigger="signal", started_at=ago(hours=30), error="old")
            await run(conn, trigger="heartbeat", started_at=ago(hours=1))
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_no_run_at_all_fails(self, pool):
        async with scenario(pool) as conn:
            await no_runs(conn)
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail


@needs_pg
class TestD4UnmarkedWrites:
    FILE = "05_d4_unmarked_writes.sql"

    @pytest.mark.asyncio
    async def test_an_order_between_two_runs_that_saw_no_mark_fails(self, pool):
        async with scenario(pool) as conn:
            await no_runs(conn)
            await run(conn, trigger="signal", started_at=ago(hours=3), seen=7)
            await bronze_order(conn, ORDER_IDS[0], mirrored_at=ago(hours=2, minutes=30))
            await run(conn, trigger="heartbeat", started_at=ago(hours=2), seen=7)
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL" and "1 order(s)" in detail, detail

    @pytest.mark.asyncio
    async def test_the_same_before_a_first_tick_run_passes(self, pool):
        """The boot sync writes before the mode is configured: a restart."""
        async with scenario(pool) as conn:
            await no_runs(conn)
            await run(conn, trigger="signal", started_at=ago(hours=3), seen=7)
            await bronze_order(conn, ORDER_IDS[0], mirrored_at=ago(hours=2, minutes=30))
            await run(conn, trigger="first_tick", started_at=ago(hours=2), seen=7)
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_a_restart_whose_first_run_is_signal_passes(self, pool):
        """Measured 2026-09-17: after a deploy the first run is usually `signal`,
        not `first_tick`. It needs no exclusion — owed means its
        `requested_seen` moved — and the unmarked boot-sync rows before it
        must not read as a lost mark."""
        async with scenario(pool) as conn:
            await no_runs(conn)
            await run(conn, trigger="heartbeat", started_at=ago(hours=3), seen=7)
            await bronze_order(conn, ORDER_IDS[0], mirrored_at=ago(hours=2, minutes=2))
            await bronze_order(conn, ORDER_IDS[1], mirrored_at=ago(hours=2, minutes=1))
            await run(conn, trigger="signal", started_at=ago(hours=2), seen=9)
            await run(conn, trigger="heartbeat", started_at=ago(hours=1), seen=9)
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_a_quiet_pair_with_nothing_written_passes(self, pool):
        async with scenario(pool) as conn:
            await no_runs(conn)
            await run(conn, trigger="heartbeat", started_at=ago(hours=3), seen=7)
            await run(conn, trigger="heartbeat", started_at=ago(hours=2), seen=7)
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail


@needs_pg
class TestD5DroppedMarks:
    FILE = "06_d5_dropped_marks.sql"

    @pytest.mark.asyncio
    async def test_a_drop_newer_than_the_last_validated_run_fails(self, pool):
        async with scenario(pool) as conn:
            await no_runs(conn)
            await run(conn, trigger="heartbeat", started_at=ago(minutes=30))
            await mirror_state(conn, "meta.derivation_signal", ok_at=None,
                               attempted_at=ago(minutes=10), failures=1,
                               error="LockNotAvailableError: timeout")
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL" and "not covered" in detail, detail

    @pytest.mark.asyncio
    async def test_a_drop_older_than_the_last_validated_run_passes(self, pool):
        async with scenario(pool) as conn:
            await no_runs(conn)
            await run(conn, trigger="heartbeat", started_at=ago(minutes=30))
            await mirror_state(conn, "meta.derivation_signal", ok_at=None,
                               attempted_at=ago(hours=2), failures=3,
                               error="LockNotAvailableError: timeout")
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS" and "covered" in detail, detail

    @pytest.mark.asyncio
    async def test_a_later_run_that_did_not_validate_covers_nothing(self, pool):
        async with scenario(pool) as conn:
            await no_runs(conn)
            await run(conn, trigger="heartbeat", started_at=ago(hours=3))
            await mirror_state(conn, "meta.derivation_signal", ok_at=None,
                               attempted_at=ago(hours=2), failures=1)
            await run(conn, trigger="heartbeat", started_at=ago(minutes=30), passed=False)
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail

    @pytest.mark.asyncio
    async def test_no_drop_on_record_passes(self, pool):
        async with scenario(pool) as conn:
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = 'meta.derivation_signal'")
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail


@needs_pg
class TestStaleEvidenceIsUnknown:
    """A check that reads `app.data_quality_*` reads an hourly copy. A copy
    that stopped moving must never read as a clean journal."""

    TODAY_0730 = datetime(2030, 6, 5, 7, 30, tzinfo=KYIV)

    async def _clean_mirror_landing_run(self, conn):
        await dq_run(conn, DQ_RUN_IDS[0], layer="mirror_landing", started_at=self.TODAY_0730)

    @pytest.mark.asyncio
    async def test_a_copy_two_hours_old_makes_d8_unknown(self, pool):
        async with scenario(pool) as conn:
            await self._clean_mirror_landing_run(conn)
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(hours=2))
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "UNKNOWN" and "120 min old" in detail, detail

    @pytest.mark.asyncio
    async def test_a_failing_copy_makes_d8_unknown(self, pool):
        async with scenario(pool) as conn:
            await self._clean_mirror_landing_run(conn)
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(minutes=70),
                               attempted_at=ago(minutes=10), failures=1, error="boom")
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "UNKNOWN" and "failing" in detail, detail

    @pytest.mark.asyncio
    async def test_a_copy_two_hours_old_makes_the_history_unknown(self, pool):
        async with scenario(pool) as conn:
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(hours=2))
            v, detail = await verdict(conn, "20_reconciliation_pg_history.sql")
        assert v == "UNKNOWN", detail

    @pytest.mark.asyncio
    async def test_a_copy_two_hours_old_makes_pairing_unknown(self, pool):
        async with scenario(pool) as conn:
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(hours=2))
            v, detail = await verdict(conn, "21_pairing.sql", dq_pg_warehouse_on="1")
        assert v == "UNKNOWN", detail

    @pytest.mark.asyncio
    async def test_no_copy_row_at_all_is_unknown(self, pool):
        async with scenario(pool) as conn:
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = 'app.data_quality_runs'")
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "UNKNOWN", detail

    @pytest.mark.asyncio
    async def test_a_fresh_copy_with_a_clean_run_passes_d8(self, pool):
        async with scenario(pool) as conn:
            await self._clean_mirror_landing_run(conn)
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(minutes=10))
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_a_fresh_copy_with_a_finding_fails_d8(self, pool):
        async with scenario(pool) as conn:
            await self._clean_mirror_landing_run(conn)
            await dq_issue(conn, DQ_RUN_IDS[0], "gold_cell_values", table="gold.daily_revenue")
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(minutes=10))
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "FAIL" and "gold_cell_values" in detail, detail

    @pytest.mark.asyncio
    async def test_a_fresh_copy_taken_before_the_run_is_unknown(self, pool):
        """Copied at 07:40: the 07:30 run cannot be in it yet."""
        at_0800 = datetime(2030, 6, 5, 8, 0, tzinfo=KYIV)
        async with scenario(pool) as conn:
            await self._clean_mirror_landing_run(conn)
            await mirror_state(conn, "app.data_quality_runs",
                               ok_at=datetime(2030, 6, 5, 7, 40, tzinfo=KYIV))
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql", now=at_0800)
        assert v == "UNKNOWN", detail


@needs_pg
class TestPairing:
    FILE = "21_pairing.sql"

    async def _fresh_integrity_run(self, conn):
        await mirror_state(conn, "app.data_quality_runs", ok_at=ago(minutes=10))
        await dq_run(conn, DQ_RUN_IDS[1], layer="integrity", started_at=ago(hours=5))

    @pytest.mark.asyncio
    async def test_twin_line_items_absent_while_duckdb_looked_passes(self, pool):
        async with scenario(pool) as conn:
            await self._fresh_integrity_run(conn)
            await dq_issue(conn, DQ_RUN_IDS[1], "headline_vs_line_items")
            v, detail = await verdict(conn, self.FILE, dq_pg_warehouse_on="1")
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_line_items_disagreeing_fails(self, pool):
        async with scenario(pool) as conn:
            await self._fresh_integrity_run(conn)
            await dq_issue(conn, DQ_RUN_IDS[1], "headline_vs_line_items")
            await dq_issue(conn, DQ_RUN_IDS[1], "pg_line_items_disagree")
            v, detail = await verdict(conn, self.FILE, dq_pg_warehouse_on="1")
        assert v == "FAIL" and "pg_line_items_disagree" in detail, detail

    @pytest.mark.asyncio
    async def test_landing_twins_absent_while_duckdb_looked_passes(self, pool):
        """DN-23: DuckDB's landing findings stand; the twins compared."""
        async with scenario(pool) as conn:
            await self._fresh_integrity_run(conn)
            await dq_issue(conn, DQ_RUN_IDS[1], "orders_without_line_items", count=40)
            await dq_issue(conn, DQ_RUN_IDS[1], "value_domain_orders_status_id")
            v, detail = await verdict(conn, self.FILE, dq_pg_warehouse_on="1")
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_order_landing_disagreeing_fails(self, pool):
        async with scenario(pool) as conn:
            await self._fresh_integrity_run(conn)
            await dq_issue(conn, DQ_RUN_IDS[1], "pg_order_landing_disagree")
            v, detail = await verdict(conn, self.FILE, dq_pg_warehouse_on="1")
        assert v == "FAIL" and "pg_order_landing_disagree" in detail, detail

    @pytest.mark.asyncio
    async def test_order_landing_unwatched_fails(self, pool):
        async with scenario(pool) as conn:
            await self._fresh_integrity_run(conn)
            await dq_issue(conn, DQ_RUN_IDS[1], "pg_order_landing_unwatched")
            v, detail = await verdict(conn, self.FILE, dq_pg_warehouse_on="1")
        assert v == "FAIL" and "pg_order_landing_unwatched" in detail, detail

    @pytest.mark.asyncio
    async def test_a_duckdb_finding_without_its_twin_fails(self, pool):
        async with scenario(pool) as conn:
            await self._fresh_integrity_run(conn)
            await dq_issue(conn, DQ_RUN_IDS[1], "silver_missing_rows", count=3)
            v, detail = await verdict(conn, self.FILE, dq_pg_warehouse_on="1")
        assert v == "FAIL" and "pg_silver_missing_rows absent" in detail, detail

    @pytest.mark.asyncio
    async def test_the_flag_off_is_not_applicable(self, pool):
        async with scenario(pool) as conn:
            await self._fresh_integrity_run(conn)
            await dq_issue(conn, DQ_RUN_IDS[1], "silver_missing_rows", count=3)
            v, detail = await verdict(conn, self.FILE, dq_pg_warehouse_on="0")
        assert v == "PASS" and detail.startswith("not applicable"), detail


@needs_pg
class TestReconciliationPgHistory:
    """Would the canary have paged on this history? It pages when the newest
    successful run is more than 30 h old, so the check measures silences as
    well as calendar days, and counts only the runs that could end one.

    Every scenario seeds a run on the day before the window opens, so no row
    another test committed can be the one that opens its first silence."""

    FILE = "20_reconciliation_pg_history.sql"
    FIRST_RUN_ID = 990_000_200

    @staticmethod
    def at(days_ago: int, hour: int = 5, minute: int = 30) -> datetime:
        day = NOW.date() - timedelta(days=days_ago)
        return datetime(day.year, day.month, day.day, hour, minute, tzinfo=KYIV)

    def every_morning(self, *, first: int = 15, last: int = 0, moved=None) -> list:
        """05:30 on each day from `first` days ago to `last`; `moved` maps a day
        to another start, or to None for no run at all."""
        moved = moved or {}
        starts = [moved.get(d, self.at(d)) for d in range(first, last - 1, -1)]
        return [s for s in starts if s is not None]

    async def seed(self, conn, starts, *, failed=(), copied=None):
        """A reconciliation_pg run per start, in order, `failed` ones errored,
        and a copy of the journal taken at `copied` (ten minutes ago)."""
        for n, started_at in enumerate(sorted(set(starts) | set(failed))):
            await dq_run(conn, self.FIRST_RUN_ID + n, layer="reconciliation_pg",
                         started_at=started_at,
                         error="boom" if started_at in failed else None)
        await mirror_state(conn, "app.data_quality_runs",
                           ok_at=copied or ago(minutes=10))

    @pytest.mark.asyncio
    async def test_a_run_every_morning_passes(self, pool):
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning())
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail
        assert detail.startswith("15 successful run(s) since 22.05 00:00 Kyiv"), detail
        assert "the longest silence 24.0 h, the last success 6.5 h ago" in detail, detail

    @pytest.mark.asyncio
    async def test_a_failed_run_is_not_counted(self, pool):
        """The figure is what could have ended a silence; an errored run checked
        nothing and wrote a row anyway."""
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(),
                            failed=[self.at(3, 6, 0), self.at(0, 6, 0)])
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail
        assert detail.startswith("15 successful run(s)"), detail

    @pytest.mark.asyncio
    async def test_a_silence_past_the_limit_fails_though_every_day_has_a_run(self, pool):
        """05:30 one day, 12:00 the next: 30.5 h in which the canary pages, and a
        count of calendar days never notices."""
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(moved={5: self.at(5, 12, 0)}))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "no successful run on" not in detail, detail
        assert "silent for 30.5 h, from 30.05 05:30 to 31.05 12:00 Kyiv" in detail, detail

    @pytest.mark.asyncio
    async def test_a_silence_inside_the_limit_passes(self, pool):
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(moved={5: self.at(5, 11, 0)}))
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail
        assert "the longest silence 29.5 h" in detail, detail

    @pytest.mark.asyncio
    async def test_a_silence_of_exactly_the_limit_passes(self, pool):
        """05:30 one day, 11:30 the next: 30.0 h. The canary pages on
        `age > limit`, so on this history it stayed quiet, and a check that
        failed here would restart DN-21's fourteen days over a page that never
        went out."""
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(moved={5: self.at(5, 11, 30)}))
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail
        assert "the longest silence 30.0 h" in detail, detail
        assert "silent for" not in detail, detail

    @pytest.mark.asyncio
    async def test_a_failed_run_does_not_end_a_silence(self, pool):
        """The canary's age is taken over successful runs, so the 05:30 run that
        errored leaves the 30.5 h standing."""
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(moved={5: self.at(5, 12, 0)}),
                            failed=[self.at(5)])
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "silent for 30.5 h" in detail, detail

    @pytest.mark.asyncio
    async def test_a_silence_lasts_until_the_next_run_is_written(self, pool):
        """Started 29.5 h after the last, written 45 minutes later: /api/health
        showed the old age until then, so the silence was 30.3 h."""
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(moved={5: self.at(5, 11, 0)}))
            await conn.execute(
                "UPDATE app.data_quality_runs SET ended_at = started_at + interval '45 minutes'"
                " WHERE layer = 'reconciliation_pg' AND started_at = $1", self.at(5, 11, 0))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "silent for 30.3 h, from 30.05 05:30 to 31.05 11:45 Kyiv" in detail, detail

    @pytest.mark.asyncio
    async def test_a_silence_starts_when_the_last_success_started(self, pool):
        """The 05:30 run was written at 06:15 and the next started 30 h 20 min
        after it. /api/health's age counts from `started_at`, so the canary
        paged from 11:30; measured from when the run was written the gap would
        read 29.6 h and pass."""
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(moved={5: self.at(5, 11, 50)}))
            await conn.execute(
                "UPDATE app.data_quality_runs SET ended_at = started_at + interval '45 minutes'"
                " WHERE layer = 'reconciliation_pg' AND started_at = $1", self.at(6))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "silent for 30.3 h, from 30.05 05:30 to 31.05 11:50 Kyiv" in detail, detail

    @pytest.mark.asyncio
    async def test_the_silence_the_window_opens_in_is_measured_whole(self, pool):
        """The last run before the window was three days before it: the canary
        was already paging when the window opened."""
        async with scenario(pool) as conn:
            await self.seed(conn, [self.at(17)] + self.every_morning(first=14))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "silent for 72.0 h, from 19.05 05:30 to 22.05 05:30 Kyiv" in detail, detail

    @pytest.mark.asyncio
    async def test_a_day_without_a_run_fails_though_no_silence_is_long(self, pool):
        """Runs drifting 29.5 h apart step over 31.05 without the canary ever
        paging, so the calendar question is asked as well."""
        drift = {8: self.at(8, 11, 0), 7: self.at(7, 16, 30), 6: self.at(6, 22, 0),
                 5: None, 4: self.at(4, 3, 30)}
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(moved=drift))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "no successful run on 31.05" in detail, detail
        assert "silent for" not in detail and "the longest silence 29.5 h" in detail, detail

    @pytest.mark.asyncio
    async def test_a_day_whose_only_run_failed_is_a_day_without_one(self, pool):
        """The same drift, with the stepped-over day's 05:30 run on record and
        errored. It wrote a row and checked nothing, so 31.05 still has no
        successful run."""
        drift = {8: self.at(8, 11, 0), 7: self.at(7, 16, 30), 6: self.at(6, 22, 0),
                 5: None, 4: self.at(4, 3, 30)}
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(moved=drift), failed=[self.at(5)])
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "no successful run on 31.05" in detail, detail
        assert "silent for" not in detail, detail

    @pytest.mark.asyncio
    async def test_no_run_by_noon_fails(self, pool):
        """Yesterday's 05:30 was the last, and the copy taken at 11:50 holds
        nothing since: 30.3 h, past the limit before the copy was even taken.
        Today needs no run by the calendar; it needs one by the clock."""
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(last=1), copied=ago(minutes=10))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "no successful run on" not in detail, detail
        assert "no success since 04.06 05:30 Kyiv, 30.3 h before the copy was taken" in detail, detail

    @pytest.mark.asyncio
    async def test_a_copy_taken_before_the_limit_is_unknown_after_it(self, pool):
        """Copied at 11:00, 29.5 h after the last run: at noon the silence is past
        the limit unless a run landed after 11:00, which the copy cannot show."""
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(last=1), copied=ago(hours=1))
            v, detail = await verdict(conn, self.FILE)
        assert v == "UNKNOWN", detail
        assert "the copy taken at 05.06 11:00 Kyiv cannot say whether a run landed since" in detail, detail

    @pytest.mark.asyncio
    async def test_a_critical_run_fails(self, pool):
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning())
            await conn.execute(
                "UPDATE app.data_quality_runs SET critical_count = 2"
                " WHERE layer = 'reconciliation_pg' AND started_at = $1", self.at(10))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "CRITICAL on 26.05" in detail, detail

    @pytest.mark.asyncio
    async def test_a_stale_copy_of_a_clean_history_is_unknown(self, pool):
        """A run every morning, and a copy two hours old. This is the history a
        frozen replication would certify: without the staleness rule it reads
        PASS, where the empty journal in TestStaleEvidenceIsUnknown would at
        least have read FAIL."""
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(), copied=ago(hours=2))
            v, detail = await verdict(conn, self.FILE)
        assert v == "UNKNOWN", detail
        assert "is 120 min old (limit 75)" in detail, detail


@needs_pg
class TestE1ExpensesStoodDown:
    FILE = "12_e1_expenses_stood_down.sql"

    @pytest.mark.asyncio
    async def test_a_copy_that_wrote_the_table_this_hour_fails(self, pool):
        async with scenario(pool) as conn:
            await mirror_state(conn, "app.manual_expenses", ok_at=ago(minutes=20), rows=0)
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail

    @pytest.mark.asyncio
    async def test_a_copy_that_last_wrote_it_long_ago_passes(self, pool):
        async with scenario(pool) as conn:
            await mirror_state(conn, "app.manual_expenses", ok_at=ago(days=3), rows=0)
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail
