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
VARIABLES = {"inventory_on": "0", "inventory_flip_at": "", "dq_pg_warehouse_on": "0",
             "buyers_on": "0", "buyers_flip_at": "", "buyers_held_by": "",
             "buyers_override_floor": "",
             # Stage 5's clocks (OD-17 (a)); tests/integration/test_stage5_soak_sql.py.
             "duckdb_off": "0", "parallel_from": "", "duckdb_file_last": "none",
             "duckdb_file_since": "", "duckdb_file_since_reason": "",
             "duckdb_file_checked_at": ""}
RUN_AS_OWNER = "-- soak:run-as ks_app"
# The two histories the canary's 30 h watch rests on: DN-21's and OD-08's.
HISTORY_CHECKS = (
    ("20_reconciliation_pg_history.sql", "reconciliation_pg"),
    ("22_reconciliation_ch_history.sql", "reconciliation_ch"),
)

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

    @pytest.mark.parametrize("name,layer", HISTORY_CHECKS)
    def test_the_history_check_measures_against_the_canarys_limit(self, name, layer):
        """20 and 22 ask whether the canary would have paged, so the limit is
        the canary's: a copy that drifted would certify a history the page
        judges differently."""
        from bot.canary import DQ_MAX_AGE_S

        sql = _uncommented((SQL_DIR / name).read_text(encoding="utf-8"))
        found = re.findall(r"interval\s+'(\d+)\s+hours'\s+AS\s+canary_max_age", sql)
        assert len(found) == 1, found
        assert int(found[0]) * 3600 == DQ_MAX_AGE_S[layer]
        assert re.findall(r"r\.layer = '(\w+)'", sql) == [layer]

    def test_the_two_history_checks_differ_only_in_what_a_success_is(self):
        """22 is 20 for the ClickHouse arm (OD-08 review): one body, so a fix
        to the silence arithmetic in one cannot leave the other behind.
        `layer_runs` is the one CTE allowed to differ, and the label the one
        literal. Literals are compared, not blanked: the limits live in them.
        Mutation: change `'75 minutes'` in either file and this fails."""
        def without_layer_runs(name, layer):
            code = _uncommented((SQL_DIR / name).read_text(encoding="utf-8"))
            label = f"'{layer} history'"
            assert code.count(label) == 1, name
            code = code.replace(label, "'<layer> history'")
            start = code.index("layer_runs AS (")
            depth, i = 0, code.index("(", start)
            while True:
                depth += {"(": 1, ")": -1}.get(code[i], 0)
                i += 1
                if depth == 0:
                    break
            assert code[i] == ",", code[i:i + 20]
            return re.sub(r"\s+", " ", code[:start] + code[i + 1:]).strip()

        assert without_layer_runs(*HISTORY_CHECKS[0]) == \
            without_layer_runs(*HISTORY_CHECKS[1])

    def test_d8s_blind_checks_are_every_warn_the_clickhouse_comparison_files(self):
        """D8 reads a finding that only says ClickHouse did not compare as
        UNKNOWN, not as a disagreement (OD-08 review). The list is derived:
        `gold_values_unwatched` and every WARN `reconcile_clickhouse` files
        itself — each is ClickHouse saying it could not look."""
        import ast
        import inspect

        from core import ch_silver

        tree = ast.parse(inspect.getsource(ch_silver.reconcile_clickhouse))
        warns = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                kws = {k.arg: k.value for k in node.keywords}
                name, sev = kws.get("check_name"), kws.get("severity")
                if (isinstance(name, ast.Constant) and isinstance(sev, ast.Attribute)
                        and sev.attr == "WARN"):
                    warns.add(name.value)
        sql = _uncommented((SQL_DIR / "09_d8_mirror_landing.sql").read_text(encoding="utf-8"))
        block = re.search(r"blind_checks AS \((.*?)\n\),", sql, flags=re.S).group(1)
        assert set(re.findall(r"'(\w+)'", block)) == warns | {ch_silver.GOLD_UNWATCHED}

    def test_the_read_fallback_check_reads_what_the_canary_writes(self):
        """F1 judges two things the canary makes, so every name and number it
        shares with the canary is the canary's: the watch row's key and gap,
        the pages' keys — every key a check emits for a read served from
        DuckDB, in both places it asks — and the words both lines begin
        with, which the line is read back out of. The day is the report's,
        the week OD-07's 168 h."""
        import os.path

        from bot import canary

        sql = (SQL_DIR / "22_f1_read_fallbacks.sql").read_text(encoding="utf-8")
        code = _uncommented(sql)
        gap = re.findall(r"interval\s+'(\d+)\s+minutes'\s+AS\s+watch_gap", code)
        assert [int(m) * 60 for m in gap] == [canary.READ_FALLBACK_WATCH_GAP_S]
        assert re.findall(r"interval\s+'(\d+)\s+hours'\s+AS\s+span", code) == ["24"]
        assert re.findall(r"interval\s+'(\d+)\s+hours'\s+AS\s+week", code) == ["168"]
        assert re.findall(r"condition_key = '(watch:[^']*)'", code) == [
            canary.READ_FALLBACK_WATCH_KEY]
        served = {k for k, _ in canary.check_read_fallbacks(
            {"read_fallbacks": {"a": {}}})}
        served |= {k for k, _ in canary.check_read_routes(
            {"read_fallback_mode": {"mode": "duckdb", "misconfigured": ["x"]}})}
        lists = re.findall(r"condition_key IN \(([^)]*)\)", code)
        assert len(lists) == 2, lists
        for found in lists:
            assert set(re.findall(r"'([^']*)'", found)) == served
        [stem] = re.findall(r"substring\(m\.message FROM '\((reads [^\[]*)\[", code)
        common = os.path.commonprefix([canary.READ_FALLBACK_LINE, canary.READ_ROUTED_LINE])
        assert stem == common.rstrip() == "reads served from DuckDB"


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
        {"inventory_on": "0", "dq_pg_warehouse_on": "0", "buyers_on": "0"},
        {"inventory_on": "1", "dq_pg_warehouse_on": "1",
         "inventory_flip_at": "2030-06-01 10:00+03",
         "buyers_on": "1", "buyers_flip_at": "2030-06-01 10:30+03",
         "buyers_override_floor": "0"},
        {"inventory_on": "1", "dq_pg_warehouse_on": "1", "buyers_on": "1"},
        {"inventory_on": "invalid", "dq_pg_warehouse_on": "invalid", "buyers_on": "invalid"},
        {"inventory_on": "unknown", "dq_pg_warehouse_on": "unknown", "buyers_on": "unknown"},
        {"inventory_on": "0", "dq_pg_warehouse_on": "0", "buyers_on": "held",
         "buyers_held_by": "KS_SMS_STORE"},
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
    async def test_buyer_checks_are_not_applicable_while_the_chain_is_off(self, pool):
        for path in FILES:
            if not path.name[3:].startswith("b"):
                continue
            async with scenario(pool) as conn:
                v, detail = await verdict(conn, path.name, buyers_on="0")
            assert (v, detail.startswith("not applicable")) == ("PASS", True), (path.name, detail)

    @pytest.mark.asyncio
    async def test_a_held_chain_fails_b1_once_and_nothing_else(self, pool):
        """The flip did not move the chain: B1 says so and names the reader;
        B2–B5 have nothing moved to judge."""
        verdicts = {}
        for path in FILES:
            if not path.name[3:].startswith("b"):
                continue
            async with scenario(pool) as conn:
                verdicts[path.name[:2]] = await verdict(
                    conn, path.name, buyers_on="held", buyers_held_by="KS_READ_DASHBOARD")
        assert verdicts.pop("23")[0] == "FAIL"
        assert all(v == "PASS" and "held on DuckDB" in d for v, d in verdicts.values()), verdicts

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
    @pytest.mark.parametrize("name,layer", HISTORY_CHECKS)
    async def test_a_copy_two_hours_old_makes_the_history_unknown(self, pool, name, layer):
        async with scenario(pool) as conn:
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(hours=2))
            v, detail = await verdict(conn, name)
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

    FILE, LAYER = HISTORY_CHECKS[0]
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
        """A run of the layer per start, in order, `failed` ones errored,
        and a copy of the journal taken at `copied` (ten minutes ago)."""
        for n, started_at in enumerate(sorted(set(starts) | set(failed))):
            await dq_run(conn, self.FIRST_RUN_ID + n, layer=self.LAYER,
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
                " WHERE layer = $2 AND started_at = $1", self.at(5, 11, 0), self.LAYER)
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
                " WHERE layer = $2 AND started_at = $1", self.at(6), self.LAYER)
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
                " WHERE layer = $2 AND started_at = $1", self.at(10), self.LAYER)
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
class TestReconciliationChHistory(TestReconciliationPgHistory):
    """Every scenario above, again, for the ClickHouse arm (OD-08 review):
    22 is 20 with one CTE changed, and these prove the body behaves the same
    on the other layer. Then what differs: a run that gated a stale copy."""

    FILE, LAYER = HISTORY_CHECKS[1]

    async def gated(self, conn, started_at, run_id):
        """A run as the arm wrote a stale-copy gate before the review: no
        error, and a WARN `ch_reconcile_pending` beside it."""
        await dq_run(conn, run_id, layer=self.LAYER, started_at=started_at)
        await dq_issue(conn, run_id, "ch_reconcile_pending")

    @pytest.mark.asyncio
    async def test_a_gated_run_does_not_end_a_silence(self, pool):
        """05:30 on 30.05, then gated at 05:30 on 31.05 and nothing until
        12:00: the canary shipped with this check would have paged from 11:30,
        and 31.05 has no run that compared. Mutation: drop the NOT EXISTS
        from 22's `layer_runs` and this reads PASS."""
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(moved={5: self.at(5, 12, 0)}))
            await self.gated(conn, self.at(5), self.FIRST_RUN_ID + 900)
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "silent for 30.5 h, from 30.05 05:30 to 31.05 12:00 Kyiv" in detail, detail

    @pytest.mark.asyncio
    async def test_a_day_whose_only_run_was_gated_has_none(self, pool):
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning(moved={5: None}))
            await self.gated(conn, self.at(5), self.FIRST_RUN_ID + 900)
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "no successful run on 31.05" in detail, detail

    @pytest.mark.asyncio
    async def test_a_gate_finding_on_another_layer_is_not_this_ones(self, pool):
        """The NOT EXISTS reads the run's own findings, not the name anywhere."""
        async with scenario(pool) as conn:
            await self.seed(conn, self.every_morning())
            await dq_run(conn, self.FIRST_RUN_ID + 900, layer="mirror_landing",
                         started_at=self.at(5, 7, 30))
            await dq_issue(conn, self.FIRST_RUN_ID + 900, "ch_reconcile_pending")
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail


@needs_pg
class TestD8ABlindComparisonIsNotADisagreement:
    """OD-08 review, fifth finding: `gold_values_unwatched` is filed on
    `gold.daily_revenue`, which D8 counted as a disagreement between the two
    stores and marked FAIL. It says only that ClickHouse did not compare; the
    soak's rule for a check that could not see is UNKNOWN. Mutation: drop the
    `blind` branch of the verdict and the first two read FAIL."""

    TODAY_0730 = datetime(2030, 6, 5, 7, 30, tzinfo=KYIV)

    async def _run_with(self, conn, *findings):
        await dq_run(conn, DQ_RUN_IDS[0], layer="mirror_landing", started_at=self.TODAY_0730)
        for name, table in findings:
            await dq_issue(conn, DQ_RUN_IDS[0], name, table=table)
        await mirror_state(conn, "app.data_quality_runs", ok_at=ago(minutes=10))

    @pytest.mark.asyncio
    async def test_no_ks_ch_url_is_unknown_not_a_disagreement(self, pool):
        """The reviewer's reproduction: the one finding, on gold.daily_revenue."""
        async with scenario(pool) as conn:
            await self._run_with(conn, ("gold_values_unwatched", "gold.daily_revenue"))
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "UNKNOWN", detail
        assert "ClickHouse did not compare Gold (gold_values_unwatched WARN)" in detail
        assert "finding(s) on the derived tables" not in detail

    @pytest.mark.asyncio
    async def test_a_clickhouse_outage_is_unknown(self, pool):
        """An outage files the WARN about ClickHouse on silver.orders beside it."""
        async with scenario(pool) as conn:
            await self._run_with(conn, ("ch_silver_unreachable", "silver.orders"),
                                 ("gold_values_unwatched", "gold.daily_revenue"))
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "UNKNOWN", detail
        assert "ch_silver_unreachable WARN, gold_values_unwatched WARN" in detail

    @pytest.mark.asyncio
    async def test_a_disagreement_beside_a_blind_spot_still_fails(self, pool):
        async with scenario(pool) as conn:
            await self._run_with(conn, ("gold_cell_values", "gold.daily_revenue"),
                                 ("gold_values_unwatched", "gold.daily_revenue"))
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "FAIL", detail
        assert "1 finding(s) on the derived tables: gold_cell_values" in detail
        assert "gold_values_unwatched" not in detail


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


# ── chain 4 ───────────────────────────────────────────────────────────────────

BUYER_IDS = [990_000_201, 990_000_202, 990_000_203]


async def buyer(conn, bid, *, name="Олена", phone=None, mirrored_at=None, contact=True):
    await conn.execute(
        "INSERT INTO bronze.buyers (id, full_name, phone, mirrored_at) VALUES ($1, $2, $3, $4)",
        bid, name, phone, mirrored_at or ago(hours=3))
    if contact and phone:
        await conn.execute(
            "INSERT INTO bronze.buyer_contacts (buyer_id, contact_type, value, is_primary) "
            "VALUES ($1, 'phone', $2, true)", bid, phone)


async def verdict_row(conn, bid, *, override=False):
    await conn.execute(
        "INSERT INTO app.buyer_gender (buyer_id, gender, method, rules_version, "
        "override_by_human) VALUES ($1, 'f', 'given', 1, $2)", bid, override)


async def owners(conn, at):
    for table in ("bronze.buyers", "bronze.buyer_contacts", "app.buyer_gender"):
        await conn.execute(
            "INSERT INTO meta.chain_watermarks (key, value, updated_at) VALUES ($1, $2, $3) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, "
            "updated_at = EXCLUDED.updated_at", f"owner:{table}", at.isoformat(), at)


async def clean_buyers(conn):
    """The checks read whole tables; a scenario starts from none of their rows."""
    for table in ("bronze.buyer_contacts", "app.buyer_gender", "bronze.buyers",
                  "silver.orders"):
        await conn.execute(f"DELETE FROM {table}")
    await conn.execute("DELETE FROM meta.chain_watermarks "
                       "WHERE key LIKE 'owner:%' OR key = 'last_sync_buyers'")
    await conn.execute("DELETE FROM meta.mirror_state WHERE table_name IN "
                       "('bronze.buyers', 'bronze.buyer_contacts', 'app.buyer_gender')")


async def silver_order(conn, oid, *, buyer_id, ordered_at):
    await conn.execute(
        """
        INSERT INTO silver.orders (id, source_id, status_id, grand_total, ordered_at,
               buyer_id, order_date, is_return, sales_type, is_active_source, source_name)
        VALUES ($1, 1, 1, 100, $2, $3, $4, false, 'retail', true, 'Instagram')
        """, oid, ordered_at, buyer_id, ordered_at.date())


@needs_pg
class TestB1BuyersCopiesStoodDown:
    FILE = "23_b1_buyers_copies_stood_down.sql"

    @pytest.mark.asyncio
    async def test_a_copy_after_the_handover_fails(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await owners(conn, ago(days=2))
            await mirror_state(conn, "app.buyer_gender", ok_at=ago(hours=5))
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "FAIL" and "app.buyer_gender written by a copy" in detail, detail

    @pytest.mark.asyncio
    async def test_a_copy_before_the_handover_passes(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await owners(conn, ago(days=2))
            await mirror_state(conn, "bronze.buyers", ok_at=ago(days=3))
            await mirror_state(conn, "app.buyer_gender", ok_at=ago(days=2, minutes=5))
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "PASS" and "since the handover" in detail, detail

    @pytest.mark.asyncio
    async def test_a_failure_before_the_handover_is_not_the_shippers_now(self, pool):
        """A mirror that failed the day before the flip keeps its count for ever
        — nothing ships again to clear it. Only a failure after is one."""
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await owners(conn, ago(days=2))
            await mirror_state(conn, "bronze.buyers", ok_at=ago(days=4),
                               attempted_at=ago(days=3), failures=2, error="boom")
            v, _ = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "PASS"
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await owners(conn, ago(days=2))
            await mirror_state(conn, "bronze.buyers", ok_at=ago(days=4),
                               attempted_at=ago(hours=1), failures=3, error="boom")
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "FAIL" and "failing (3)" in detail, detail

    @pytest.mark.asyncio
    async def test_without_owner_rows_the_flip_time_decides(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await mirror_state(conn, "bronze.buyers", ok_at=ago(hours=2))
            v, _ = await verdict(conn, self.FILE, buyers_on="1",
                                 buyers_flip_at=ago(hours=3).isoformat())
            assert v == "FAIL"
            v, detail = await verdict(conn, self.FILE, buyers_on="1",
                                      buyers_flip_at=ago(hours=1).isoformat())
        assert v == "PASS" and "since the flip" in detail, detail

    @pytest.mark.asyncio
    async def test_with_neither_owner_row_nor_flip_time_a_recent_stamp_is_unknown(self, pool):
        """The hourly copy stamps app.buyer_gender every run until the flip,
        so a healthy flip reads one inside the window until the first write."""
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await mirror_state(conn, "app.buyer_gender", ok_at=ago(minutes=30))
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "UNKNOWN" and "SOAK_BUYERS_FLIP_AT" in detail, detail

    @pytest.mark.asyncio
    async def test_held_and_invalid_fail_with_their_reason(self, pool):
        async with scenario(pool) as conn:
            v, detail = await verdict(conn, self.FILE, buyers_on="held",
                                      buyers_held_by="KS_SMS_STORE")
            assert v == "FAIL" and "KS_SMS_STORE is not postgres" in detail, detail
            v, detail = await verdict(conn, self.FILE, buyers_on="invalid")
        assert v == "FAIL" and "no chain understands" in detail, detail


@needs_pg
class TestB2BuyersWatermark:
    FILE = "24_b2_buyers_watermark.sql"

    async def mark(self, conn, value):
        await conn.execute(
            "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
            "VALUES ('last_sync_buyers', $1, $2)", value, NOW)

    @pytest.mark.asyncio
    async def test_the_stored_value_is_judged_not_the_rows_stamp(self, pool):
        """`updated_at` is NOW; the value the step wrote is two hours old."""
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await self.mark(conn, ago(hours=2).isoformat())
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "FAIL" and "moved 120 min ago" in detail, detail

    @pytest.mark.asyncio
    async def test_a_fresh_value_passes(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await self.mark(conn, ago(minutes=40).isoformat())
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "PASS" and "moved 40 min ago" in detail, detail

    @pytest.mark.asyncio
    async def test_missing_fails_unless_just_flipped(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            v, _ = await verdict(conn, self.FILE, buyers_on="1")
            assert v == "FAIL"
            v, detail = await verdict(conn, self.FILE, buyers_on="1",
                                      buyers_flip_at=ago(minutes=20).isoformat())
        assert v == "PASS" and "not written yet" in detail, detail

    @pytest.mark.asyncio
    async def test_a_value_that_is_not_a_timestamp_fails_and_does_not_error(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await self.mark(conn, "not a time")
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "FAIL" and "not a timestamp" in detail, detail


@needs_pg
class TestB3BuyersVerdicts:
    FILE = "25_b3_gender_coverage.sql"

    @pytest.mark.asyncio
    async def test_a_buyer_past_the_grace_with_no_verdict_fails(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await buyer(conn, BUYER_IDS[0], mirrored_at=ago(hours=2))
            await buyer(conn, BUYER_IDS[1], mirrored_at=ago(minutes=30))
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "FAIL" and detail.startswith("1 buyer(s)"), detail

    @pytest.mark.asyncio
    async def test_fewer_overrides_than_on_flip_day_fails(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await buyer(conn, BUYER_IDS[0])
            await verdict_row(conn, BUYER_IDS[0], override=True)
            v, _ = await verdict(conn, self.FILE, buyers_on="1", buyers_override_floor="1")
            assert v == "PASS"
            v, detail = await verdict(conn, self.FILE, buyers_on="1",
                                      buyers_override_floor="2")
        assert v == "FAIL" and "against 2 on flip day" in detail, detail


@needs_pg
class TestB4SelectionBacklog:
    FILE = "26_b4_selection_backlog.sql"

    @pytest.mark.asyncio
    async def test_a_buyer_owed_for_a_day_fails(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await silver_order(conn, ORDER_IDS[0], buyer_id=BUYER_IDS[0],
                               ordered_at=ago(hours=30))
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "FAIL" and "1 of them for over a day" in detail, detail

    @pytest.mark.asyncio
    async def test_a_fresh_one_and_a_landed_one_pass(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await silver_order(conn, ORDER_IDS[0], buyer_id=BUYER_IDS[0],
                               ordered_at=ago(hours=1))
            await buyer(conn, BUYER_IDS[1])
            await silver_order(conn, ORDER_IDS[1], buyer_id=BUYER_IDS[1],
                               ordered_at=ago(days=3))
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "PASS" and detail.startswith("1 buyer(s) owed"), detail


@needs_pg
class TestB5BuyersIntegrity:
    FILE = "27_b5_buyers_integrity.sql"

    @pytest.mark.asyncio
    async def test_a_clean_set_passes(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await buyer(conn, BUYER_IDS[0], phone="+380500000001")
            await verdict_row(conn, BUYER_IDS[0])
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_an_empty_phone_is_the_parse_not_a_lost_contact(self, pool):
        """KeyCRM's list starting with '' is stored as '' and gets no contact
        row: the shared parse, reproduced by every rewrite."""
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await buyer(conn, BUYER_IDS[0], phone="", contact=False)
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "PASS", detail

    @pytest.mark.parametrize("defect", ["orphan_verdict", "null_name", "lost_contact"])
    @pytest.mark.asyncio
    async def test_each_defect_fails(self, pool, defect):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            if defect == "orphan_verdict":
                await verdict_row(conn, BUYER_IDS[2])
            elif defect == "null_name":
                await buyer(conn, BUYER_IDS[0], name=None)
            else:
                await buyer(conn, BUYER_IDS[0], phone="+380500000001", contact=False)
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == "FAIL", (defect, detail)


@needs_pg
class TestF1ReadFallbacks:
    """OD-07's evidence, judged a day at a time, from the canary's pages and
    the canary's watch.

    FAIL is a read some page answered from DuckDB in the day — paged,
    escalated, standing (however old), resolved inside it, or read by the
    watch's latest probe even when no page was delivered. UNKNOWN is a day
    nobody can say was watched. PASS needs both, and says how much of the
    168 h week OD-07 waits for the clean run has covered."""

    FILE = "22_f1_read_fallbacks.sql"
    LINE = "reads served from DuckDB: "
    ROUTED = "reads served from DuckDB uncounted: "
    KEYS = ("read_fallback_used", "read_routed_to_duckdb")

    async def clear(self, conn):
        await conn.execute(
            "DELETE FROM app.alert_events WHERE condition_key = ANY($1::text[])",
            list(self.KEYS))
        await conn.execute(
            "DELETE FROM app.alert_series WHERE condition_key = ANY($1::text[])",
            [*self.KEYS, "watch:read_fallbacks"])

    @staticmethod
    async def watch(conn, *, since, last, probes):
        await conn.execute(
            """
            INSERT INTO app.alert_series (condition_key, kind, state, first_fired_at,
                                          last_fired_at, fired_count, instance)
            VALUES ('watch:read_fallbacks', 'event', 'event', $1, $2, $3, 'bot')
            """, since, last, probes)

    @staticmethod
    async def series(conn, *, state, first, last, resolved=None,
                     key="read_fallback_used"):
        await conn.execute(
            """
            INSERT INTO app.alert_series (condition_key, kind, state, first_fired_at,
                                          last_fired_at, fired_count, resolved_at,
                                          instance)
            VALUES ($5, 'condition', $1, $2, $3, 1, $4, 'bot')
            """, state, first, last, resolved, key)

    @staticmethod
    async def event(conn, key, *, at, event_type="fired", message=None):
        await conn.execute(
            """
            INSERT INTO app.alert_events (condition_key, event_type, at, instance,
                                          delivered_to, message)
            VALUES ($1, $2, $3, 'bot', 1, $4)
            """, key, event_type, at, message)

    async def clean_week(self, conn):
        await self.watch(conn, since=ago(hours=169), last=ago(minutes=10), probes=676)

    @pytest.mark.asyncio
    async def test_a_watched_clean_week_passes_and_says_it_is_covered(self, pool):
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.clean_week(conn)
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail
        assert "since 29.05 11:00 Kyiv (676 probes)" in detail, detail
        assert "the 168 h KS_READ_FALLBACK=off waits for are covered" in detail, detail

    @pytest.mark.asyncio
    async def test_a_watched_clean_day_passes_and_counts_the_week(self, pool):
        """The day after this ships, and every day of the week after a reset:
        nothing wrong, the canary watching — a PASS, with the week's progress
        in the detail rather than an UNKNOWN the daily report would carry for
        seven days (review of OD-07, finding 5)."""
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.watch(conn, since=ago(hours=30), last=ago(minutes=10), probes=120)
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail
        assert "30 h of the 168 KS_READ_FALLBACK=off waits for" in detail, detail
        assert "covered" not in detail, detail

    @pytest.mark.asyncio
    async def test_the_week_is_covered_from_its_168th_hour(self, pool):
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.watch(conn, since=ago(hours=168), last=ago(minutes=10), probes=672)
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS" and "are covered" in detail, detail
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.watch(conn, since=ago(hours=168) + timedelta(seconds=1),
                             last=ago(minutes=10), probes=671)
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS" and "167 h of the 168" in detail, detail

    @pytest.mark.asyncio
    async def test_no_watch_is_unknown_and_says_why(self, pool):
        """No page and no watch is the state production is in the day this
        ships: a quiet journal proves nothing without somebody looking."""
        async with scenario(pool) as conn:
            await self.clear(conn)
            v, detail = await verdict(conn, self.FILE)
        assert v == "UNKNOWN", detail
        assert "nothing durable says the canary read" in detail, detail

    @pytest.mark.asyncio
    async def test_a_watch_nobody_wrote_for_the_gap_is_unknown(self, pool):
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.watch(conn, since=ago(hours=200), last=ago(minutes=36), probes=600)
            v, detail = await verdict(conn, self.FILE)
        assert v == "UNKNOWN", detail
        assert "36 min ago (limit 35)" in detail, detail

    @pytest.mark.asyncio
    async def test_the_staleness_limit_is_exact(self, pool):
        """35 min to the second is alive; one second more is not. The pin on
        the number alone let a comparison shifted by under a minute through
        (review of OD-07, finding 6)."""
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.watch(conn, since=ago(hours=200), last=ago(minutes=35), probes=600)
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.watch(conn, since=ago(hours=200), last=ago(minutes=35, seconds=1),
                             probes=600)
            v, detail = await verdict(conn, self.FILE)
        assert v == "UNKNOWN", detail

    @pytest.mark.asyncio
    async def test_a_watch_younger_than_the_day_is_unknown(self, pool):
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.watch(conn, since=ago(hours=10), last=ago(minutes=10), probes=40)
            v, detail = await verdict(conn, self.FILE)
        assert v == "UNKNOWN", detail
        assert "only since 05.06 02:00 Kyiv, 10.0 h of the 24" in detail, detail

    @pytest.mark.asyncio
    async def test_a_page_inside_the_day_fails_naming_its_surfaces(self, pool):
        """The body rides the first condition of a raise; this page shared
        its raise with a mirror page, so the line is found on the sibling."""
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.clean_week(conn)
            at = ago(hours=20)
            await self.series(conn, state="resolved", first=at, last=at,
                              resolved=ago(hours=19))
            await self.event(conn, "mirror_failing:bronze.orders", at=at, message=(
                "⚠️ <b>Dashboard warning</b>\n• mirror bronze.orders: failing, 4× in a row\n"
                f"• {self.LINE}dashboard ×2 (last 2030-06-04T12:55:00+00:00)\n→ Check it"))
            await self.event(conn, "read_fallback_used", at=at)
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert ("paged 1 time(s) in 24 h, last 04.06 16:00 Kyiv: reads served from "
                "DuckDB: dashboard ×2 (last 2030-06-04T12:55:00+00:00)") in detail, detail

    @pytest.mark.asyncio
    async def test_a_route_to_duckdb_paged_inside_the_day_fails_naming_it(self, pool):
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.clean_week(conn)
            at = ago(hours=3)
            await self.series(conn, state="resolved", first=at, last=at,
                              resolved=ago(hours=2), key="read_routed_to_duckdb")
            await self.event(conn, "read_routed_to_duckdb", at=at, message=(
                f"⚠️ <b>Dashboard warning</b>\n• {self.ROUTED}"
                "KS_READ_COHORTS=clickhouse without KS_CH_URL\n→ Give web"))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert ("reads served from DuckDB uncounted: KS_READ_COHORTS=clickhouse "
                "without KS_CH_URL") in detail, detail

    @pytest.mark.asyncio
    async def test_a_route_still_paging_fails(self, pool):
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.clean_week(conn)
            await self.series(conn, state="firing", first=ago(days=3), last=ago(days=3),
                              key="read_routed_to_duckdb")
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "still paging (last paged 02.06 12:00 Kyiv)" in detail, detail

    @pytest.mark.asyncio
    async def test_a_route_no_page_reached_fails_through_the_watch(self, pool):
        """The review's case end to end: web publishing a switch with no
        address, the canary reading it, the real statement writing the watch
        — and F1, which used to PASS over it."""
        from bot import canary
        from core.alert_archive import _WATCH_SQL

        payload = {"read_fallbacks": {},
                   "read_fallback_mode": {"mode": "duckdb", "error": None,
                                          "misconfigured": [
                                              "KS_READ_COHORTS=clickhouse without KS_CH_URL"],
                                          "no_engine": []}}
        async with scenario(pool) as conn:
            await self.clear(conn)
            await conn.execute(
                """
                INSERT INTO app.alert_series (condition_key, kind, state, first_fired_at,
                                              last_fired_at, fired_count, instance)
                VALUES ('watch:read_fallbacks', 'event', 'event',
                        now() - interval '170 hours', now() - interval '14 minutes',
                        680, 'bot')
                """)
            await conn.execute(_WATCH_SQL, canary.READ_FALLBACK_WATCH_KEY,
                               canary.read_fallbacks_clean(payload),
                               float(canary.READ_FALLBACK_WATCH_GAP_S), "bot", 3600.0)
            v, detail = await verdict(conn, self.FILE, now=None)
        assert v == "FAIL", detail
        assert "found a read served from DuckDB" in detail, detail

    @pytest.mark.asyncio
    async def test_a_fallback_a_day_old_is_the_week_restarting_not_a_fail(self, pool):
        """One fallback is one day's FAIL, not seven: a page 30 h ago, web
        restarted an hour later, the watch clean since — the daily question
        passes, and the week counts from the restart."""
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.watch(conn, since=ago(hours=29), last=ago(minutes=10), probes=116)
            await self.series(conn, state="resolved", first=ago(hours=30),
                              last=ago(hours=30), resolved=ago(hours=29))
            await self.event(conn, "read_fallback_used", at=ago(hours=30),
                             message=f"• {self.LINE}goals ×1")
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail
        assert "29 h of the 168" in detail, detail

    @pytest.mark.asyncio
    async def test_an_escalation_inside_the_day_fails(self, pool):
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.clean_week(conn)
            await self.series(conn, state="resolved", first=ago(hours=40),
                              last=ago(hours=40), resolved=ago(hours=30))
            await self.event(conn, "read_fallback_used", at=ago(hours=40),
                             message=f"• {self.LINE}goals ×1")
            await self.event(conn, "read_fallback_used", at=ago(hours=23),
                             event_type="escalated")
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "paged 1 time(s)" in detail, detail

    @pytest.mark.asyncio
    async def test_a_page_still_standing_fails_however_old(self, pool):
        """Paged ten days ago, the reminders never delivered: a web process
        that has not restarted still holds that fallback."""
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.clean_week(conn)
            await self.series(conn, state="firing", first=ago(days=10), last=ago(days=10))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "still paging (last paged 26.05 12:00 Kyiv)" in detail, detail

    @pytest.mark.asyncio
    async def test_a_page_that_stood_into_the_day_fails(self, pool):
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.clean_week(conn)
            await self.series(conn, state="resolved", first=ago(days=9),
                              last=ago(days=9), resolved=ago(hours=10))
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "stood until 05.06 02:00 Kyiv, inside the window" in detail, detail

    @pytest.mark.asyncio
    async def test_a_fallback_no_page_reached_still_fails(self, pool):
        """A page is journaled only when delivered. The watch's latest probe
        read the fallback anyway, and that is a fallback."""
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.watch(conn, since=ago(minutes=10), last=ago(minutes=10), probes=0)
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        assert "the canary's probe at 05.06 11:50 Kyiv found a read served" in detail, detail

    @pytest.mark.asyncio
    async def test_a_dirty_probe_counts_inside_the_day_only(self, pool):
        """The watch's latest probe read a fallback two hours ago and nothing
        has read since: a FAIL, inside the day. The same probe before the day
        began is a watch nobody wrote: UNKNOWN — the pin the review's
        mutation removed survived because nothing sat on that side."""
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.watch(conn, since=ago(hours=2), last=ago(hours=2), probes=0)
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.watch(conn, since=ago(hours=30), last=ago(hours=30), probes=0)
            v, detail = await verdict(conn, self.FILE)
        assert v == "UNKNOWN", detail
        assert "min ago (limit 35)" in detail, detail

    @pytest.mark.asyncio
    async def test_a_fallback_before_the_day_does_not_count(self, pool):
        async with scenario(pool) as conn:
            await self.clear(conn)
            await self.clean_week(conn)
            await self.series(conn, state="resolved", first=ago(days=9),
                              last=ago(days=9), resolved=ago(hours=25))
            await self.event(conn, "read_fallback_used", at=ago(days=9),
                             message=f"• {self.LINE}goals ×1")
            await self.event(conn, "read_fallback_mode_invalid", at=ago(hours=5),
                             message="• read fallback: KS_READ_FALLBACK='of'")
            v, detail = await verdict(conn, self.FILE)
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_the_real_writer_and_the_real_clock_agree(self, pool):
        """The watch as `core.alert_archive` writes it, judged on the real
        clock: a run seeded 170 h long, then one probe through the statement
        the bot runs, of the same web process."""
        from bot.canary import READ_FALLBACK_WATCH_GAP_S, READ_FALLBACK_WATCH_KEY
        from core.alert_archive import _WATCH_SQL

        async with scenario(pool) as conn:
            await self.clear(conn)
            await conn.execute(
                """
                INSERT INTO app.alert_series (condition_key, kind, state, first_fired_at,
                                              last_fired_at, fired_count, instance)
                VALUES ('watch:read_fallbacks', 'event', 'event',
                        now() - interval '170 hours', now() - interval '14 minutes',
                        680, 'bot')
                """)
            await conn.execute(_WATCH_SQL, READ_FALLBACK_WATCH_KEY, True,
                               float(READ_FALLBACK_WATCH_GAP_S), "bot", 86400.0)
            v, detail = await verdict(conn, self.FILE, now=None)
        assert v == "PASS", detail
        assert "(681 probes)" in detail and "are covered" in detail, detail
