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
VARIABLES = {"inventory_on": "0", "inventory_flip_at": "",
             "buyers_on": "0", "buyers_flip_at": "",
             "buyers_override_floor": "", "dq_journal_direct": "0",
             # The DuckDB file (P1, OD-17 (a)); tests/integration/test_stage5_soak_sql.py.
             "duckdb_off": "0", "duckdb_file_last": "none",
             "duckdb_file_since": "", "duckdb_file_since_reason": "",
             "duckdb_file_checked_at": "", "duckdb_file_missing_at": ""}
# Chain 5's one (31_m2; M1 went in step 14's S14-13).
VARIABLES.update({"managers_on": "0", "managers_flip_at": ""})
# Chain 3's four (33_o2 … 36_o5; O1 went in step 14's S14-13, and with it
# `markers_read_at`, which only O1 and M1 read, and the three shadow chains'
# states, which only H1 read).
VARIABLES.update({"orders_on": "0", "orders_flip_at": ""})
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

    def test_d8s_named_checks_are_filed_by_the_code_that_stays(self):
        """Step 14's S14-12 deleted the comparisons against DuckDB, and with
        them the names D8 listed for them (`mirror_buckets_disagree`,
        `gold_missing_cells`, `gold_orphan_cells`). Every name D8 still lists
        by name is a registered condition. Mutation: list one with no
        emitter."""
        from core.alerting import REGISTRY

        sql = _uncommented((SQL_DIR / "09_d8_mirror_landing.sql").read_text(encoding="utf-8"))
        block = re.search(r"derived AS \((.*?)\n\),", sql, flags=re.S).group(1)
        named = set(re.findall(r"'(\w+)'", block.split("i.check_name IN", 1)[1]))
        assert named == {"gold_rollup_mismatch", "customer_profile_mismatch"}
        assert named <= set(REGISTRY)

    def test_d8s_grace_checks_are_the_info_the_utm_completeness_files(self):
        """D8 forgives a UTM verdict still inside its grace (the 07:30 run
        files one nearly every morning since KS_UTM_PARSE=postgres). Derived,
        not remembered: exactly the INFO checks `order_utm_completeness_findings`
        files, so its CRITICALs past the grace can never join the list."""
        import ast
        import inspect

        from core import mirror_reconciliation

        tree = ast.parse(inspect.getsource(
            mirror_reconciliation.order_utm_completeness_findings))
        by_severity = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                kws = {k.arg: k.value for k in node.keywords}
                name, sev = kws.get("check_name"), kws.get("severity")
                if isinstance(name, ast.Constant) and isinstance(sev, ast.Attribute):
                    by_severity.setdefault(sev.attr, set()).add(name.value)
        assert by_severity.get("CRITICAL"), "the walk found no CRITICAL: it reads nothing"
        sql = _uncommented((SQL_DIR / "09_d8_mirror_landing.sql").read_text(encoding="utf-8"))
        block = re.search(r"grace_checks AS \((.*?)\n\),", sql, flags=re.S).group(1)
        assert set(re.findall(r"'(\w+)'", block)) == by_severity["INFO"]
        filed_above_info = set().union(*(v for k, v in by_severity.items() if k != "INFO"))
        assert not by_severity["INFO"] & filed_above_info


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


async def dq_issue(conn, run_id, check_name, *, table="silver.orders", count=1,
                   severity="WARN"):
    await conn.execute(
        """
        INSERT INTO app.data_quality_issues (run_id, check_name, table_name, severity, count)
        VALUES ($1, $2, $3, $5, $4)
        """, run_id, check_name, table, count, severity)


@needs_pg
class TestEveryCheckRuns:
    VARIANTS = (
        {"inventory_on": "0", "buyers_on": "0"},
        {"inventory_on": "1",
         "inventory_flip_at": "2030-06-01 10:00+03",
         "buyers_on": "1", "buyers_flip_at": "2030-06-01 10:30+03",
         "buyers_override_floor": "0",
         "orders_on": "1", "orders_flip_at": "2030-06-01 11:00+03"},
        {"inventory_on": "1", "buyers_on": "1",
         "orders_on": "1"},
        {"inventory_on": "invalid", "buyers_on": "invalid",
         "orders_on": "invalid"},
        {"inventory_on": "unknown", "buyers_on": "unknown",
         "orders_on": "unknown"},
        {"inventory_on": "0", "buyers_on": "held",
         "orders_on": "pending"},
        {"managers_on": "pending", "orders_on": "pending"},
        # Chain 9's journal (OD-02 (c)): every state the script can pass.
        {"dq_journal_direct": "1"},
        {"dq_journal_direct": "invalid"},
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
                await clean_buyers(conn)
                v, detail = await verdict(conn, path.name, buyers_on="0")
            assert (v, detail.startswith("not applicable")) == ("PASS", True), (path.name, detail)

    @pytest.mark.asyncio
    async def test_a_held_chain_is_not_applicable_to_every_buyer_check(self, pool):
        """The flip did not move the chain: B2–B5 have nothing moved to judge.
        (B1 said so and named the reader, until step 14's S14-13 deleted it
        with the copies it judged.)"""
        verdicts = {}
        for path in FILES:
            if not path.name[3:].startswith("b"):
                continue
            async with scenario(pool) as conn:
                await clean_buyers(conn)
                verdicts[path.name[:2]] = await verdict(
                    conn, path.name, buyers_on="held")
        assert set(verdicts) == {"24", "25", "26", "27"}, verdicts
        assert all(v == "PASS" and "held on DuckDB" in d for v, d in verdicts.values()), verdicts

    @pytest.mark.asyncio
    async def test_inventory_checks_are_not_applicable_while_the_chain_is_off(self, pool):
        for path in FILES:
            if not path.name[3:].startswith("i"):
                continue
            async with scenario(pool) as conn:
                await clean_inventory(conn)
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
    async def test_a_utm_verdict_in_flight_passes_d8_and_is_counted(self, pool):
        """The 07:30 run of 2026-10-01 and 10-02: one order without its verdict
        inside the 20-minute grace, INFO, "Not a defect yet"."""
        async with scenario(pool) as conn:
            await self._clean_mirror_landing_run(conn)
            await dq_issue(conn, DQ_RUN_IDS[0], "pg_order_utm_in_flight",
                           table="silver.order_utm", severity="INFO")
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(minutes=10))
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "PASS" and "1 UTM verdict(s) in flight" in detail, detail

    @pytest.mark.asyncio
    async def test_a_verdict_in_flight_beside_a_real_finding_still_fails(self, pool):
        async with scenario(pool) as conn:
            await self._clean_mirror_landing_run(conn)
            await dq_issue(conn, DQ_RUN_IDS[0], "pg_order_utm_in_flight",
                           table="silver.order_utm", severity="INFO")
            await dq_issue(conn, DQ_RUN_IDS[0], "pg_order_utm_missing",
                           table="silver.order_utm", severity="CRITICAL")
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(minutes=10))
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "FAIL" and "pg_order_utm_missing" in detail, detail
        assert "pg_order_utm_in_flight" not in detail, detail

    @pytest.mark.asyncio
    async def test_the_grace_check_filed_above_info_is_not_forgiven(self, pool):
        """Forgiven at INFO only: the same name at another severity is a
        finding, so a future change to its severity cannot pass silently."""
        async with scenario(pool) as conn:
            await self._clean_mirror_landing_run(conn)
            await dq_issue(conn, DQ_RUN_IDS[0], "pg_order_utm_in_flight",
                           table="silver.order_utm", severity="WARN")
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(minutes=10))
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "FAIL" and "pg_order_utm_in_flight" in detail, detail

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
class TestADirectJournalIsNotGatedOnTheCopy:
    """Chain 9 (OD-02 (c)): once the journal is written in Postgres, these
    rows are the journal and not a copy of it. The copy stands down with the
    chain, its `last_ok_at` freezes, and without this D8, 20, 21 and 22 read
    UNKNOWN for as long as the chain is on. Direct is the script's flag or the
    latch's owner row, which outranks it. Mutation: drop the `journal` CTE's
    effect from one file — its case below stays UNKNOWN."""

    TODAY_0730 = datetime(2030, 6, 5, 7, 30, tzinfo=KYIV)
    FROZEN = dict(ok_at=datetime(2030, 6, 1, 3, 0, tzinfo=KYIV))

    @staticmethod
    async def owner_row(conn):
        await conn.execute(
            "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
            "VALUES ('owner:app.data_quality_runs', '2030-06-01T00:00:00+00:00', now()) "
            "ON CONFLICT (key) DO NOTHING")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("how", ["flag", "owner_row"])
    async def test_d8_judges_the_run(self, pool, how):
        async with scenario(pool) as conn:
            await dq_run(conn, DQ_RUN_IDS[0], layer="mirror_landing",
                         started_at=self.TODAY_0730)
            await mirror_state(conn, "app.data_quality_runs", **self.FROZEN)
            if how == "owner_row":
                await self.owner_row(conn)
            v, detail = await verdict(
                conn, "09_d8_mirror_landing.sql",
                dq_journal_direct="1" if how == "flag" else "0")
        assert v == "PASS", detail

    @pytest.mark.asyncio
    async def test_d8_still_fails_a_finding(self, pool):
        async with scenario(pool) as conn:
            await dq_run(conn, DQ_RUN_IDS[0], layer="mirror_landing",
                         started_at=self.TODAY_0730)
            await dq_issue(conn, DQ_RUN_IDS[0], "gold_cell_values", table="gold.daily_revenue")
            await mirror_state(conn, "app.data_quality_runs", **self.FROZEN)
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql",
                                      dq_journal_direct="1")
        assert v == "FAIL" and "gold_cell_values" in detail, detail

    @pytest.mark.asyncio
    async def test_without_either_the_frozen_copy_is_still_unknown(self, pool):
        async with scenario(pool) as conn:
            await dq_run(conn, DQ_RUN_IDS[0], layer="mirror_landing",
                         started_at=self.TODAY_0730)
            await mirror_state(conn, "app.data_quality_runs", **self.FROZEN)
            v, detail = await verdict(conn, "09_d8_mirror_landing.sql")
        assert v == "UNKNOWN", detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,layer", HISTORY_CHECKS)
    async def test_the_histories_judge_the_runs(self, pool, name, layer):
        history = TestReconciliationPgHistory()
        history.LAYER = layer
        async with scenario(pool) as conn:
            await history.seed(conn, history.every_morning(), copied=self.FROZEN["ok_at"])
            stale_v, _ = await verdict(conn, name)
            v, detail = await verdict(conn, name, dq_journal_direct="1")
        assert stale_v == "UNKNOWN"
        assert v == "PASS", detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,layer", HISTORY_CHECKS)
    async def test_a_silence_now_is_a_fail_not_an_unknown(self, pool, name, layer):
        """With the journal direct there is no copy to wait for: a silence past
        the limit at the read is one that happened."""
        history = TestReconciliationPgHistory()
        history.LAYER = layer
        async with scenario(pool) as conn:
            await history.seed(conn, history.every_morning(last=2),
                               copied=self.FROZEN["ok_at"])
            v, detail = await verdict(conn, name, dq_journal_direct="1")
        assert v == "FAIL", detail


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


# ── chain 1: I1 ───────────────────────────────────────────────────────────────

INVENTORY_TABLES = ("bronze.offers", "bronze.offer_stocks", "app.stock_movements",
                    "app.sku_inventory_status", "app.inventory_sku_history",
                    "app.inventory_history")


async def clean_inventory(conn):
    """No owner row anywhere and no copy stamp on chain 1's six tables."""
    await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
    await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                       list(INVENTORY_TABLES))


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


# ── chain 5: M1 and M2 ────────────────────────────────────────────────────────

from datetime import date  # noqa: E402 — chain 5's seeds carry interval dates


async def clean_managers(conn):
    """M1 and M2 read whole tables; a scenario starts from none of their rows."""
    await conn.execute("DELETE FROM app.manager_classifications")
    await conn.execute("DELETE FROM bronze.managers")
    await conn.execute("DELETE FROM meta.chain_watermarks "
                       "WHERE key LIKE 'owner:%' OR key = 'last_sync_managers'")
    await conn.execute("DELETE FROM meta.mirror_state WHERE table_name IN "
                       "('bronze.managers', 'app.manager_classifications')")


async def manager(conn, mid, *, retail=False, intervals=((date(1970, 1, 1), None, None),),
                  set_at=NOW):
    """A manager and its intervals: (valid_from, valid_to, is_retail or None
    for the manager's own)."""
    await conn.execute("INSERT INTO bronze.managers (id, name, is_retail) VALUES ($1, 'M', $2)",
                       mid, retail)
    for valid_from, valid_to, is_retail in intervals:
        await conn.execute(
            "INSERT INTO app.manager_classifications "
            "(manager_id, is_retail, valid_from, valid_to, set_at) VALUES ($1, $2, $3, $4, $5)",
            mid, retail if is_retail is None else is_retail, valid_from, valid_to, set_at)


async def managers_synced(conn, at):
    await conn.execute(
        "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
        "VALUES ('last_sync_managers', $1, $2)", at.isoformat(), NOW)


@needs_pg
class TestM2ClassificationShape:
    FILE = "31_m2_classification_shape.sql"

    @pytest.mark.asyncio
    async def test_a_clean_shape_and_a_fresh_stamp_pass(self, pool):
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager(conn, 1)
            await manager(conn, 2, retail=True, intervals=(
                (date(1970, 1, 1), date(2030, 3, 1), False), (date(2030, 3, 1), None, True)))
            await managers_synced(conn, ago(hours=5))
            v, detail = await verdict(conn, self.FILE, managers_on="1")
        assert v == "PASS" and "2 manager(s)" in detail and "5 h old" in detail, detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("seed,expected", [
        ("no_open", "not exactly one open interval: {3}"),
        ("two_open", "not exactly one open interval: {3}"),
        ("none", "no interval at all: {3}"),
        ("overlap", "history overlaps or gaps: {3}"),
        ("gap", "history overlaps or gaps: {3}"),
        ("disagree", "is_retail disagrees with the open interval: {3}"),
        ("set_at", "1 interval(s) with no set_at"),
    ])
    async def test_each_shape_fails_by_name(self, pool, seed, expected):
        """Mutation: remove or invert any predicate."""
        d = date
        shapes = {
            "no_open": dict(intervals=((d(1970, 1, 1), d(2030, 1, 1), None),)),
            "two_open": dict(intervals=((d(1970, 1, 1), None, None), (d(2030, 1, 1), None, None))),
            "none": dict(intervals=()),
            "overlap": dict(intervals=((d(1970, 1, 1), d(2030, 3, 1), None),
                                       (d(2030, 1, 1), None, None))),
            "gap": dict(intervals=((d(1970, 1, 1), d(2030, 1, 1), None),
                                   (d(2030, 2, 1), None, None))),
            "disagree": dict(retail=True, intervals=((d(1970, 1, 1), None, False),)),
            "set_at": dict(set_at=None),
        }
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager(conn, 1)
            await manager(conn, 3, **shapes[seed])
            await managers_synced(conn, ago(hours=1))
            v, detail = await verdict(conn, self.FILE, managers_on="1")
        assert v == "FAIL" and expected in detail, detail

    @pytest.mark.asyncio
    async def test_the_stamp_is_judged_by_its_value(self, pool):
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager(conn, 1)
            await managers_synced(conn, ago(hours=27))
            v, detail = await verdict(conn, self.FILE, managers_on="1")
        assert v == "FAIL" and "moved 27 h ago" in detail, detail
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager(conn, 1)
            v, detail = await verdict(conn, self.FILE, managers_on="1")
            assert v == "FAIL" and "no manager sync has completed" in detail, detail
            v, detail = await verdict(conn, self.FILE, managers_on="1",
                                      managers_flip_at=ago(minutes=10).isoformat())
        assert v == "PASS" and "not written yet" in detail, detail

    @pytest.mark.asyncio
    async def test_an_empty_table_fails(self, pool):
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await managers_synced(conn, ago(hours=1))
            v, detail = await verdict(conn, self.FILE, managers_on="1")
        assert v == "FAIL" and "bronze.managers is empty" in detail, detail

    @pytest.mark.asyncio
    async def test_off_and_pending_judge_nothing(self, pool):
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager(conn, 3, intervals=())
            for state in ("0", "pending"):
                v, detail = await verdict(conn, self.FILE, managers_on=state)
                assert v == "PASS" and detail.startswith("not applicable"), detail


# ── chain 3: O1–O5 ────────────────────────────────────────────────────────────

ORDER_TABLES = ("bronze.orders", "bronze.order_products", "bronze.expenses",
                "app.order_backfill_misses")
CHAIN3_IDS = [990_000_301, 990_000_302, 990_000_303, 990_000_304]
O_FILES = ("33_o2_orders_watermark.sql", "34_o3_order_versions.sql",
           "35_o4_halfwritten_orders.sql", "36_o5_orders_integrity.sql")


async def clean_orders(conn):
    """O1–O5 read whole tables; a scenario starts from none of their rows."""
    for table in ("bronze.order_products", "bronze.expenses", "app.order_backfill_misses",
                  "app.order_versions", "bronze.orders"):
        await conn.execute(f"DELETE FROM {table}")
    await conn.execute("DELETE FROM meta.chain_watermarks "
                       "WHERE key LIKE 'owner:%' OR key = 'last_sync_orders'")
    await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                       list(ORDER_TABLES))


async def order_owners(conn, at):
    for table in ORDER_TABLES:
        await conn.execute(
            "INSERT INTO meta.chain_watermarks (key, value, updated_at) VALUES ($1, $2, $3)",
            f"owner:{table}", at.isoformat(), at)


async def order_version(conn, oid, *, captured_at, kind="change"):
    await conn.execute(
        "INSERT INTO app.order_versions (order_id, captured_at, kind) VALUES ($1, $2, $3)",
        oid, captured_at, kind)


async def chain_order(conn, oid, *, total=100, mirrored_at=None, created_at=None,
                      lines=1, version="create"):
    """A header, its line items and its first version, as one write of the
    chain leaves them."""
    mirrored_at = mirrored_at or ago(hours=1)
    await conn.execute(
        "INSERT INTO bronze.orders (id, source_id, status_id, grand_total, created_at, "
        "mirrored_at) VALUES ($1, 1, 1, $2, $3, $4)",
        oid, total, created_at or mirrored_at, mirrored_at)
    for n in range(lines):
        await conn.execute(
            "INSERT INTO bronze.order_products (id, order_id, name, quantity, price_sold, "
            "mirrored_at) VALUES ($1, $2, 'Крем', 1, 10, $3)",
            oid * 1000 + n + 1, oid, mirrored_at)
    if version:
        await order_version(conn, oid, captured_at=mirrored_at, kind=version)


async def order_miss(conn, oid, *, checked_at):
    await conn.execute(
        "INSERT INTO app.order_backfill_misses (order_id, checked_at, reason) "
        "VALUES ($1, $2, 're-fetched by id; KeyCRM served no line items')", oid, checked_at)


async def order_expense(conn, eid, oid, *, mirrored_at):
    await conn.execute(
        "INSERT INTO bronze.expenses (id, order_id, amount, mirrored_at) VALUES ($1, $2, 50, $3)",
        eid, oid, mirrored_at)


async def backfilled(conn, table, at):
    await conn.execute(
        "INSERT INTO meta.mirror_state (table_name, backfilled_at) VALUES ($1, $2) "
        "ON CONFLICT (table_name) DO UPDATE SET backfilled_at = EXCLUDED.backfilled_at",
        table, at)


async def orders_synced(conn, *, stamped_at, value=None):
    """The order step's watermark: the row's stamp is the step's clock, the
    value KeyCRM's newest change."""
    await conn.execute(
        "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
        "VALUES ('last_sync_orders', $1, $2)",
        value if value is not None else (stamped_at - timedelta(minutes=3)).isoformat(),
        stamped_at)


@needs_pg
class TestChain3IsJudgedOnlyOnceItMoved:
    @pytest.mark.asyncio
    async def test_off_is_not_applicable_everywhere(self, pool):
        for name in O_FILES:
            async with scenario(pool) as conn:
                await clean_orders(conn)
                await chain_order(conn, CHAIN3_IDS[0], lines=0, version=None,
                                  created_at=ago(days=3))
                v, detail = await verdict(conn, name, orders_on="0")
            assert (v, detail.startswith("not applicable")) == ("PASS", True), (name, detail)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["pending", "0"])
    async def test_owner_rows_or_none_a_chain_that_has_not_moved_is_not_applicable(
            self, pool, state):
        """The flag without the latch (`pending`), or the flag at duckdb with
        owner rows and no marker (a lost marker): O2–O5 have no chain writing
        Postgres to judge. Until step 14's S14-13 O1 judged both — UNKNOWN
        naming web's preconditions, or FAIL naming the lost marker and the
        lever; the lost marker is `chain_latch_disagrees`' since S14-12, on
        the chain invariants. Mutation: judge either as a chain that moved."""
        for owned in (False, True):
            verdicts = {}
            for name in O_FILES:
                async with scenario(pool) as conn:
                    await clean_orders(conn)
                    if owned:
                        await order_owners(conn, ago(days=2))
                    verdicts[name] = await verdict(conn, name, orders_on=state)
            assert all(v == "PASS" and d.startswith("not applicable")
                       for v, d in verdicts.values()), (owned, verdicts)

    @pytest.mark.asyncio
    async def test_a_state_nobody_named_is_unknown_everywhere(self, pool):
        """Invalid or unread: O2–O5 cannot say whether chain 3 moved, so each
        says so itself. (O1 FAILed an invalid flag, until S14-13.)"""
        for name in O_FILES:
            for state in ("unknown", "invalid"):
                async with scenario(pool) as conn:
                    await clean_orders(conn)
                    v, detail = await verdict(conn, name, orders_on=state)
                assert v == "UNKNOWN", (name, state, detail)
                if state == "invalid":
                    assert "KS_WRITE_ORDERS not understood" in detail, (name, detail)


@needs_pg
class TestO2OrdersWatermark:
    FILE = "33_o2_orders_watermark.sql"

    @pytest.mark.asyncio
    async def test_the_rows_stamp_is_judged_not_its_value(self, pool):
        """The value is KeyCRM's newest change and stands still overnight; the
        row is rewritten by every tick. Mutation: judge the value."""
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await orders_synced(conn, stamped_at=ago(minutes=2),
                                value=ago(hours=9).isoformat())
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "PASS" and "rewritten 2 min ago" in detail, detail
        assert "newest change 05.06 03:00 Kyiv" in detail, detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("age,shown,expected", [
        (timedelta(minutes=104), 104, "PASS"),
        (timedelta(minutes=105), 105, "PASS"),
        (timedelta(minutes=105, seconds=1), 105, "FAIL"),
        (timedelta(minutes=240), 240, "FAIL"),
    ])
    async def test_a_row_older_than_the_canarys_bound_fails(self, pool, age, shown, expected):
        """105 minutes: the 90 the canary excuses for a lock wait plus the 15
        it allows the step once the wait is subtracted — exactly 105 is a tick
        the canary calls waiting, a second more one it pages. Mutation: the
        old 90, or `>=`."""
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await orders_synced(conn, stamped_at=NOW - age)
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == expected and f"rewritten {shown} min ago (limit 105)" in detail, detail

    @pytest.mark.asyncio
    async def test_missing_fails_unless_the_handover_is_recent(self, pool):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            v, detail = await verdict(conn, self.FILE, orders_on="1")
            assert v == "FAIL" and "no order step has completed" in detail, detail
            v, detail = await verdict(conn, self.FILE, orders_on="1",
                                      orders_flip_at=ago(minutes=20).isoformat())
            assert v == "PASS" and "not written yet" in detail, detail
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(minutes=30))
            v, _ = await verdict(conn, self.FILE, orders_on="1")
        assert v == "PASS"
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(hours=2))
            v, _ = await verdict(conn, self.FILE, orders_on="1",
                                 orders_flip_at=ago(minutes=20).isoformat())
        assert v == "FAIL", "the owner rows date the handover before the flip time does"

    @pytest.mark.asyncio
    async def test_a_value_that_is_not_a_timestamp_fails_and_does_not_error(self, pool):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await orders_synced(conn, stamped_at=ago(minutes=1), value="not a time")
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "FAIL" and "not a timestamp" in detail, detail


@needs_pg
class TestO3OrderVersions:
    FILE = "34_o3_order_versions.sql"

    @pytest.mark.asyncio
    async def test_a_living_archive_passes(self, pool):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await chain_order(conn, CHAIN3_IDS[0], mirrored_at=ago(days=3), version="baseline")
            await chain_order(conn, CHAIN3_IDS[1], mirrored_at=ago(minutes=5))
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "PASS" and "1 in 24 h, every order covered" in detail, detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind,expected", [("change", "PASS"), ("create", "PASS"),
                                               ("baseline", "PASS"), ("backfill", "FAIL")])
    async def test_a_day_without_the_writer_fails(self, pool, kind, expected):
        """`reconcile_order_versions`' stall: any kind but an operator's
        backfill proves the writer was alive, the baseline included."""
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await chain_order(conn, CHAIN3_IDS[0], mirrored_at=ago(hours=25))
            await order_version(conn, CHAIN3_IDS[0], captured_at=ago(hours=2), kind=kind)
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == expected, (kind, detail)
        if expected == "FAIL":
            assert "no version since 04.06 11:00 Kyiv (limit 24 h)" in detail, detail

    @pytest.mark.asyncio
    async def test_an_empty_archive_fails(self, pool):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "FAIL" and "the archive holds no version" in detail, detail

    @pytest.mark.asyncio
    async def test_an_order_with_no_version_fails_naming_it(self, pool):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await chain_order(conn, CHAIN3_IDS[0], mirrored_at=ago(minutes=5))
            await chain_order(conn, CHAIN3_IDS[1], mirrored_at=ago(minutes=5), version=None)
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "FAIL" and f"1 order(s) with no version at all, e.g. {{{CHAIN3_IDS[1]}}}" \
            in detail, detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind,expected", [("change", "FAIL"), ("baseline", "PASS"),
                                               ("backfill", "PASS")])
    async def test_over_a_thousand_of_the_writers_versions_in_a_day_fails(self, pool, kind,
                                                                          expected):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await chain_order(conn, CHAIN3_IDS[0], mirrored_at=ago(minutes=5))
            await conn.execute(
                "INSERT INTO app.order_versions (order_id, captured_at, kind) "
                "SELECT $1, $2, $3 FROM generate_series(1, 1000)",
                CHAIN3_IDS[0], ago(hours=3), kind)
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == expected, (kind, detail)
        if expected == "FAIL":
            assert "1001 versions in 24 h (limit 1000): flooding" in detail, detail


class TestO3IsReconcileOrderVersions:
    """O3 asks `reconcile_order_versions`' questions, so its numbers and the
    kinds it leaves out are that function's: a copy that drifted would pass
    an archive the 07:30 run calls stalled. Read out of the SQL, not grepped
    for: the comments name the same numbers."""

    SQL = _uncommented((SQL_DIR / "34_o3_order_versions.sql").read_text(encoding="utf-8"))

    def test_the_limits(self):
        from core.mirror_reconciliation import (
            ORDER_VERSIONS_FLOOD_PER_DAY, ORDER_VERSIONS_STALL_HOURS,
        )

        assert set(re.findall(r"interval '(\d+) hours'", self.SQL)) == {
            str(ORDER_VERSIONS_STALL_HOURS)}
        assert re.findall(r"recent > (\d+)", self.SQL) == [str(ORDER_VERSIONS_FLOOD_PER_DAY)]

    def test_the_kinds_left_out(self):
        from core.pg_order_versions import BACKFILL, NOT_THE_WRITER

        assert re.findall(r"kind <> '(\w+)'", self.SQL) == [BACKFILL]
        [listed] = re.findall(r"kind NOT IN \(([^)]*)\)", self.SQL)
        assert tuple(re.findall(r"'(\w+)'", listed)) == NOT_THE_WRITER


class TestO2SharesTheCanarysBound:
    O2 = _uncommented((SQL_DIR / "33_o2_orders_watermark.sql").read_text(encoding="utf-8"))

    def test_the_lock_wait_bound(self):
        """O2's bound is the longest the canary goes without paging for this
        chain's step: a wait for the heavy-job lock excuses itself up to
        `ORDERS_SYNC_LOCK_WAIT_MAX_S`, and the clock left once the wait is
        subtracted may still be `ORDERS_SYNC_STALE_S` old — 90 + 15 minutes.
        A report stricter than the page would FAIL a tick the canary calls
        waiting. Mutation: change either constant, or put the limit back at
        the lock wait alone (90), as O2 had it."""
        from bot.canary import ORDERS_CHAIN, ORDERS_SYNC_LOCK_WAIT_MAX_S, ORDERS_SYNC_STALE_S

        limit = (ORDERS_SYNC_LOCK_WAIT_MAX_S + ORDERS_SYNC_STALE_S) // 60
        assert (ORDERS_SYNC_LOCK_WAIT_MAX_S + ORDERS_SYNC_STALE_S) % 60 == 0
        assert set(re.findall(r"interval '(\d+) minutes'", self.O2)) == {str(limit)}
        # Judged as an interval, so the boundary is the canary's to the second.
        assert len(re.findall(r"stamped_at > interval '(\d+) minutes'", self.O2)) == 1
        assert re.findall(r"limit (\d+)\)", self.O2) == [str(limit)]
        assert ORDERS_CHAIN == "pg_orders_write"

    def test_the_canary_is_quiet_up_to_that_bound_and_pages_past_it(self):
        """The arithmetic above, checked against the page itself rather than
        against its constants: a step whose last success is exactly the bound
        old, with the longest wait the canary excuses, is not paged; one
        second more is. If the canary's subtraction ever changes shape, this
        fails before O2 and the page disagree. Mutation: page on `>=` in the
        canary, or stop subtracting the wait."""
        from bot.canary import (
            ORDERS_CHAIN, ORDERS_SYNC_LOCK_WAIT_MAX_S, ORDERS_SYNC_STALE_S,
            check_orders_sync_chain,
        )

        def page(ok_age):
            return check_orders_sync_chain({"write_chains": {ORDERS_CHAIN: {
                "mode": "postgres",
                "sync_step": {"consecutive_failures": 0,
                              "last_ok_age_s": ok_age,
                              "last_attempt_age_s": ORDERS_SYNC_LOCK_WAIT_MAX_S + 60,
                              "lock_wait_s": ORDERS_SYNC_LOCK_WAIT_MAX_S}}}})

        bound = ORDERS_SYNC_LOCK_WAIT_MAX_S + ORDERS_SYNC_STALE_S
        assert page(bound) == []
        assert [key for key, _ in page(bound + 1)] == ["orders_sync_failing"]


@needs_pg
class TestO4HalfwrittenOrders:
    FILE = "35_o4_halfwritten_orders.sql"

    @pytest.mark.asyncio
    async def test_revenue_and_no_lines_past_six_hours_fails_naming_it(self, pool):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await chain_order(conn, CHAIN3_IDS[0], lines=0, created_at=ago(hours=7),
                              mirrored_at=ago(hours=1))
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "FAIL" and detail.startswith("1 order(s) worth 100.00 written since "
                                                 "the handover"), detail
        assert f"e.g. {{{CHAIN3_IDS[0]}}}" in detail, detail
        assert "none of them in app.order_backfill_misses" in detail, detail

    @pytest.mark.asyncio
    async def test_a_young_one_a_whole_one_and_a_free_one_pass(self, pool):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await chain_order(conn, CHAIN3_IDS[0], lines=0, created_at=ago(hours=5))
            await chain_order(conn, CHAIN3_IDS[2], lines=2, created_at=ago(days=3))
            await chain_order(conn, CHAIN3_IDS[3], total=0, lines=0, created_at=ago(days=3))
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "PASS" and detail.startswith("every order written since the handover"), \
            detail
        assert "ledger" not in detail, detail

    @pytest.mark.asyncio
    async def test_a_ledger_row_does_not_excuse_one(self, pool):
        """The repair re-fetches a bare order through the chain's own writer
        and ledgers whatever the store still holds empty afterwards, so a
        writer that drops line items launders its own orders into the ledger
        (review of chain 3's soak checks). Measured on production 2026-10-09:
        no such order in the 120 days before the flip. Both FAIL; the detail
        says how many the repair had already asked about. Mutation: exclude
        an order with a ledger row, as O4 first did."""
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await chain_order(conn, CHAIN3_IDS[0], lines=0, created_at=ago(days=1))
            await order_miss(conn, CHAIN3_IDS[0], checked_at=ago(hours=20))
            await chain_order(conn, CHAIN3_IDS[1], total=250, lines=0, created_at=ago(days=1))
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "FAIL" and detail.startswith("2 order(s) worth 350.00 written since "
                                                 "the handover"), detail
        assert f"e.g. {{{CHAIN3_IDS[1]},{CHAIN3_IDS[0]}}}" in detail, detail
        assert "1 of them in app.order_backfill_misses" in detail, detail
        assert "0 in the 120 days before the flip" in detail, detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("hours,expected", [(5, "PASS"), (7, "FAIL")])
    async def test_the_six_hours_are_the_repairs_and_a_ledger_row_adds_none(self, pool, hours,
                                                                              expected):
        """Three of the repair's 2-hour intervals: a run held back by the
        heavy-job lock, or a timer a deploy restarted, is not a defect. Past
        them a ledgered order fails like any other. Mutation: drop the six
        hours, or forgive the ledgered one."""
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await chain_order(conn, CHAIN3_IDS[0], lines=0, created_at=ago(hours=hours),
                              mirrored_at=ago(hours=1))
            await order_miss(conn, CHAIN3_IDS[0], checked_at=ago(minutes=50))
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == expected, detail

    @pytest.mark.asyncio
    async def test_a_header_the_chain_did_not_write_is_not_its_to_answer(self, pool):
        """Written before the handover, it is DuckDB's legacy — the standing
        `orders_without_line_items` counts those. Mutation: drop the
        `mirrored_at` bound."""
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await chain_order(conn, CHAIN3_IDS[0], lines=0, created_at=ago(days=5),
                              mirrored_at=ago(days=3))
            v, _ = await verdict(conn, self.FILE, orders_on="1")
        assert v == "PASS"

    @pytest.mark.asyncio
    async def test_without_owner_rows_the_flip_time_and_without_it_unknown(self, pool):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await chain_order(conn, CHAIN3_IDS[0], lines=0, created_at=ago(hours=8),
                              mirrored_at=ago(hours=1))
            v, detail = await verdict(conn, self.FILE, orders_on="1",
                                      orders_flip_at=ago(hours=2).isoformat())
            assert v == "FAIL" and "since the flip" in detail, detail
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "UNKNOWN" and "SOAK_ORDERS_FLIP_AT" in detail, detail


@needs_pg
class TestO5OrdersIntegrity:
    FILE = "36_o5_orders_integrity.sql"

    @pytest.mark.asyncio
    async def test_a_clean_set_passes(self, pool):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await chain_order(conn, CHAIN3_IDS[0], mirrored_at=ago(days=3))
            await order_expense(conn, CHAIN3_IDS[0], CHAIN3_IDS[0], mirrored_at=ago(days=3))
            # In flight: written this hour, its order not yet — not a day old.
            await order_expense(conn, CHAIN3_IDS[1], CHAIN3_IDS[1], mirrored_at=ago(hours=2))
            await order_miss(conn, CHAIN3_IDS[2], checked_at=ago(days=1))
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "PASS", detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("defect,expected", [
        ("orphan", f"1 expense(s) older than a day with no order (e.g. {{{CHAIN3_IDS[1]}}})"),
        ("null_checked", "1 ledger row(s) with no checked_at"),
    ])
    async def test_each_defect_fails_by_name(self, pool, defect, expected):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            if defect == "orphan":
                await order_expense(conn, CHAIN3_IDS[1], CHAIN3_IDS[1], mirrored_at=ago(days=2))
            else:
                await order_miss(conn, CHAIN3_IDS[2], checked_at=None)
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "FAIL" and expected in detail, (defect, detail)

    @pytest.mark.asyncio
    async def test_the_counts_are_the_standing_watchs_own(self, pool):
        """O5 is the watch's chain-3 group asked by the report, so over the
        same rows the two count the same — on the real clock, which is the
        only one the watch reads. Mutation: change either predicate."""
        from datetime import timezone

        from core.pg_chain_invariants import _EXPENSE_ORPHANS_SQL, _MISS_NULLS_SQL

        real = datetime.now(timezone.utc)
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await chain_order(conn, CHAIN3_IDS[0], mirrored_at=real - timedelta(days=3))
            await order_expense(conn, 990_000_311, CHAIN3_IDS[0],
                                mirrored_at=real - timedelta(days=3))
            await order_expense(conn, 990_000_312, CHAIN3_IDS[1],
                                mirrored_at=real - timedelta(days=2))
            await order_expense(conn, 990_000_313, CHAIN3_IDS[2],
                                mirrored_at=real - timedelta(hours=23))
            await order_miss(conn, CHAIN3_IDS[3], checked_at=None)
            await order_miss(conn, CHAIN3_IDS[2], checked_at=real)
            orphans = await conn.fetchrow(_EXPENSE_ORPHANS_SQL)
            nulls = await conn.fetchrow(_MISS_NULLS_SQL)
            v, detail = await verdict(conn, self.FILE, now=None, orders_on="1")
        assert (orphans["orphans"], nulls["checked_at"]) == (1, 1)
        assert v == "FAIL" and detail.startswith(
            f"{orphans['orphans']} expense(s) older than a day with no order (e.g. "
            f"{{{orphans['sample'][0]}}}), {nulls['checked_at']} ledger row(s)"), detail


# ── the owner rows date the handover before the flip time does ───────────────
# Every check that dates a chain's handover by its owner rows and only then by
# the operator's flip time, each with the one thing it counts after the
# handover. Both given, the owner rows win: they are the chain's first write,
# on the clock the shippers stamp with, and a flip time is what somebody typed.
# The review of the chain 3 soak checks found I1's order survived every I1
# test — swap `owner.at` and `flag.flip_at` and nothing failed — and the same
# swap then survived B1's, M1's, O1's and O4's (2026-10-10). I1, B1, M1 and O1
# went in step 14's S14-13; O4 is the one left on that clock. Walked, not
# listed: a new file on that clock with no arrangement here fails
# `test_every_handover_clock_is_pinned`.


async def _o4_between(conn, latch, problem_at):
    await clean_orders(conn)
    await order_owners(conn, latch)
    await chain_order(conn, CHAIN3_IDS[0], lines=0, mirrored_at=problem_at,
                      created_at=problem_at - timedelta(hours=2))


# file -> (the flag that says the chain writes Postgres, its flip-time
# variable, the arrangement: owner rows at `latch`, one problem at `problem_at`)
HANDOVER_CLOCKS = {
    "35_o4_halfwritten_orders.sql": ("orders_on", "orders_flip_at", _o4_between),
}
# O2 asks whether a missing watermark is forgiven by a recent handover — a
# different shape, pinned by its own test, which has to go on existing.
HANDOVER_CLOCKS_ELSEWHERE = {
    "33_o2_orders_watermark.sql": ("TestO2OrdersWatermark",
                                   "test_missing_fails_unless_the_handover_is_recent"),
}


def test_every_handover_clock_is_pinned():
    """Mutation: add `COALESCE(owner.at, flag.flip_at)` to a check and leave
    it out of both maps."""
    dating = {p.name for p in FILES
              if re.search(r"COALESCE\(\s*owner\.at\s*,\s*flag\.flip_at\b",
                           _code(p.read_text(encoding="utf-8")))}
    assert dating == set(HANDOVER_CLOCKS) | set(HANDOVER_CLOCKS_ELSEWHERE), dating
    for cls, test in HANDOVER_CLOCKS_ELSEWHERE.values():
        assert callable(getattr(globals()[cls], test, None)), (cls, test)


@needs_pg
class TestTheOwnerRowsDateTheHandoverFirst:
    LATCH = ago(days=2)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", sorted(HANDOVER_CLOCKS))
    async def test_a_flip_time_after_the_latch_excuses_nothing(self, pool, name):
        """Owner rows two days old, a flip time an hour old: a problem five
        hours ago is after the handover. Mutation: date the handover by the
        flip time first — the problem falls before it and passes."""
        state, flip_var, arrange = HANDOVER_CLOCKS[name]
        async with scenario(pool) as conn:
            await arrange(conn, self.LATCH, ago(hours=5))
            v, detail = await verdict(conn, name, **{
                state: "1", flip_var: ago(hours=1).isoformat()})
        assert v == "FAIL" and "since the handover" in detail, detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", sorted(HANDOVER_CLOCKS))
    async def test_a_flip_time_before_the_latch_blames_nothing(self, pool, name):
        """Owner rows two days old, a flip time three days old: a problem
        stamped between the two is before the chain's first write, history.
        Mutation: date the handover by the flip time first — it fails."""
        state, flip_var, arrange = HANDOVER_CLOCKS[name]
        async with scenario(pool) as conn:
            await arrange(conn, self.LATCH, ago(days=2, hours=12))
            v, detail = await verdict(conn, name, **{
                state: "1", flip_var: ago(days=3).isoformat()})
        assert v == "PASS" and "since the handover" in detail, detail
