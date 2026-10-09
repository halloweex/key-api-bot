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
             "buyers_override_floor": "", "dq_journal_direct": "0",
             "watchdogs_on": "0", "weekly_ledger_on": "0", "traffic_ledger_on": "0",
             # Stage 5's clocks (OD-17 (a)); tests/integration/test_stage5_soak_sql.py.
             "duckdb_off": "0", "parallel_from": "", "duckdb_file_last": "none",
             "duckdb_file_since": "", "duckdb_file_since_reason": "",
             "duckdb_file_checked_at": "", "duckdb_file_missing_at": "",
             # What `.env` tells the host-cron sidecars (P4): off, so a test
             # that turns web off is not also testing `.env`.
             "duckdb_off_env": "1", "duckdb_env_changed_at": ""}
# Chain 5's two (30_m1, 31_m2).
VARIABLES.update({"managers_on": "0", "managers_flip_at": ""})
# Chain 3's five (32_o1 … 36_o5).
VARIABLES.update({"orders_on": "0", "orders_flip_at": ""})
# When the script read the latch markers: O1 and M1 tell a latch taken during
# the report from a lost marker by it (chain 3 soak review). Empty judges every
# owner row as older, which is what the scenarios below mean unless they say.
VARIABLES.update({"markers_read_at": ""})
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
        {"inventory_on": "0", "dq_pg_warehouse_on": "0", "buyers_on": "0"},
        {"inventory_on": "1", "dq_pg_warehouse_on": "1",
         "inventory_flip_at": "2030-06-01 10:00+03",
         "buyers_on": "1", "buyers_flip_at": "2030-06-01 10:30+03",
         "buyers_override_floor": "0",
         "orders_on": "1", "orders_flip_at": "2030-06-01 11:00+03"},
        {"inventory_on": "1", "dq_pg_warehouse_on": "1", "buyers_on": "1",
         "orders_on": "1"},
        {"inventory_on": "invalid", "dq_pg_warehouse_on": "invalid", "buyers_on": "invalid",
         "orders_on": "invalid"},
        {"inventory_on": "unknown", "dq_pg_warehouse_on": "unknown", "buyers_on": "unknown",
         "orders_on": "unknown"},
        {"inventory_on": "0", "dq_pg_warehouse_on": "0", "buyers_on": "held",
         "buyers_held_by": "KS_SMS_STORE", "orders_on": "pending"},
        {"managers_on": "pending", "orders_on": "pending",
         "markers_read_at": "2030-06-05T08:59:30Z"},
        # The shadow chains (OD-02 (c)): every state the script can pass.
        {"dq_journal_direct": "1", "watchdogs_on": "1", "weekly_ledger_on": "1",
         "traffic_ledger_on": "1"},
        {"dq_journal_direct": "invalid", "watchdogs_on": "invalid",
         "weekly_ledger_on": "unknown", "traffic_ledger_on": "unknown"},
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
    async def test_a_held_chain_fails_b1_once_and_nothing_else(self, pool):
        """The flip did not move the chain: B1 says so and names the reader;
        B2–B5 have nothing moved to judge."""
        verdicts = {}
        for path in FILES:
            if not path.name[3:].startswith("b"):
                continue
            async with scenario(pool) as conn:
                await clean_buyers(conn)
                verdicts[path.name[:2]] = await verdict(
                    conn, path.name, buyers_on="held", buyers_held_by="KS_READ_DASHBOARD")
        v, detail = verdicts.pop("23")
        assert v == "FAIL" and "so nothing moved" in detail, detail
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

    @pytest.mark.asyncio
    async def test_pairing_judges_the_run(self, pool):
        async with scenario(pool) as conn:
            await mirror_state(conn, "app.data_quality_runs", **self.FROZEN)
            await dq_run(conn, DQ_RUN_IDS[1], layer="integrity", started_at=ago(hours=5))
            await dq_issue(conn, DQ_RUN_IDS[1], "pg_line_items_disagree")
            stale_v, _ = await verdict(conn, "21_pairing.sql", dq_pg_warehouse_on="1")
            v, detail = await verdict(conn, "21_pairing.sql", dq_pg_warehouse_on="1",
                                      dq_journal_direct="1")
        assert stale_v == "UNKNOWN"
        assert v == "FAIL" and "pg_line_items_disagree" in detail, detail


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

    @staticmethod
    async def clean(conn):
        await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = 'app.manual_expenses'")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("attempted,expected", [(timedelta(hours=23), "FAIL"),
                                                    (timedelta(hours=25), "PASS")])
    async def test_a_failure_is_judged_a_day_at_a_time(self, pool, attempted, expected):
        """Only a successful copy resets `failures_since_ok`, and under chain 8
        the copy never succeeds: it stamps the table failing while the cause
        lasts, then stands down silently — so one episode failed E1 for ever.
        O1's rule (chain 3 review). Mutation: drop the 24 hours."""
        async with scenario(pool) as conn:
            await self.clean(conn)
            await mirror_state(conn, "app.manual_expenses", ok_at=ago(days=20),
                               attempted_at=NOW - attempted, failures=4,
                               error="not shipped: KS_WRITE_EXPENSES='postgress' is not understood")
            v, detail = await verdict(conn, self.FILE)
        assert v == expected, detail
        if expected == "PASS":
            assert "are history" in detail, detail

    @pytest.mark.asyncio
    async def test_a_failure_before_the_handover_is_history(self, pool):
        """Chain 8's owner row is its handover: a failure stamped before it is
        not a copy at work over rows the chain wrote. Mutation: drop the
        owner row's bound."""
        for attempted, failures, expected in ((ago(hours=5), 1, "PASS"),
                                              (ago(hours=1), 2, "FAIL")):
            async with scenario(pool) as conn:
                await self.clean(conn)
                await conn.execute(
                    "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                    "VALUES ('owner:app.manual_expenses', 'x', $1)", ago(hours=2))
                await mirror_state(conn, "app.manual_expenses", ok_at=ago(days=20),
                                   attempted_at=attempted, failures=failures,
                                   error="owner rows in Postgres since 2030-06-05 "
                                         "and no local marker")
                v, detail = await verdict(conn, self.FILE)
            assert v == expected, (attempted, detail)
        assert "no local marker" in detail, detail

    @pytest.mark.asyncio
    async def test_another_chains_owner_row_is_not_chain_8s_handover(self, pool):
        """Mutation: read every `owner:` row rather than chain 8's."""
        async with scenario(pool) as conn:
            await self.clean(conn)
            await conn.execute(
                "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
                "VALUES ('owner:app.buyer_gender', 'x', $1)", ago(hours=2))
            await mirror_state(conn, "app.manual_expenses", ok_at=ago(days=20),
                               attempted_at=ago(hours=5), failures=1, error="boom")
            v, detail = await verdict(conn, self.FILE)
        assert v == "FAIL", detail


# ── chain 1: I1 ───────────────────────────────────────────────────────────────

INVENTORY_TABLES = ("bronze.offers", "bronze.offer_stocks", "app.stock_movements",
                    "app.sku_inventory_status", "app.inventory_sku_history",
                    "app.inventory_history")


async def clean_inventory(conn):
    """No owner row anywhere and no copy stamp on chain 1's six tables."""
    await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
    await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                       list(INVENTORY_TABLES))


async def inventory_owners(conn, at):
    for table in INVENTORY_TABLES:
        await conn.execute(
            "INSERT INTO meta.chain_watermarks (key, value, updated_at) VALUES ($1, $2, $3)",
            f"owner:{table}", at.isoformat(), at)


@needs_pg
class TestI1InventoryCopyStoodDown:
    """Chain 1 is latched in production since 2026-09-30, so its owner rows
    stand: they date the handover, as O1's do (chain 3 review)."""

    FILE = "15_i1_inventory_copy_stood_down.sql"

    @pytest.mark.asyncio
    async def test_a_copy_after_the_handover_fails(self, pool):
        """Mutation: date the handover by the flip time or the window alone —
        a copy five hours after a handover two days ago passes."""
        async with scenario(pool) as conn:
            await clean_inventory(conn)
            await inventory_owners(conn, ago(days=2))
            await mirror_state(conn, "app.stock_movements", ok_at=ago(hours=5))
            v, detail = await verdict(conn, self.FILE, inventory_on="1")
        assert v == "FAIL" and "app.stock_movements written by the copy" in detail, detail
        assert "since the handover" in detail, detail

    @pytest.mark.asyncio
    async def test_a_copy_before_the_handover_passes(self, pool):
        """The last copy before the chain's first write is history.
        Mutation: count any `last_ok_at`, not one after the handover."""
        async with scenario(pool) as conn:
            await clean_inventory(conn)
            await inventory_owners(conn, ago(days=2))
            await mirror_state(conn, "bronze.offers", ok_at=ago(days=2, minutes=5))
            v, detail = await verdict(conn, self.FILE, inventory_on="1")
        assert v == "PASS" and "since the handover" in detail, detail

    @pytest.mark.asyncio
    async def test_a_failure_before_the_handover_is_not_the_copys_now(self, pool):
        """Every failure ever stamped used to count, the ones from before the
        flip included. Mutation: drop `last_attempted_at > since.at`."""
        for attempted, expected in ((ago(hours=5), "PASS"), (ago(hours=1), "FAIL")):
            async with scenario(pool) as conn:
                await clean_inventory(conn)
                await inventory_owners(conn, ago(hours=2))
                await mirror_state(conn, "app.inventory_history", ok_at=ago(days=3),
                                   attempted_at=attempted, failures=3,
                                   error="owned by Postgres since 2030-06-05")
                v, detail = await verdict(conn, self.FILE, inventory_on="1")
            assert v == expected, (attempted, detail)
        assert "failing (3): owned by Postgres since" in detail, detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("attempted,expected", [(timedelta(hours=23), "FAIL"),
                                                    (timedelta(hours=25), "PASS")])
    async def test_a_failure_is_judged_a_day_at_a_time(self, pool, attempted, expected):
        """The copy stamps the six tables failing while the flag and the latch
        disagree, then stands down silently once they agree, and only a
        successful copy would reset the count — the sticky FAIL the file's old
        'KNOWN AND LEFT' note described. Mutation: drop the 24 hours."""
        async with scenario(pool) as conn:
            await clean_inventory(conn)
            await inventory_owners(conn, ago(days=4))
            await mirror_state(conn, "bronze.offer_stocks", ok_at=ago(days=5),
                               attempted_at=NOW - attempted, failures=3,
                               error="owned by Postgres since 2030-06-01")
            v, detail = await verdict(conn, self.FILE, inventory_on="1")
        assert v == expected, detail

    @pytest.mark.asyncio
    async def test_without_owner_rows_the_flip_time_decides_and_without_it_unknown(self, pool):
        """The copy stamps all six tables every hour until the flip, so with
        neither an owner row nor a flip time a healthy flip reads a stamp
        inside the window until the chain's first write. Mutation: FAIL a
        guessed window."""
        async with scenario(pool) as conn:
            await clean_inventory(conn)
            await mirror_state(conn, "bronze.offers", ok_at=ago(minutes=30))
            v, _ = await verdict(conn, self.FILE, inventory_on="1",
                                 inventory_flip_at=ago(hours=1).isoformat())
            assert v == "FAIL"
            v, detail = await verdict(conn, self.FILE, inventory_on="1",
                                      inventory_flip_at=ago(minutes=10).isoformat())
            assert v == "PASS" and "since the flip" in detail, detail
            v, detail = await verdict(conn, self.FILE, inventory_on="1")
        assert v == "UNKNOWN" and "SOAK_INVENTORY_FLIP_AT" in detail, detail

    @pytest.mark.asyncio
    async def test_a_lost_marker_fails_i1_once_and_nothing_else(self, pool):
        """Owner rows in Postgres, no marker, the flag at duckdb: the script
        passes 0, the stock step writes DuckDB again, and the hourly copy
        stands down on the owner rows, so the inventory in Postgres stops
        moving. I1 says so, naming the marker and the copy-back; I2–I5 have no
        chain writing Postgres to judge. Mutation: judge 0 as not applicable
        without reading the owner rows."""
        verdicts = {}
        for path in FILES:
            if not path.name[3:].startswith("i"):
                continue
            async with scenario(pool) as conn:
                await clean_inventory(conn)
                await inventory_owners(conn, ago(days=2))
                verdicts[path.name[:2]] = await verdict(conn, path.name, inventory_on="0")
        v, detail = verdicts.pop("15")
        assert v == "FAIL", detail
        assert "data/write-chain-owners/pg_inventory_write" in detail, detail
        assert "scripts/chain_copy_back.py inventory" in detail, detail
        assert "since 03.06 12:00 Kyiv" in detail, detail
        assert verdicts and all(v == "PASS" and d.startswith("not applicable")
                                for v, d in verdicts.values()), verdicts

    @pytest.mark.asyncio
    async def test_another_chains_owner_rows_are_not_chain_1s(self, pool):
        """Mutation: read every `owner:` row rather than chain 1's six."""
        async with scenario(pool) as conn:
            await clean_inventory(conn)
            await manager_owners(conn, ago(days=2))
            v, detail = await verdict(conn, self.FILE, inventory_on="0")
        assert v == "PASS" and detail.startswith("not applicable"), detail

    @pytest.mark.asyncio
    async def test_invalid_fails_and_unknown_is_unknown(self, pool):
        """A flag no chain understands writes nowhere (DN-01); a flag nobody
        could read says nothing. Mutation: answer `invalid` as UNKNOWN."""
        async with scenario(pool) as conn:
            await clean_inventory(conn)
            v, detail = await verdict(conn, self.FILE, inventory_on="invalid")
            assert v == "FAIL" and "no chain understands" in detail, detail
            v, detail = await verdict(conn, self.FILE, inventory_on="unknown")
        assert v == "UNKNOWN" and "could not read KS_WRITE_INVENTORY" in detail, detail


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
    @pytest.mark.parametrize("attempted,expected", [(timedelta(hours=23), "FAIL"),
                                                    (timedelta(hours=25), "PASS")])
    async def test_a_failure_is_judged_a_day_at_a_time(self, pool, attempted, expected):
        """The hourly copy stamps app.buyer_gender failing while the flag and
        the latch disagree, then stands down silently once they agree, and
        only a successful copy would reset the count. O1's rule (chain 3
        review). Mutation: drop the 24 hours."""
        async with scenario(pool) as conn:
            await clean_buyers(conn)
            await owners(conn, ago(days=4))
            await mirror_state(conn, "app.buyer_gender", ok_at=ago(days=5),
                               attempted_at=NOW - attempted, failures=3,
                               error="owned by Postgres since 2030-06-01")
            v, detail = await verdict(conn, self.FILE, buyers_on="1")
        assert v == expected, detail

    @pytest.mark.asyncio
    async def test_a_lost_marker_fails_b1_once_and_nothing_else(self, pool):
        """Owner rows in Postgres, no marker, the flag at duckdb: the script
        passes 0, the buyers step writes DuckDB again, and nothing carries it
        to Postgres — the buyers mirror and the hourly copy both stand down on
        the owner rows — so every buyer reader there stops moving. Chain 4 is
        latched in production. Mutation: judge 0 as not applicable without
        reading the owner rows."""
        verdicts = {}
        for path in FILES:
            if not path.name[3:].startswith("b"):
                continue
            async with scenario(pool) as conn:
                await clean_buyers(conn)
                await owners(conn, ago(days=2))
                verdicts[path.name[:2]] = await verdict(conn, path.name, buyers_on="0")
        v, detail = verdicts.pop("23")
        assert v == "FAIL", detail
        assert "data/write-chain-owners/pg_buyers_write" in detail, detail
        assert "scripts/chain_copy_back.py buyers" in detail, detail
        assert all(v == "PASS" and d.startswith("not applicable")
                   for v, d in verdicts.values()), verdicts

    @pytest.mark.asyncio
    async def test_held_with_owner_rows_is_the_marker_lost(self, pool):
        """Owner rows, no marker, the flag at postgres and a reader on duckdb:
        the script passes `held`, but this is no flip that did not move — the
        unmet reader keeps the buyers step on DuckDB with nothing shipping it,
        and taking the flag back would only make it state 0. B1 names the
        marker and the copy-back; B2–B5 stay as held. Mutation: answer `held`
        without reading the owner rows."""
        verdicts = {}
        for path in FILES:
            if not path.name[3:].startswith("b"):
                continue
            async with scenario(pool) as conn:
                await clean_buyers(conn)
                await owners(conn, ago(days=2))
                verdicts[path.name[:2]] = await verdict(
                    conn, path.name, buyers_on="held", buyers_held_by="KS_SMS_STORE")
        v, detail = verdicts.pop("23")
        assert v == "FAIL", detail
        assert "data/write-chain-owners/pg_buyers_write" in detail, detail
        assert "scripts/chain_copy_back.py buyers" in detail, detail
        assert "KS_SMS_STORE" in detail and "since 03.06 12:00 Kyiv" in detail, detail
        assert "so nothing moved" not in detail, detail
        assert all(v == "PASS" and "held on DuckDB" in d for v, d in verdicts.values()), verdicts

    @pytest.mark.asyncio
    async def test_held_and_invalid_fail_with_their_reason(self, pool):
        async with scenario(pool) as conn:
            await clean_buyers(conn)
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


async def manager_owners(conn, at):
    for table in ("bronze.managers", "app.manager_classifications"):
        await conn.execute(
            "INSERT INTO meta.chain_watermarks (key, value, updated_at) VALUES ($1, $2, $3)",
            f"owner:{table}", at.isoformat(), at)


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
class TestM1ManagersCopyStoodDown:
    FILE = "30_m1_managers_copy_stood_down.sql"

    @pytest.mark.asyncio
    async def test_a_copy_after_the_handover_fails(self, pool):
        """Mutation: judge the tables the replica does not write."""
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager_owners(conn, ago(days=2))
            await mirror_state(conn, "app.manager_classifications", ok_at=ago(hours=5))
            v, detail = await verdict(conn, self.FILE, managers_on="1")
        assert v == "FAIL" and "app.manager_classifications written by a copy" in detail, detail

    @pytest.mark.asyncio
    async def test_a_copy_before_the_handover_passes(self, pool):
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager_owners(conn, ago(days=2))
            await mirror_state(conn, "bronze.managers", ok_at=ago(days=2, minutes=5))
            v, detail = await verdict(conn, self.FILE, managers_on="1")
        assert v == "PASS" and "since the handover" in detail, detail

    @pytest.mark.asyncio
    async def test_a_failure_after_the_handover_fails(self, pool):
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager_owners(conn, ago(days=2))
            await mirror_state(conn, "bronze.managers", ok_at=ago(days=3),
                               attempted_at=ago(hours=1), failures=2, error="boom")
            v, detail = await verdict(conn, self.FILE, managers_on="1")
        assert v == "FAIL" and "failing (2)" in detail, detail

    @pytest.mark.asyncio
    async def test_without_owner_rows_the_flip_time_decides_and_without_it_unknown(self, pool):
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await mirror_state(conn, "bronze.managers", ok_at=ago(minutes=30))
            v, _ = await verdict(conn, self.FILE, managers_on="1",
                                 managers_flip_at=ago(hours=1).isoformat())
            assert v == "FAIL"
            v, detail = await verdict(conn, self.FILE, managers_on="1",
                                      managers_flip_at=ago(minutes=10).isoformat())
            assert v == "PASS" and "since the flip" in detail, detail
            v, detail = await verdict(conn, self.FILE, managers_on="1")
        assert v == "UNKNOWN" and "SOAK_MANAGERS_FLIP_AT" in detail, detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("attempted,expected", [(timedelta(hours=23), "FAIL"),
                                                    (timedelta(hours=25), "PASS")])
    async def test_a_failure_is_judged_a_day_at_a_time(self, pool, attempted, expected):
        """Nothing resets `failures_since_ok` while the chain owns the tables:
        the replica stands down without a success to clear it. O1's rule
        (chain 3 review). Mutation: drop the 24 hours."""
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager_owners(conn, ago(days=4))
            await mirror_state(conn, "bronze.managers", ok_at=ago(days=5),
                               attempted_at=NOW - attempted, failures=2, error="boom")
            v, detail = await verdict(conn, self.FILE, managers_on="1")
        assert v == expected, detail

    @pytest.mark.asyncio
    async def test_a_lost_marker_fails_m1_and_not_m2(self, pool):
        """Owner rows in Postgres, no marker, the flag at duckdb: the script
        passes 0, a classification is written to DuckDB again, and the replica
        stands down on the owner rows, so it never reaches Postgres. M1 says
        so and names the marker and the copy-back; M2 has no chain writing
        Postgres to judge. Mutation: judge 0 as not applicable without reading
        the owner rows."""
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager_owners(conn, ago(days=2))
            v, detail = await verdict(conn, self.FILE, managers_on="0")
            v2, detail2 = await verdict(conn, "31_m2_classification_shape.sql",
                                        managers_on="0")
        assert v == "FAIL", detail
        assert "data/write-chain-owners/pg_managers_write" in detail, detail
        assert "scripts/chain_copy_back.py managers" in detail, detail
        assert v2 == "PASS" and detail2.startswith("not applicable"), detail2

    @pytest.mark.asyncio
    async def test_pending_with_owner_rows_is_the_marker_lost(self, pool):
        """Owner rows, no marker, the flag at postgres: the script passes
        `pending`, but this is no flip waiting for its first tick — while a
        precondition is unmet a classification goes to DuckDB and the replica
        stands down on the owner rows. M1 FAILs naming the marker and the
        copy-back, not UNKNOWN pointing at the preconditions. Mutation: answer
        `pending` without reading the owner rows."""
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager_owners(conn, ago(days=2))
            v, detail = await verdict(conn, self.FILE, managers_on="pending")
        assert v == "FAIL", detail
        assert "data/write-chain-owners/pg_managers_write" in detail, detail
        assert "scripts/chain_copy_back.py managers" in detail, detail
        assert "since 03.06 12:00 Kyiv" in detail, detail

    @pytest.mark.asyncio
    async def test_a_latch_taken_while_the_report_ran_is_not_a_lost_marker(self, pool):
        """The script reads the markers first and runs M1 some thirty psql
        sessions later, and a healthy first write takes the marker before it
        claims the owner rows (`pg_managers_write`: `_latch()`, then the
        transaction whose `now()` dates them). So owner rows dated after the
        script read the marker are a chain that latched during the report —
        flip day, the first tick — not a marker lost: UNKNOWN, run it again.
        Rows dated before the read are the lost marker still. Mutation: judge
        `pending` with owner rows as FAIL whenever they were written (the
        review reproduced exactly that against a latch 10 seconds old)."""
        async with scenario(pool) as conn:
            await clean_managers(conn)
            await manager_owners(conn, ago(seconds=10))
            v, detail = await verdict(conn, self.FILE, managers_on="pending",
                                      markers_read_at=ago(seconds=40).isoformat())
            assert v == "UNKNOWN", detail
            assert "latched while this report ran" in detail, detail
            assert "run the report again" in detail, detail
            assert "11:59:50" in detail and "11:59:20" in detail, detail
            v, detail = await verdict(conn, self.FILE, managers_on="pending",
                                      markers_read_at=ago(seconds=5).isoformat())
        assert v == "FAIL" and "the marker is lost" in detail, detail

    @pytest.mark.asyncio
    async def test_the_states_that_judge_nothing(self, pool):
        async with scenario(pool) as conn:
            await clean_managers(conn)
            v, detail = await verdict(conn, self.FILE, managers_on="0")
            assert (v, detail.startswith("not applicable")) == ("PASS", True)
            v, detail = await verdict(conn, self.FILE, managers_on="pending")
            assert v == "UNKNOWN" and "unmet_precondition" in detail, detail
            v, detail = await verdict(conn, self.FILE, managers_on="invalid")
        assert v == "FAIL" and "no chain understands" in detail, detail


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
O_FILES = ("32_o1_orders_copies_stood_down.sql", "33_o2_orders_watermark.sql",
           "34_o3_order_versions.sql", "35_o4_halfwritten_orders.sql",
           "36_o5_orders_integrity.sql")


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
    async def test_pending_is_o1s_unknown_and_nothing_else(self, pool):
        """The flag without the latch: the preconditions are web's to judge, so
        O1 names where web says why, and O2–O5 have nothing moved to judge."""
        verdicts = {}
        for name in O_FILES:
            async with scenario(pool) as conn:
                await clean_orders(conn)
                verdicts[name] = await verdict(conn, name, orders_on="pending")
        v, detail = verdicts.pop(O_FILES[0])
        assert v == "UNKNOWN" and "write_chains.pg_orders_write.unmet_precondition" in detail
        assert all(v == "PASS" and d.startswith("not applicable")
                   for v, d in verdicts.values()), verdicts

    @pytest.mark.asyncio
    async def test_a_lost_marker_fails_o1_once_and_nothing_else(self, pool):
        """Owner rows in Postgres and no marker, the flag at duckdb: the script
        passes 0, and the writes follow the flag back to DuckDB while the
        sync's per-tick mirror — which asks only the local answer — ships
        DuckDB's orders over the chain's every tick (DN-22a,
        `order_owner_row_without_marker`). O1 says so, naming the marker and
        the copy-back; O2–O5 have no chain writing Postgres to judge.
        Mutation: judge 0 as not applicable without reading the owner rows."""
        verdicts = {}
        for name in O_FILES:
            async with scenario(pool) as conn:
                await clean_orders(conn)
                await order_owners(conn, ago(days=2))
                verdicts[name] = await verdict(conn, name, orders_on="0")
        v, detail = verdicts.pop(O_FILES[0])
        assert v == "FAIL", detail
        assert "data/write-chain-owners/pg_orders_write" in detail, detail
        assert "scripts/chain_copy_back.py orders" in detail, detail
        assert "since 03.06 12:00 Kyiv" in detail, detail
        assert all(v == "PASS" and d.startswith("not applicable")
                   for v, d in verdicts.values()), verdicts

    @pytest.mark.asyncio
    async def test_pending_with_owner_rows_fails_o1_once_and_nothing_else(self, pool):
        """Owner rows and no marker with the flag at postgres: the script
        passes `pending`, but this is no flip waiting for its first write.
        While a precondition is unmet the writes stay on DuckDB and the sync's
        mirror ships DuckDB's orders over the chain's every tick, as at 0. O1
        FAILs naming the marker and the copy-back rather than UNKNOWN pointing
        at the preconditions; O2–O5 stay not applicable. Mutation: answer
        `pending` without reading the owner rows."""
        verdicts = {}
        for name in O_FILES:
            async with scenario(pool) as conn:
                await clean_orders(conn)
                await order_owners(conn, ago(days=2))
                verdicts[name] = await verdict(conn, name, orders_on="pending")
        v, detail = verdicts.pop(O_FILES[0])
        assert v == "FAIL", detail
        assert "data/write-chain-owners/pg_orders_write" in detail, detail
        assert "scripts/chain_copy_back.py orders" in detail, detail
        assert "since 03.06 12:00 Kyiv" in detail, detail
        assert all(v == "PASS" and d.startswith("not applicable")
                   for v, d in verdicts.values()), verdicts

    @pytest.mark.asyncio
    async def test_a_latch_taken_while_the_report_ran_is_o1s_unknown_and_nothing_else(
            self, pool):
        """Flip day: the checklist runs the report at +2 min, and chain 3
        latches on the first order KeyCRM changes. The script read the marker
        before log_check and some thirty psql sessions; the first write took
        the marker, then claimed the owner rows in its transaction
        (`pg_orders_write`: `_latch()`, then `chain_latch.claim`). Rows dated
        after the script read the marker are that latch, not a lost marker,
        and the lever a FAIL names is a rollback tool: O1 is UNKNOWN and says
        to run the report again; O2–O5 stay not applicable. Rows dated before
        the read are the lost marker still. Mutation: judge `pending` with
        owner rows as FAIL whenever they were written (the review reproduced
        it against a latch 10 seconds old)."""
        verdicts = {}
        for name in O_FILES:
            async with scenario(pool) as conn:
                await clean_orders(conn)
                await order_owners(conn, ago(seconds=10))
                verdicts[name] = await verdict(conn, name, orders_on="pending",
                                               markers_read_at=ago(seconds=40).isoformat())
        v, detail = verdicts.pop(O_FILES[0])
        assert v == "UNKNOWN", detail
        assert "latched while this report ran" in detail, detail
        assert "run the report again" in detail, detail
        assert "11:59:50" in detail and "11:59:20" in detail, detail
        assert "chain_copy_back" not in detail, detail
        assert all(v == "PASS" and d.startswith("not applicable")
                   for v, d in verdicts.values()), verdicts
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(seconds=10))
            v, detail = await verdict(conn, O_FILES[0], orders_on="pending",
                                      markers_read_at=ago(seconds=5).isoformat())
        assert v == "FAIL" and "the marker is lost" in detail, detail

    @pytest.mark.asyncio
    async def test_another_chains_owner_rows_are_not_chain_3s(self, pool):
        """Mutation: read every `owner:` row rather than chain 3's four."""
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await owners(conn, ago(days=2))
            v, detail = await verdict(conn, O_FILES[0], orders_on="0")
        assert v == "PASS" and detail.startswith("not applicable"), detail

    @pytest.mark.asyncio
    async def test_a_state_nobody_named_is_unknown_everywhere_but_invalid_fails_o1(self, pool):
        for name in O_FILES:
            async with scenario(pool) as conn:
                await clean_orders(conn)
                v, _ = await verdict(conn, name, orders_on="unknown")
                assert v == "UNKNOWN", name
                v, detail = await verdict(conn, name, orders_on="invalid")
            if name == O_FILES[0]:
                assert v == "FAIL" and "no chain understands" in detail, detail
            else:
                assert v == "UNKNOWN" and "see O1" in detail, (name, detail)


@needs_pg
class TestO1OrdersCopiesStoodDown:
    FILE = "32_o1_orders_copies_stood_down.sql"

    @pytest.mark.asyncio
    async def test_the_hourly_copy_after_the_handover_fails(self, pool):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await mirror_state(conn, "app.order_backfill_misses", ok_at=ago(hours=5))
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "FAIL" and "app.order_backfill_misses written by the hourly copy" in detail, \
            detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("table", ["bronze.orders", "bronze.order_products",
                                       "bronze.expenses"])
    async def test_a_backfill_after_the_handover_fails_naming_the_table(self, pool, table):
        """The hourly ids-diffs stamp `backfilled_at` on every complete run;
        after the handover one means DuckDB's rows were shipped over the
        chain's. Mutation: drop a table from `wanted`."""
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await backfilled(conn, table, ago(hours=3))
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "FAIL" and f"{table} backfilled out of DuckDB" in detail, detail

    @pytest.mark.asyncio
    async def test_the_chains_own_stamps_and_failures_are_not_a_copy(self, pool):
        """The chain stamps `last_ok_at` on the three bronze tables with the
        mirror's own statement, and its failing order step marks
        bronze.orders failing — neither is a copy. What the copies stamped
        before the handover is history. Mutation: judge `last_ok_at` or the
        failures on the bronze three."""
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await mirror_state(conn, "bronze.orders", ok_at=ago(minutes=1))
            await mirror_state(conn, "bronze.expenses", ok_at=ago(minutes=40),
                               attempted_at=ago(minutes=1), failures=3, error="boom")
            await mirror_state(conn, "bronze.order_products", ok_at=ago(minutes=1))
            await backfilled(conn, "bronze.orders", ago(days=2, minutes=30))
            await mirror_state(conn, "app.order_backfill_misses", ok_at=ago(days=2, minutes=5))
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "PASS" and "since the handover" in detail, detail

    @pytest.mark.asyncio
    async def test_a_failing_copy_after_the_handover_fails(self, pool):
        """`owned by Postgres since` is the latch with its flag put back: the
        copy stood down and said so on the one table it ships itself."""
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await mirror_state(conn, "app.order_backfill_misses", ok_at=ago(days=3),
                               attempted_at=ago(hours=1), failures=2,
                               error="owned by Postgres since 2030-06-03; "
                                     "run scripts/chain_copy_back.py")
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "FAIL" and "failing (2): owned by Postgres since" in detail, detail
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=2))
            await mirror_state(conn, "app.order_backfill_misses", ok_at=ago(days=4),
                               attempted_at=ago(days=3), failures=2, error="boom")
            v, _ = await verdict(conn, self.FILE, orders_on="1")
        assert v == "PASS"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("attempted,expected", [(timedelta(hours=23), "FAIL"),
                                                    (timedelta(hours=25), "PASS")])
    async def test_a_failure_is_judged_a_day_at_a_time(self, pool, attempted, expected):
        """Nothing resets `failures_since_ok` while the chain owns the table:
        only a successful copy does, and the copy stands down silently once
        the flag and the latch agree again. So a stamp after the handover
        stood for ever; now it counts for the day it was stamped, and a cause
        that keeps going restamps it every hour. Mutation: drop the 24 hours."""
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await order_owners(conn, ago(days=4))
            await mirror_state(conn, "app.order_backfill_misses", ok_at=ago(days=5),
                               attempted_at=NOW - attempted, failures=3,
                               error="owned by Postgres since 2030-06-01")
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == expected, detail

    @pytest.mark.asyncio
    async def test_without_owner_rows_the_flip_time_decides_and_without_it_unknown(self, pool):
        async with scenario(pool) as conn:
            await clean_orders(conn)
            await backfilled(conn, "bronze.expenses", ago(minutes=30))
            v, _ = await verdict(conn, self.FILE, orders_on="1",
                                 orders_flip_at=ago(hours=1).isoformat())
            assert v == "FAIL"
            v, detail = await verdict(conn, self.FILE, orders_on="1",
                                      orders_flip_at=ago(minutes=10).isoformat())
            assert v == "PASS" and "since the flip" in detail, detail
            v, detail = await verdict(conn, self.FILE, orders_on="1")
        assert v == "UNKNOWN" and "SOAK_ORDERS_FLIP_AT" in detail, detail


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


# ── H1/H2: the shadow chains (OD-02 (c)) ──────────────────────────────────────

SHADOW_TABLES = ("app.data_quality_runs", "app.data_quality_issues",
                 "app.data_quality_diffs", "app.disk_samples", "app.data_dir_samples",
                 "app.memory_samples", "app.weekly_report_sends",
                 "app.traffic_report_sends")
SHADOW_OFF = {"dq_journal_direct": "0", "watchdogs_on": "0", "weekly_ledger_on": "0",
              "traffic_ledger_on": "0"}


async def clean_shadow(conn):
    """No owner row and no copy stamp for any shadow chain's table — inside the
    scenario's transaction, so whatever another test left is only hidden."""
    await conn.execute("DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")
    await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                       list(SHADOW_TABLES))


async def shadow_owner(conn, table, at):
    await conn.execute(
        "INSERT INTO meta.chain_watermarks (key, value, updated_at) VALUES ($1, $2, $3) "
        "ON CONFLICT (key) DO UPDATE SET updated_at = EXCLUDED.updated_at",
        f"owner:{table}", at.isoformat(), at)


@needs_pg
class TestH1ShadowCopiesStoodDown:
    """The hourly copy must stay off a shadow chain's tables: its full replace
    out of DuckDB would delete every row whose shadow write failed — for a
    ledger, a delivered week, which the next tick then sends again."""

    FILE = "28_h1_shadow_copies_stood_down.sql"

    @pytest.mark.asyncio
    async def test_all_off_is_not_applicable(self, pool):
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            await mirror_state(conn, "app.memory_samples", ok_at=ago(minutes=10))
            v, detail = await verdict(conn, self.FILE, **SHADOW_OFF)
        assert v == "PASS" and detail.startswith("not applicable"), detail

    @pytest.mark.asyncio
    async def test_a_copy_after_the_handover_fails_naming_the_table(self, pool):
        """Mutation: judge the copy against the flip window alone, ignoring the
        owner row — a stamp an hour after a handover two days ago would read
        UNKNOWN for a copy that is plainly still running."""
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            await shadow_owner(conn, "app.memory_samples", ago(days=2))
            await mirror_state(conn, "app.memory_samples", ok_at=ago(hours=1))
            v, detail = await verdict(conn, self.FILE, **{**SHADOW_OFF, "watchdogs_on": "1"})
        assert v == "FAIL" and "app.memory_samples written by the copy" in detail, detail
        assert "chain 10" in detail

    @pytest.mark.asyncio
    async def test_a_copy_before_the_handover_passes(self, pool):
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            await shadow_owner(conn, "app.weekly_report_sends", ago(days=2))
            await mirror_state(conn, "app.weekly_report_sends", ok_at=ago(days=3))
            v, detail = await verdict(conn, self.FILE,
                                      **{**SHADOW_OFF, "weekly_ledger_on": "1"})
        assert v == "PASS" and "no copy since the handover" in detail, detail

    @pytest.mark.asyncio
    async def test_an_owner_row_outranks_a_flag_put_back(self, pool):
        """A lost marker with the flag at duckdb: the copy stands down on the
        owner row, so this judges it. Mutation: read the passed state alone."""
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            await shadow_owner(conn, "app.traffic_report_sends", ago(days=1))
            await mirror_state(conn, "app.traffic_report_sends", ok_at=ago(minutes=20))
            v, detail = await verdict(conn, self.FILE, **SHADOW_OFF)
        assert v == "FAIL" and "chain 11b" in detail, detail

    @pytest.mark.asyncio
    async def test_a_failure_after_the_handover_fails(self, pool):
        """`owned by Postgres since` is the latch with its flag put back —
        reported, and the way back named in the file's header."""
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            await shadow_owner(conn, "app.data_quality_runs", ago(days=2))
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(days=3),
                               attempted_at=ago(hours=1), failures=2,
                               error="not shipped: owned by Postgres since 2030-06-03")
            v, detail = await verdict(conn, self.FILE,
                                      **{**SHADOW_OFF, "dq_journal_direct": "1"})
        assert v == "FAIL" and "failing (2)" in detail, detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("attempted,expected", [(timedelta(hours=23), "FAIL"),
                                                    (timedelta(hours=25), "PASS")])
    async def test_a_failure_is_judged_a_day_at_a_time(self, pool, attempted, expected):
        """Only a successful copy resets `failures_since_ok`, and under the
        chain the copy stamps failing while the flag and the latch disagree,
        then stands down silently — so one episode after the handover failed
        H1 for ever. O1's rule (chain 3 review). Mutation: drop the 24 hours."""
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            await shadow_owner(conn, "app.weekly_report_sends", ago(days=4))
            await mirror_state(conn, "app.weekly_report_sends", ok_at=ago(days=5),
                               attempted_at=NOW - attempted, failures=3,
                               error="not shipped: owned by Postgres since 2030-06-01")
            v, detail = await verdict(conn, self.FILE,
                                      **{**SHADOW_OFF, "weekly_ledger_on": "1"})
        assert v == expected, detail

    @pytest.mark.asyncio
    async def test_without_an_owner_row_a_recent_stamp_is_unknown(self, pool):
        """The copy stamps the table every hour until the flip, so a healthy
        flip reads one inside the window until the chain's first write."""
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            await mirror_state(conn, "app.disk_samples", ok_at=ago(minutes=30))
            v, detail = await verdict(conn, self.FILE, **{**SHADOW_OFF, "watchdogs_on": "1"})
        assert v == "UNKNOWN" and "no owner row yet" in detail, detail

    @pytest.mark.asyncio
    async def test_invalid_fails_and_unknown_is_unknown(self, pool):
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            v, detail = await verdict(conn, self.FILE,
                                      **{**SHADOW_OFF, "traffic_ledger_on": "invalid"})
            assert v == "FAIL" and "KS_WRITE_TRAFFIC_LEDGER" in detail, detail
            v, detail = await verdict(conn, self.FILE,
                                      **{**SHADOW_OFF, "weekly_ledger_on": "unknown"})
        assert v == "UNKNOWN" and "could not read KS_WRITE_WEEKLY_LEDGER" in detail, detail


async def shadow_issue(conn, run_id, check_name, table, severity):
    await conn.execute(
        "INSERT INTO app.data_quality_issues (run_id, check_name, table_name, severity, count) "
        "VALUES ($1, $2, $3, $4, 1)", run_id, check_name, table, severity)


@needs_pg
class TestH2ShadowComparison:
    """The runbook's soak criterion for the shadow chains: zero `shadow_*`
    findings from the 07:30 comparison, read the way D8 reads its own."""

    FILE = "29_h2_shadow_comparison.sql"
    TODAY_0730 = datetime(2030, 6, 5, 7, 30, tzinfo=KYIV)
    ON = {**SHADOW_OFF, "dq_journal_direct": "1", "watchdogs_on": "1"}

    @pytest.mark.asyncio
    async def test_no_shadow_chain_on_is_not_applicable(self, pool):
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            v, detail = await verdict(conn, self.FILE, **SHADOW_OFF)
        assert v == "PASS" and detail.startswith("not applicable"), detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("check", ["shadow_missing_in_duckdb", "shadow_duckdb_only_rows",
                                       "shadow_row_values"])
    async def test_a_shadow_finding_fails_naming_it(self, pool, check):
        """Mutation: drop a severity from the filter, or the LIKE — each of
        the three counted findings must fail the soak."""
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            await dq_run(conn, DQ_RUN_IDS[0], layer="mirror_landing", started_at=self.TODAY_0730)
            severity = "WARN" if check == "shadow_missing_in_duckdb" else "CRITICAL"
            await shadow_issue(conn, DQ_RUN_IDS[0], check, "app.memory_samples", severity)
            v, detail = await verdict(conn, self.FILE, **self.ON)
        assert v == "FAIL" and check in detail, detail

    @pytest.mark.asyncio
    async def test_a_lagging_prune_and_other_findings_are_not_this_checks(self, pool):
        """INFO `shadow_pruned_rows` is DuckDB's prune behind Postgres's by a
        tick, and a non-shadow finding is D8's or the digest's. Mutation:
        count INFO — every edge sample would fail the soak."""
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            await dq_run(conn, DQ_RUN_IDS[0], layer="mirror_landing", started_at=self.TODAY_0730)
            await shadow_issue(conn, DQ_RUN_IDS[0], "shadow_pruned_rows", "app.disk_samples", "INFO")
            await shadow_issue(conn, DQ_RUN_IDS[0], "mirror_row_values", "bronze.products", "CRITICAL")
            v, detail = await verdict(conn, self.FILE, **self.ON)
        assert v == "PASS" and "zero shadow findings for chain 10, chain 9" in detail, detail

    @pytest.mark.asyncio
    async def test_no_run_is_d8s_fail_and_unknown_here(self, pool):
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            await conn.execute("DELETE FROM app.data_quality_runs WHERE layer = 'mirror_landing'")
            v, detail = await verdict(conn, self.FILE, **self.ON)
        assert v == "UNKNOWN" and "D8 says why" in detail, detail

    @pytest.mark.asyncio
    async def test_a_frozen_copy_of_the_journal_is_unknown(self, pool):
        """Chain 9 still at duckdb: the journal is a copy, and a copy that
        stopped shows yesterday's clean run. Mutation: drop the gate."""
        async with scenario(pool) as conn:
            await clean_shadow(conn)
            await dq_run(conn, DQ_RUN_IDS[0], layer="mirror_landing", started_at=self.TODAY_0730)
            await mirror_state(conn, "app.data_quality_runs", ok_at=ago(hours=3))
            v, detail = await verdict(conn, self.FILE,
                                      **{**SHADOW_OFF, "weekly_ledger_on": "1"})
        assert v == "UNKNOWN" and "min old" in detail, detail


# ── the owner rows date the handover before the flip time does ───────────────
# Every check that dates a chain's handover by its owner rows and only then by
# the operator's flip time, each with the one thing it counts after the
# handover. Both given, the owner rows win: they are the chain's first write,
# on the clock the shippers stamp with, and a flip time is what somebody typed.
# The review of the chain 3 soak checks found I1's order survived every I1
# test — swap `owner.at` and `flag.flip_at` and nothing failed — and the same
# swap then survived B1's, M1's, O1's and O4's (2026-10-10). Walked, not
# listed: a new file on that clock with no arrangement here fails
# `test_every_handover_clock_is_pinned`.


async def _i1_between(conn, latch, problem_at):
    await clean_inventory(conn)
    await inventory_owners(conn, latch)
    await mirror_state(conn, "app.stock_movements", ok_at=problem_at)


async def _b1_between(conn, latch, problem_at):
    await clean_buyers(conn)
    await owners(conn, latch)
    await mirror_state(conn, "app.buyer_gender", ok_at=problem_at)


async def _m1_between(conn, latch, problem_at):
    await clean_managers(conn)
    await manager_owners(conn, latch)
    await mirror_state(conn, "app.manager_classifications", ok_at=problem_at)


async def _o1_between(conn, latch, problem_at):
    await clean_orders(conn)
    await order_owners(conn, latch)
    await mirror_state(conn, "app.order_backfill_misses", ok_at=problem_at)


async def _o4_between(conn, latch, problem_at):
    await clean_orders(conn)
    await order_owners(conn, latch)
    await chain_order(conn, CHAIN3_IDS[0], lines=0, mirrored_at=problem_at,
                      created_at=problem_at - timedelta(hours=2))


# file -> (the flag that says the chain writes Postgres, its flip-time
# variable, the arrangement: owner rows at `latch`, one problem at `problem_at`)
HANDOVER_CLOCKS = {
    "15_i1_inventory_copy_stood_down.sql": ("inventory_on", "inventory_flip_at", _i1_between),
    "23_b1_buyers_copies_stood_down.sql": ("buyers_on", "buyers_flip_at", _b1_between),
    "30_m1_managers_copy_stood_down.sql": ("managers_on", "managers_flip_at", _m1_between),
    "32_o1_orders_copies_stood_down.sql": ("orders_on", "orders_flip_at", _o1_between),
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
