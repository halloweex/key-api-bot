"""Step 13 as code with no mode (step 14's S14-15).

Step 13 — Postgres alone derives Silver, Gold and the UTM verdicts — has been
in force since 2026-10-09 and a constant since S14-00. S14-15 deletes what
the switch was made of: `core/warehouse_cutover.py` (the mode, the
preconditions and the readiness report, the recorded writer and its settle,
the way back and its refusal, the UTM re-parse marker), `core/pg_order_utm.py`
(the copy of DuckDB's UTM parse), DuckDB's own warehouse rebuild and its dirty
flag, `KS_PG_DERIVE` and `KS_UTM_PARSE` with their health blocks and canary
checks, and the Alert Gate's marks API. What is pinned here:

- the deleted names are gone and nothing in production reaches for them;
- the three variables it stops reading cost a start nothing, whatever they
  say (rule 3), and the derivation is registered regardless;
- `/api/health` and its model carry neither block, and `derivation` neither
  `mode` nor `error`; the canary judges neither, and the six conditions are
  out of the registry and the canary's tables (rule 8);
- the admin levers run the Postgres derivation alone, and the status page
  answers it alone;
- the UTM doors parse Postgres alone, and their answer is the router's;
- the marks on `data/alert-gate-*.json` entries survive the Gate rewriting
  the file: an image from before step 14 reads them.

The on-disk record (`warehouse_writer` in `app.sync_metadata`) and the marker
(`data/warehouse-utm-reparsed-at`) are left as they are: no production string
names either for a write (`TestWhatIsGone`). That the canary no longer judges
the blocks an image from before step 14 still publishes is in
`tests/unit/test_canary.py`.
"""
from __future__ import annotations

import ast
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

REPO = Path(__file__).resolve().parents[2]
PRODUCTION_ROOTS = ("core", "web", "bot", "scripts", "deploy")

DELETED_MODULES = ("core.warehouse_cutover", "core.pg_order_utm")
# Names that left production in S14-15, by where they lived.
DELETED_ATTRS = {
    "core.duckdb_store": ("GOLD_REVENUE_SELECT_SQL", "DUCKDB_GOLD_COLUMNS",
                          "GOLD_SOURCE_MEASURES", "gold_select_items",
                          "MAX_VALIDATION_RETRIES", "STUCK_REBUILD_COOLDOWN_SECONDS",
                          "UTM_DATE_LOOKUP_LIMIT"),
    "core.pg_derivation": ("ENV", "PIGGYBACK", "OWN", "configure_mode", "mode",
                           "mode_error", "owns", "mark_if_owned"),
    "core.pg_utm_parse": ("ENV", "DUCKDB", "POSTGRES", "configure_mode", "mode",
                          "mode_error", "parses_in_postgres"),
    "core.pg_landing": ("enabled", "mirror_on"),
    "core.alerting": ("mark_delivered", "delivered_marked"),
    "core.data_quality": ("REMEDIATION_UTM_PARSED_IN_POSTGRES",),
    "core.mirror_reconciliation": ("_ORDER_UTM_LEVER_REASONS_POSTGRES",),
    "bot.canary": ("check_derivation_mode", "check_warehouse_writer_mode",
                   "check_warehouse_preconditions", "check_warehouse_way_back_refused",
                   "check_warehouse_hold", "check_utm_parse_mode",
                   "WAREHOUSE_HOLD_WARN_S"),
}
DELETED_STORE_METHODS = (
    "refresh_warehouse_layers", "get_warehouse_status", "mark_warehouse_dirty",
    "peek_warehouse_dirty", "clear_warehouse_dirty", "consume_warehouse_dirty",
    "_claim_stuck_rebuild_slot", "_count_consecutive_refresh_failures",
    "refresh_utm_silver_layer", "_parse_utm_into_silver",
)
DELETED_SCHEDULER_METHODS = (
    "_settle_warehouse_writer", "_run_warehouse_refresh",
    "_rebuild_postgres_layers", "_rebuild_pg_layers",
)
# The conditions the switch paged under. Out of the registry and the canary's
# tables; the names are not reused (rule 15).
RETIRED_CONDITIONS = (
    "warehouse_mode_invalid", "warehouse_preconditions_unmet",
    "warehouse_way_back_refused", "warehouse_hold_stuck",
    "utm_parse_mode_invalid", "derivation_mode_invalid",
)
STOPPED_READING = ("KS_WRITE_WAREHOUSE", "KS_UTM_PARSE", "KS_PG_DERIVE",
                   "KS_MIRROR_LANDING")
JUNK = ("", "postgress", "duckdb", "piggyback", "off", "0", "\x7f", "x" * 300)


def _trees():
    for root in PRODUCTION_ROOTS:
        for path in sorted((REPO / root).rglob("*.py")):
            yield str(path.relative_to(REPO)), ast.parse(path.read_text())


def _unwrap(fn):
    while hasattr(fn, "__wrapped__"):
        fn = fn.__wrapped__
    return fn


# ─── what is gone ────────────────────────────────────────────────────────────


class TestWhatIsGone:
    @pytest.mark.parametrize("module", DELETED_MODULES)
    def test_the_module_is_deleted(self, module):
        path = REPO / Path(*module.split(".")).with_suffix(".py")
        assert not path.exists(), path
        with pytest.raises(ImportError):
            __import__(module)

    @pytest.mark.parametrize("module, names", sorted(DELETED_ATTRS.items()))
    def test_the_names_are_deleted(self, module, names):
        import importlib

        mod = importlib.import_module(module)
        left = [n for n in names if hasattr(mod, n)]
        assert left == [], f"{module} still has {left}"

    def test_the_store_and_the_scheduler_lost_their_methods(self):
        from core.duckdb_store import DuckDBStore
        from core.scheduler import BackgroundScheduler

        assert [m for m in DELETED_STORE_METHODS if hasattr(DuckDBStore, m)] == []
        assert [m for m in DELETED_SCHEDULER_METHODS
                if hasattr(BackgroundScheduler, m)] == []

    def test_nothing_in_production_calls_or_imports_them(self):
        """By attribute and by bare name, anywhere under the production roots:
        `store.refresh_warehouse_layers(...)` is a call `test_imports_resolve`
        cannot see. `scripts/compact_duckdb.py` mentions one in its docstring
        (do-not-touch, gone in S14-18); a docstring is not a call."""
        names = (set(DELETED_STORE_METHODS) | set(DELETED_SCHEDULER_METHODS)
                 | {"settle_writer", "note_utm_reparsed_in_postgres",
                    "ship_order_utm", "ship_after_reparse", "duckdb_derives",
                    "mark_if_owned", "parses_in_postgres", "mark_delivered",
                    "delivered_marked"})
        found = []
        for rel, tree in _trees():
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    func = node.func
                    name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
                    if name in names:
                        found.append((rel, node.lineno, name))
                elif isinstance(node, (ast.Import, ast.ImportFrom)):
                    mods = ([a.name for a in node.names] if isinstance(node, ast.Import)
                            else [node.module or ""] + [f"{node.module}.{a.name}" for a in node.names])
                    if any(m in DELETED_MODULES for m in mods):
                        found.append((rel, node.lineno, "import"))
        assert found == [], found

    def test_no_production_code_names_the_marker_or_the_record_for_a_write(self):
        """Both stay on disk for an image from before step 14. Nothing here
        writes, renames or removes them: the marker's name is in no string,
        and the record's key in no statement that writes."""
        hits = []
        for rel, tree in _trees():
            docs = {id(n.body[0].value) for n in ast.walk(tree)
                    if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                      ast.AsyncFunctionDef))
                    and n.body and isinstance(n.body[0], ast.Expr)
                    and isinstance(n.body[0].value, ast.Constant)}
            for node in ast.walk(tree):
                if id(node) in docs:
                    continue
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    text = node.value
                    if "warehouse-utm-reparsed-at" in text:
                        hits.append((rel, node.lineno, "marker"))
                    if "warehouse_writer" in text and any(
                            verb in text.upper() for verb in ("INSERT", "UPDATE", "DELETE")):
                        hits.append((rel, node.lineno, "record"))
        assert hits == [], hits


# ─── rule 3: what is no longer read cannot stop a start ──────────────────────


class TestTheVariablesNoLongerRead:
    @pytest.mark.parametrize("value", JUNK)
    def test_configure_modes_neither_reads_nor_minds_them(self, monkeypatch, value):
        from core.runtime_modes import configure_modes

        for name in STOPPED_READING:
            monkeypatch.setenv(name, value)
        modes = configure_modes(web=True)
        assert set(modes) == {"KS_DUCKDB", "KS_READ_FALLBACK"}, modes

    @pytest.mark.parametrize("value", ("", "postgress", "duckdb", "piggyback"))
    def test_the_derivation_is_registered_whatever_they_say(self, monkeypatch, value):
        from core.scheduler import BackgroundScheduler

        for name in STOPPED_READING:
            monkeypatch.setenv(name, value)
        sched = BackgroundScheduler()
        ids = []
        monkeypatch.setattr(sched, "_add_job", lambda **kw: ids.append(kw["job_id"]))
        asyncio.run(sched._register_jobs())
        assert "pg_warehouse_derive" in ids
        assert "warehouse_refresh" not in ids
        assert "warehouse_refresh" not in BackgroundScheduler._HEAVY_JOBS

    def test_web_imports_in_a_fresh_interpreter_with_junk_in_them(self, tmp_path):
        env = {"PATH": os.environ.get("PATH", ""), "TZ": "UTC",
               "PYTHONPATH": str(REPO), "PYTHONDONTWRITEBYTECODE": "1",
               "KS_ALERTS_DISABLED": "1", "KS_ALERT_GATE_STATE_DIR": "",
               "DASHBOARD_SECRET_KEY": "test-signing-key-not-a-real-secret",
               "BOT_TOKEN": "123456:test-bot-token-not-a-real-secret",
               "ADMIN_USER_IDS": "1"}
        env.update({name: "postgress" for name in STOPPED_READING})
        code = ("import web.main\n"
                "from core.runtime_modes import configure_modes\n"
                "print(sorted(configure_modes(web=True)))\n")
        done = subprocess.run([sys.executable, "-c", code], cwd=tmp_path, env=env,
                              capture_output=True, text=True, timeout=120)
        assert done.returncode == 0, done.stderr[-2000:]
        assert done.stdout.strip().splitlines()[-1] == "['KS_DUCKDB', 'KS_READ_FALLBACK']"


# ─── /api/health and the canary (rules 7 and 8) ──────────────────────────────


class TestHealthAndTheCanary:
    def test_the_model_carries_neither_block(self):
        from web.schemas import HealthResponse

        fields = set(HealthResponse.model_fields)
        assert not fields & {"warehouse_writer_mode", "utm_parse"}
        assert "derivation" in fields and "mirrors" in fields

    def test_the_derivation_block_has_no_mode(self, monkeypatch):
        from web.routes.api import health

        async def nothing_read():
            return dict(health._NOT_ASKED)

        monkeypatch.setattr(health, "_dropped_marks", nothing_read)
        block = asyncio.run(health._derivation_block())
        assert set(block) == {"signal_unreadable_ticks", "marks_dropped_unhealed",
                              "last_mark_drop_age_s"}

    def test_the_handler_builds_neither_block(self):
        src = (REPO / "web" / "routes" / "api" / "health.py").read_text()
        tree = ast.parse(src)
        keys = {node.value for node in ast.walk(tree)
                if isinstance(node, ast.Constant) and isinstance(node.value, str)}
        assert not keys & {"warehouse_writer_mode", "utm_parse"}

    def test_the_mirrors_watch_the_derived_tables_and_the_utm_table(self, monkeypatch):
        """Unconditionally since S14-15, with their own limits."""
        from core import pg_derivation, pg_utm_parse
        from web.routes.api import health

        seen = {}

        async def fetch(pool, tables):
            seen["tables"] = tables
            return {t: {"age_seconds": 1} for t in tables}

        monkeypatch.setattr(health, "_mirror_cache", {"data": None, "expires_at": 0})
        monkeypatch.setattr(health, "_mirror_cache_lock", asyncio.Lock())
        with patch("core.mirror_reconciliation.fetch_mirror_freshness", new=fetch), \
             patch("core.pg.get_pool", new=AsyncMock(return_value=object())):
            data = asyncio.run(health._mirror_freshness())
        for table in pg_derivation.DERIVED_TABLES:
            assert data[table]["max_age_s"] == pg_derivation.DERIVED_MAX_AGE_S
        assert data[pg_utm_parse.UTM_TABLE]["max_age_s"] == pg_utm_parse.MAX_AGE_S

    def test_the_conditions_are_out_of_the_registry_and_the_canary(self):
        from bot import canary
        from core.alerting import REGISTRY

        tables = {k for k, _ in canary._ACTIONS} | {k for k, _ in canary._CRITICAL_TITLES}
        for key in RETIRED_CONDITIONS:
            assert key not in REGISTRY, key
            assert not any(key.startswith(t) or t.startswith(key) for t in tables), key


# ─── the admin levers ────────────────────────────────────────────────────────


class _Scheduler:
    def __init__(self):
        self._heavy_job_lock = asyncio.Lock()
        self.derived = []

    async def _run_pg_derivation(self, *, trigger=None, force=False):
        self.derived.append((trigger, force))
        return {"status": "success", "trigger": trigger}


class TestTheAdminLevers:
    def _call(self, route, **kwargs):
        from web.routes.api import admin

        scheduler = _Scheduler()
        store = MagicMock()
        store.connection = MagicMock(side_effect=AssertionError("DuckDB touched"))
        with patch("core.scheduler.get_scheduler", return_value=scheduler), \
             patch.object(admin, "get_store", AsyncMock(return_value=store)):
            result = asyncio.run(_unwrap(getattr(admin, route))(MagicMock(), **kwargs))
        return result, scheduler

    def test_refresh_derives_postgres_alone(self):
        result, scheduler = self._call("refresh_warehouse", admin={})
        assert result == {"trigger": "manual",
                          "postgres": {"status": "success", "trigger": "manual"}}
        assert scheduler.derived == [("manual", True)]

    def test_rebuild_silver_derives_postgres_alone(self):
        result, scheduler = self._call("rebuild_silver_from_scratch", admin={})
        assert result == {"postgres": {"status": "success", "trigger": "manual"}}
        assert scheduler.derived == [("manual", True)]

    def test_the_status_page_answers_the_derivation_alone(self, monkeypatch):
        from web.routes.api import admin

        async def status():
            return {"requested": 3, "built": 3, "owed": False}

        monkeypatch.setattr(admin, "_pg_derivation_status", status)
        result, _ = self._call("get_warehouse_status")
        assert result == {"writer": "postgres",
                          "postgres": {"requested": 3, "built": 3, "owed": False}}


# ─── the UTM doors ───────────────────────────────────────────────────────────


class TestTheUtmDoors:
    def _door(self, name, router):
        from web.routes.api import traffic

        with patch.object(traffic, "reparse_router", router):
            return asyncio.run(_unwrap(getattr(traffic, name))(request=None, user={}))

    def test_the_reclassify_replaces_the_table_whole(self):
        router = AsyncMock(return_value={"rows": 5, "replaced": 4, "duration_ms": 1})
        result = self._door("reclassify_traffic", router)
        assert result == {"success": True, "utm_records": 5, "engine": "postgres"}
        router.assert_awaited_once_with(full=True)

    def test_the_refresh_only_adds(self):
        router = AsyncMock(return_value={"parsed": 2, "rows": 9, "duration_ms": 1})
        result = self._door("refresh_traffic_data", router)
        assert result == {"success": True, "utm_orders_parsed": 2, "engine": "postgres"}
        router.assert_awaited_once_with(full=False)

    @pytest.mark.parametrize("answer, status", [
        ({"error": "LockNotAvailableError: lock timeout"}, 500),
        ({"skipped": "KS_PG_DSN is not set"}, 500),
        ({"refused": "0 rows would replace 9"}, 409),
    ])
    @pytest.mark.parametrize("door", ("reclassify_traffic", "refresh_traffic_data"))
    def test_postgres_failing_is_the_doors_answer(self, door, answer, status):
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as raised:
            self._door(door, AsyncMock(return_value=answer))
        assert raised.value.status_code == status


# ─── the Gate keeps the marks an older image reads ───────────────────────────


class TestTheMarksSurviveTheGate:
    def test_a_rewrite_keeps_them(self, tmp_path):
        from core.alerting import AlertGate

        path = tmp_path / "alert-gate-web.json"
        path.write_text(json.dumps({"buckets": {}, "delivered": {
            "mirror_row_values": {"group": "dq:mirror_landing", "first_delivered": 1.0,
                                  "marks": ["warehouse_comparison"]}}}))
        gate = AlertGate(state_path=path)
        gate.note_delivered_conditions(["mirror_row_values", "pg_signal_missed"],
                                       "dq:mirror_landing")
        delivered = json.loads(path.read_text())["delivered"]
        assert delivered["mirror_row_values"]["marks"] == ["warehouse_comparison"]
        assert delivered["mirror_row_values"]["first_delivered"] == 1.0
        assert "marks" not in delivered["pg_signal_missed"]

    def test_a_restart_reads_them_back(self, tmp_path):
        from core.alerting import AlertGate

        path = tmp_path / "alert-gate-web.json"
        path.write_text(json.dumps({"buckets": {}, "delivered": {
            "x": {"group": "g", "first_delivered": 2.0, "marks": ["m"]}}}))
        AlertGate(state_path=path).note_delivered_conditions(["y"], "g")
        again = AlertGate(state_path=path)
        assert again._delivered["x"]["marks"] == ["m"]


class TestTheResolveNote:
    """`resolve_group(note=...)`: one more line under the list. Its one caller
    was step 13's settle, which closed the `warehouse` group once on the
    switch; kept as the Gate's own behaviour (moved here from the deleted
    `tests/unit/test_warehouse_writer.py`, on a condition still registered:
    the `warehouse:` ones it used went with DuckDB's validator)."""

    def test_it_is_one_more_line_under_the_list(self):
        from core.alerting import _gate, resolve_group

        _gate.note_delivered_conditions(["warehouse_pg:derive_failed"], "warehouse_pg")
        with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=True)), \
             patch("bot.main.send_admin_message", AsyncMock(return_value=1)) as send:
            asyncio.run(resolve_group("warehouse_pg", note="retired"))
        text = send.await_args.args[0]
        assert text.startswith("✅ Resolved:") and text.endswith("\nretired")

    def test_without_it_nothing_changes(self):
        from core.alerting import _gate, resolve_group

        _gate.note_delivered_conditions(["warehouse_pg:derive_failed"], "warehouse_pg")
        with patch("core.alert_archive.write_resolved_now", AsyncMock(return_value=True)), \
             patch("bot.main.send_admin_message", AsyncMock(return_value=1)) as send:
            asyncio.run(resolve_group("warehouse_pg"))
        assert send.await_args.args[0].splitlines()[-1].startswith("• warehouse_pg:")
