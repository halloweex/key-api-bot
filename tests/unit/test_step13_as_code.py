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
- the UTM doors parse Postgres alone, and their answer is the router's;
- the marks on `data/alert-gate-*.json` entries survive the Gate rewriting
  the file: an image from before step 14 reads them.

The on-disk record (`warehouse_writer` in `app.sync_metadata`) and the marker
(`data/warehouse-utm-reparsed-at`) are left as they are: no production string
names either for a write (`TestWhatIsGone`). That the canary no longer judges
the blocks an image from before step 14 still publishes is in
`tests/unit/test_canary.py`; that the admin levers run the Postgres derivation
and the status page reads it, in
`tests/integration/test_admin_levers_reach_pg_derivation.py`.
"""
from __future__ import annotations

import ast
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

REPO = Path(__file__).resolve().parents[2]
PRODUCTION_ROOTS = ("core", "web", "bot", "scripts", "deploy")

# The conditions the switch paged under. Out of the registry and the canary's
# tables; the names are not reused (rule 15).
RETIRED_CONDITIONS = (
    "warehouse_mode_invalid", "warehouse_preconditions_unmet",
    "warehouse_way_back_refused", "warehouse_hold_stuck",
    "utm_parse_mode_invalid", "derivation_mode_invalid",
)
STOPPED_READING = ("KS_WRITE_WAREHOUSE", "KS_UTM_PARSE", "KS_PG_DERIVE",
                   "KS_MIRROR_LANDING")


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
                "print(sorted(configure_modes()))\n")
        done = subprocess.run([sys.executable, "-c", code], cwd=tmp_path, env=env,
                              capture_output=True, text=True, timeout=120)
        assert done.returncode == 0, done.stderr[-2000:]
        assert done.stdout.strip().splitlines()[-1] == "[]"


# ─── /api/health and the canary (rules 7 and 8) ──────────────────────────────


class TestHealthAndTheCanary:

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
