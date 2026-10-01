"""The step-13 rehearsal's helper. Runs INSIDE the rehearsal's own containers.

`deploy/step13_rehearsal.sh` mounts this file read-only at `/reh/probe.py` into
`reh-web` (to talk to the process under test over `localhost:8080`, which it
publishes nowhere) and into the one-off `reh-probe` (to read the DuckDB copy
while `reh-web` is stopped — DuckDB's file lock is exclusive, so never while it
runs). It is mounted rather than baked so the production image, which does not
carry it, can run it.

Two halves:

- **Subcommands that collect evidence** (`api`, `snapshot`, `canary`,
  `duckdb-facts`, ...). Each prints JSON and never prints a credential: the
  admin session is minted from the rehearsal's own per-run
  `DASHBOARD_SECRET_KEY`, used for one request to `localhost`, and dropped.
- **Judges** (`judge_p1` … `judge_p8`, `judge_k0`, `judge_z0`): pure functions
  over that evidence, returning `(verdict, detail)` with verdict one of PASS,
  FAIL, UNKNOWN. `tests/unit/test_step13_rehearsal_probe.py` holds each to its
  three outcomes. A detail carries ids, counts, keys and times — never a name,
  phone or comment out of the copy.

Python's standard library only at import time, so the tests can load this
file without the application on the path.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

PASS, FAIL, UNKNOWN = "PASS", "FAIL", "UNKNOWN"
Verdict = Tuple[str, str]

BASE = "http://localhost:8080"
APP_DIR = os.environ.get("REH_APP_DIR", "/app")

# The one page the rehearsal seeds so that P1's resolve is not vacuous: under
# KS_ALERTS_DISABLED nothing is ever delivered, so without it `resolve_group`
# takes nothing and journals nothing, and "one resolve" would be proved by
# zero. A registered condition of the `warehouse` group (core/alerting.py).
SEEDED_KEY = "warehouse:validation_retrying"
SEEDED_GROUP = "warehouse"

# The note the switch's resolve carries (core.warehouse_cutover.RESOLVE_NOTE),
# matched by its stable head.
RESOLVE_NOTE_HEAD = "DuckDB derivation retired (KS_WRITE_WAREHOUSE=postgres)"

# The five DuckDB checks that stand down (core.warehouse_cutover).
STOOD_DOWN = ("attribution_coverage", "goods_shipped_without_sale", "gold_cell_values",
              "headline_vs_line_items", "silver_arc")
# The twins' groups whose DuckDB guard stands down, so the twins file alone.
STANDALONE_GROUPS = ("silver_arc", "attribution", "line_items")
RETIRED_GOLD = ("gold_missing_cells", "gold_orphan_cells", "gold_cell_values")


def _app_path() -> None:
    if APP_DIR not in sys.path:
        sys.path.insert(0, APP_DIR)


# ─── HTTP into the process under test ────────────────────────────────────────

def _admin_id() -> int:
    """An id `_resolve_session` short-circuits as a hardcoded admin —
    `core.permissions.ADMIN_USER_IDS`, a literal in the code, not the
    environment's ADMIN_USER_IDS (which the rehearsal sets to a recipient
    nobody is, and which decides alert recipients, not sessions)."""
    _app_path()
    from core.permissions import ADMIN_USER_IDS

    return min(int(i) for i in ADMIN_USER_IDS)


def _session() -> str:
    """A session for the hardcoded admin `_resolve_session` short-circuits,
    signed by this run's key — which exists only in reh-web's environment, so
    no other process accepts it. Never printed, and sent only to localhost
    inside reh-web, which publishes no port."""
    from itsdangerous import URLSafeTimedSerializer

    return URLSafeTimedSerializer(os.environ["DASHBOARD_SECRET_KEY"]).dumps(
        {"user_id": _admin_id()})


def api(method: str, path: str, *, timeout: float = 600.0) -> Tuple[int, Any]:
    """One request to the process under test. `(status, body)`; body is the
    parsed JSON when it is JSON. A connection that fails is status 0."""
    import urllib.error
    import urllib.request

    request = urllib.request.Request(BASE + path, method=method.upper(), data=b"" if method.upper() == "POST" else None)
    request.add_header("Cookie", f"dashboard_session={_session()}")
    request.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as exc:
        status, raw = exc.code, exc.read()
    except Exception as exc:  # noqa: BLE001 — a refused connection is an answer here
        return 0, {"error": type(exc).__name__}
    try:
        return status, json.loads(raw.decode("utf-8") or "null")
    except ValueError:
        return status, {"text": raw.decode("utf-8", "replace")[:500]}


def _get(path: str) -> Any:
    status, body = api("GET", path, timeout=60)
    return body if status == 200 else None


def _reduce_status(w: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """`/api/warehouse/status`, the fields the judges read."""
    if not isinstance(w, Mapping):
        return None
    cut = w.get("cutover") or {}
    frozen = w.get("duckdb_frozen") or {}
    pg = w.get("postgres") or {}
    return {
        "writer": w.get("writer"),
        "last_refresh": w.get("last_refresh"),
        "last_trigger": w.get("last_trigger"),
        "validation_passed": w.get("validation_passed"),
        "frozen_last_refresh": frozen.get("last_refresh") if frozen else None,
        "postgres": {k: pg.get(k) for k in ("requested", "built", "owed", "built_at")},
        "cutover": {
            "mode": cut.get("mode"), "value": cut.get("value"),
            "preconditions_met": cut.get("preconditions_met"),
            "unmet": [u.get("key") for u in cut.get("unmet") or [] if isinstance(u, Mapping)],
            "writer": cut.get("writer"), "held": cut.get("held"),
            "reclassify_needed": cut.get("reclassify_needed"),
            "stood_down_duckdb_checks": cut.get("stood_down_duckdb_checks"),
            "readiness_error": cut.get("readiness_error"),
        },
    }


def snapshot() -> Dict[str, Any]:
    """`/api/health`, `/api/warehouse/status` and the job ids, reduced."""
    status, health = api("GET", "/api/health", timeout=60)
    jobs = _get("/api/jobs") or {}
    h = health if isinstance(health, Mapping) else {}
    return {
        "t": datetime.now(timezone.utc).isoformat(),
        "health_code": status,
        "health": {k: h.get(k) for k in ("status", "version", "uptime_seconds",
                                         "warehouse_writer_mode", "derivation",
                                         "read_fallback_mode", "utm_parse")},
        "status": _reduce_status(_get("/api/warehouse/status")),
        "jobs": sorted(j.get("id") for j in jobs.get("jobs") or [] if isinstance(j, Mapping)),
    }


def canary() -> Dict[str, Any]:
    """The bot's canary, judged against this process — never sent. An http
    URL, so no certificate probe."""
    import asyncio

    _app_path()
    from bot.canary import check_warehouse_preconditions, run_canary

    result = asyncio.run(run_canary(BASE))
    _status, health = api("GET", "/api/health", timeout=60)
    return {
        "severity": result.severity,
        "failure_keys": list(result.failure_keys),
        "preconditions_keys": [k for k, _ in check_warehouse_preconditions(
            health if isinstance(health, Mapping) else None)],
    }


def wait_health(timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, _ = api("GET", "/api/health", timeout=10)
        if status == 200:
            return True
        time.sleep(3)
    return False


def dq_last(layer: str) -> Optional[Dict[str, Any]]:
    """The newest run of a DQ layer, as `/api/health/data-quality` has it."""
    body = _get("/api/health/data-quality") or {}
    block = body.get(layer) if isinstance(body, Mapping) else None
    run = (block or {}).get("last_run") if isinstance(block, Mapping) else None
    if not isinstance(run, Mapping):
        return None
    return {k: run.get(k) for k in ("run_id", "started_at", "ended_at", "status",
                                    "error_message", "critical_count", "warn_count")}


def _job(job_id: str) -> Optional[Dict[str, Any]]:
    body = _get("/api/jobs") or {}
    for job in body.get("jobs") or []:
        if isinstance(job, Mapping) and job.get("id") == job_id:
            return dict(job)
    return None


def trigger_and_wait(job_id: str, timeout: float) -> Dict[str, Any]:
    """`POST /api/jobs/{id}/trigger`, then wait until the scheduler has
    recorded one more execution of it — success or error. The trigger only
    moves `next_run_time`, so completion is read back from `/api/jobs`, whose
    schema carries `last_run` (stamped by the execution listener, success or
    error) and not the run counters."""
    before = _job(job_id)
    if before is None:
        return {"job": job_id, "done": False, "why": "not registered"}
    status, _body = api("POST", f"/api/jobs/{job_id}/trigger", timeout=60)
    if status != 200:
        return {"job": job_id, "done": False, "why": f"trigger answered {status}"}
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(3)
        now = _job(job_id) or {}
        if now.get("last_run") and now.get("last_run") != before.get("last_run"):
            return {"job": job_id, "done": True, "last_status": now.get("last_status"),
                    "last_run": now.get("last_run")}
    return {"job": job_id, "done": False, "why": f"no execution within {timeout:g} s"}


def wait_dq(layer: str, after: int, timeout: float) -> Optional[Dict[str, Any]]:
    """The first run of `layer` with an id above `after` that has ended."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = dq_last(layer)
        if run and isinstance(run.get("run_id"), int) and run["run_id"] > after \
                and run.get("ended_at"):
            return run
        time.sleep(5)
    return None


# The rehearsal's one order: four versions of it, each a status KeyCRM really
# uses with its group (core.data_quality.KNOWN_STATUS_IDS), so every version is
# an ordinary order to every check and only its content moves.
FIXTURE_VERSIONS = {1: (1, 1), 2: (2, 1), 3: (8, 4), 4: (9, 4)}


def fixture(order_id: int, version: int, out_dir: str, product_id: Optional[int]) -> Dict[str, Any]:
    """Write the one order the stub serves, as version `version`: same id, a new
    status and a new `updated_at`, so the next incremental sync lands a real
    change. Atomic, so the stub never reads half a file. Invented, never a
    customer: no buyer, no phone, no name."""
    from datetime import timedelta

    status_id, group = FIXTURE_VERSIONS[version]
    now = datetime.now(timezone.utc).replace(microsecond=0)
    ordered = now - timedelta(hours=1)
    line = {"id": order_id * 10 + 1, "name": "reh-step13 fixture", "quantity": 1,
            "price_sold": "120.00"}
    if product_id:
        line["offer"] = {"product_id": product_id, "sku": None}
    payload = {
        "id": order_id, "source_id": 1, "status_id": status_id, "status_group_id": group,
        "grand_total": "120.00",
        "ordered_at": ordered.isoformat(), "created_at": ordered.isoformat(),
        "updated_at": (now + timedelta(seconds=version)).isoformat(),
        "buyer": None, "manager": None,
        "manager_comment": "utm_source=instagram&utm_medium=cpc&utm_campaign=reh_step13",
        "promocode": None, "products": [line], "expenses": [],
    }
    path = Path(out_dir) / "order.json"
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps([payload]))
    os.chmod(tmp, 0o644)
    os.replace(tmp, path)
    return {"id": order_id, "version": version, "status_id": status_id,
            "updated_at": payload["updated_at"]}


def readers() -> Dict[str, Any]:
    """The env names the image itself declares, so the env list cannot drift
    from the code under test."""
    _app_path()
    from core import warehouse_cutover as wc
    from core.write_chains import WRITE_CHAINS

    return {"readers": list(wc.WAREHOUSE_READERS), "cohorts": wc.COHORTS,
            "chain_envs": sorted({c.WRITE_ENV for c in WRITE_CHAINS})}


def keycrm_url() -> str:
    _app_path()
    from core.keycrm import KEYCRM_BASE_URL

    return KEYCRM_BASE_URL


# ─── The DuckDB copy, read while reh-web is stopped ──────────────────────────

def duckdb_facts(db: str, gate: Optional[str], runs: Sequence[int]) -> Dict[str, Any]:
    """What the judges need out of the DuckDB copy. Read-only; never while
    the process under test holds the file."""
    import duckdb

    con = duckdb.connect(db, read_only=True)
    try:
        con.execute("SET memory_limit='512MB'")

        def one(sql: str, params: Sequence[Any] = ()) -> Any:
            row = con.execute(sql, list(params)).fetchone()
            return row

        dirty = one("SELECT value, CAST(updated_at AS VARCHAR) FROM sync_metadata "
                    "WHERE key = 'warehouse_dirty'")
        writer = one("SELECT value FROM sync_metadata WHERE key = 'warehouse_writer'")
        refresh = one("SELECT CAST(max(refreshed_at) AS VARCHAR), count(*) FROM warehouse_refreshes")
        last = one("SELECT trigger, validation_passed, CAST(refreshed_at AS VARCHAR) "
                   "FROM warehouse_refreshes ORDER BY id DESC LIMIT 1")
        counts = one("SELECT (SELECT count(*) FROM orders), (SELECT max(id) FROM orders), "
                     "(SELECT count(*) FROM silver_orders), (SELECT count(*) FROM silver_order_utm)")
        out_runs: Dict[str, Any] = {}
        for run_id in runs:
            row = one("SELECT run_id, layer, status, error_message, CAST(started_at AS VARCHAR), "
                      "CAST(ended_at AS VARCHAR) FROM data_quality_runs WHERE run_id = ?", [run_id])
            if row is None:
                out_runs[str(run_id)] = None
                continue
            issues = con.execute(
                "SELECT check_name, table_name, severity, count, sample_ids, description "
                "FROM data_quality_issues WHERE run_id = ? ORDER BY check_name",
                [run_id]).fetchall()
            out_runs[str(run_id)] = {
                "run_id": row[0], "layer": row[1], "status": row[2], "error_message": row[3],
                "started_at": row[4], "ended_at": row[5],
                "issues": [{
                    "check_name": i[0], "table_name": i[1], "severity": i[2], "count": i[3],
                    "sample_ids": json.loads(i[4]) if i[4] else [],
                    # Only the pairing record's description is kept: it is the
                    # JSON the judge parses. Others can quote data.
                    "description": i[5] if i[0] == "pg_twin_pairing" else None,
                } for i in issues],
            }
    finally:
        con.close()
    writer_record = None
    if writer and writer[0]:
        try:
            writer_record = json.loads(writer[0])
        except ValueError:
            writer_record = {"unreadable": True}
    delivered = None
    if gate:
        try:
            delivered = sorted((json.loads(Path(gate).read_text()).get("delivered") or {}).keys())
        except FileNotFoundError:
            delivered = []
        except Exception as exc:  # noqa: BLE001 — reported, not raised
            delivered = [f"unreadable: {type(exc).__name__}"]
    return {
        "warehouse_dirty": None if dirty is None else [dirty[0], dirty[1]],
        "writer": writer_record,
        "refreshes": {"max_refreshed_at": refresh[0], "count": refresh[1]},
        "last_refresh": None if last is None else {
            "trigger": last[0], "validation_passed": last[1], "refreshed_at": last[2]},
        "orders": counts[0], "max_order_id": counts[1],
        "silver_orders": counts[2], "silver_order_utm": counts[3],
        "runs": out_runs,
        "gate_delivered": delivered,
    }


def seed_gate(gate: str, *, age_s: float = 3600.0) -> Dict[str, Any]:
    """Merge one delivered `warehouse` page into the Alert Gate's state file,
    as if it had reached an admin an hour ago. The rehearsal's copy only."""
    path = Path(gate)
    try:
        state = json.loads(path.read_text())
    except FileNotFoundError:
        state = {}
    if "buckets" not in state:
        state = {"buckets": state if isinstance(state, dict) else {}, "delivered": {}}
    state.setdefault("delivered", {})[SEEDED_KEY] = {
        "group": SEEDED_GROUP, "first_delivered": time.time() - age_s}
    tmp = path.with_name(path.name + ".reh-tmp")
    tmp.write_text(json.dumps(state))
    os.replace(tmp, path)
    return {"seeded": True, "key": SEEDED_KEY, "delivered": sorted(state["delivered"])}


def derive_latches() -> Dict[str, Any]:
    """Write the latch markers the restored owner rows imply, so the copy's
    two halves of each latch agree, as they do on the host. Production's
    marker directory is never read."""
    import asyncio

    _app_path()
    import asyncpg

    from core import chain_latch

    async def owners() -> Dict[str, str]:
        conn = await asyncpg.connect(os.environ["KS_PG_DSN"])
        try:
            rows = await conn.fetch("SELECT key, value FROM meta.chain_watermarks "
                                    "WHERE key LIKE $1", f"{chain_latch.OWNER_PREFIX}%")
        finally:
            await conn.close()
        return {r["key"][len(chain_latch.OWNER_PREFIX):]: r["value"] for r in rows}

    found = asyncio.run(owners())
    chains = chain_latch.claimed_chains(found)
    for chain in sorted(chains):
        chain_latch.latch(chain, env="reh-step13: from the restored owner rows")
    return {"owner_rows": sorted(found), "latched": sorted(chains)}


# ─── Judges ──────────────────────────────────────────────────────────────────

def _fmt(keys: Iterable[Any]) -> str:
    keys = list(keys)
    return ",".join(str(k) for k in keys) if keys else "-"


def _block(snap: Optional[Mapping[str, Any]]) -> Mapping[str, Any]:
    return ((snap or {}).get("health") or {}).get("warehouse_writer_mode") or {}


def _cut(snap: Optional[Mapping[str, Any]]) -> Mapping[str, Any]:
    return ((snap or {}).get("status") or {}).get("cutover") or {}


def flipped(snap: Optional[Mapping[str, Any]]) -> bool:
    return _block(snap).get("mode") == "postgres"


def judge_p7(ev: Mapping[str, Any]) -> Verdict:
    """One precondition broken: runs as duckdb, the canary pages."""
    snap, can, d0 = ev.get("snapshot"), ev.get("canary"), ev.get("d0")
    if not snap or snap.get("health_code") != 200:
        return UNKNOWN, "phase 0 never answered /api/health"
    block = _block(snap)
    unmet = list(block.get("preconditions_unmet") or [])
    bad: List[str] = []
    if block.get("mode") != "duckdb":
        bad.append(f"mode={block.get('mode')}")
    if block.get("value") != "postgres":
        bad.append(f"value={block.get('value')}")
    if unmet != ["read_fallback_off"]:
        bad.append(f"unmet=[{_fmt(unmet)}]")
    if not can:
        return UNKNOWN, "the canary was not judged"
    if can.get("severity") != "critical" or "warehouse_preconditions_unmet" not in (can.get("failure_keys") or []):
        bad.append(f"canary={can.get('severity')}[{_fmt(can.get('failure_keys') or [])}]")
    if (can.get("preconditions_keys") or []) != ["warehouse_preconditions_unmet"]:
        bad.append(f"check_warehouse_preconditions=[{_fmt(can.get('preconditions_keys') or [])}]")
    if "warehouse_refresh" not in (snap.get("jobs") or []):
        bad.append("warehouse_refresh not registered")
    if not ev.get("log_unmet"):
        bad.append("no 'unmet — running as duckdb' log line")
    if d0 is None:
        return UNKNOWN, "the DuckDB copy was not read at the stop"
    if d0.get("writer") is not None:
        bad.append(f"writer recorded {d0.get('writer')}")
    others = [k for k in can.get("failure_keys") or [] if k != "warehouse_preconditions_unmet"]
    detail = (f"mode={block.get('mode')} unmet=[{_fmt(unmet)}] canary={can.get('severity')} "
              f"warehouse_refresh registered; nothing recorded; other canary keys [{_fmt(others)}]")
    return (FAIL, "; ".join(bad)) if bad else (PASS, detail)


def judge_p1(ev: Mapping[str, Any]) -> Verdict:
    """The flip with every precondition set."""
    snap = ev.get("snapshot")
    if not snap or snap.get("health_code") != 200:
        return UNKNOWN, "the flipped process never answered /api/health"
    block, cut = _block(snap), _cut(snap)
    if block.get("mode") != "postgres":
        return FAIL, (f"no flip: mode={block.get('mode')} unmet="
                      f"[{_fmt(block.get('preconditions_unmet') or cut.get('unmet') or [])}]")
    if not ev.get("seeded"):
        return UNKNOWN, "no delivered warehouse page was seeded, so one resolve proves nothing"
    bad: List[str] = []
    if block.get("error") is not None or block.get("preconditions_unmet"):
        bad.append(f"health block error={block.get('error')} unmet={block.get('preconditions_unmet')}")
    status = snap.get("status") or {}
    if status.get("writer") != "postgres":
        bad.append(f"status writer={status.get('writer')}")
    if cut.get("preconditions_met") is not True or cut.get("unmet"):
        bad.append(f"cutover unmet=[{_fmt(cut.get('unmet') or [])}]")
    writer = cut.get("writer") or {}
    if writer.get("writer") != "postgres" or writer.get("resolved") is not True:
        bad.append(f"cutover.writer={writer}")
    jobs = snap.get("jobs") or []
    if "warehouse_refresh" in jobs:
        bad.append("warehouse_refresh registered")
    if "pg_warehouse_derive" not in jobs:
        bad.append("pg_warehouse_derive missing")
    logs = ev.get("log") or {}
    for key, want in (("not_registered", 1), ("holds", 1), ("recorded", 1)):
        if logs.get(key) != want:
            bad.append(f"log {key}={logs.get(key)}")
    if ev.get("resolved_events") != 1:
        bad.append(f"resolved events with the note={ev.get('resolved_events')}")
    d1 = ev.get("d1")
    if d1 is None:
        bad.append("the DuckDB writer record was not read")
    else:
        rec = d1.get("writer") or {}
        if rec.get("writer") != "postgres" or rec.get("resolved") is not True:
            bad.append(f"sync_metadata.warehouse_writer={rec}")
    if bad:
        return FAIL, "; ".join(bad)
    return PASS, (f"mode=postgres unmet=[] warehouse_refresh absent, pg_warehouse_derive present; "
                  f"writer resolved {writer.get('resolved_at')}; resolved+note events=1")


def judge_p2(ev: Mapping[str, Any]) -> Verdict:
    """DuckDB stays frozen while Postgres derives."""
    if not ev.get("flipped"):
        return UNKNOWN, "no flip"
    d0, d1 = ev.get("d0"), ev.get("d1")
    if d0 is None or d1 is None:
        return UNKNOWN, "the DuckDB copy was not read at both stops"
    bad: List[str] = []
    if d0.get("warehouse_dirty") != d1.get("warehouse_dirty"):
        bad.append(f"warehouse_dirty moved {d0.get('warehouse_dirty')} -> {d1.get('warehouse_dirty')}")
    if d0.get("refreshes") != d1.get("refreshes"):
        bad.append(f"warehouse_refreshes moved {d0.get('refreshes')} -> {d1.get('refreshes')}")
    r0 = (d0.get("refreshes") or {}).get("max_refreshed_at")
    frozen = [s for s in ev.get("frozen_samples") or []]
    if not frozen:
        bad.append("no duckdb_frozen samples")
    elif any(_same_instant(s, r0) is False for s in frozen):
        bad.append(f"duckdb_frozen.last_refresh moved: {sorted(set(map(str, frozen)))} vs {r0}")
    runs = ev.get("runs_ok")
    if runs is None or runs < 3:
        bad.append(f"derivation runs since the flip={runs}")
    if ev.get("refreshing_lines") != 0:
        bad.append(f"'Warehouse dirty — refreshing' lines={ev.get('refreshing_lines')}")
    ticks = ev.get("sync_ticks")
    if ticks is None or ticks < 3:
        bad.append(f"order-list requests={ticks}")
    if bad:
        return FAIL, "; ".join(bad)
    dirty = d0.get("warehouse_dirty")
    return PASS, (f"warehouse_dirty unchanged ({'absent' if dirty is None else 'set'}); "
                  f"warehouse_refreshes max {r0} count {(d0.get('refreshes') or {}).get('count')} "
                  f"unchanged; derivation runs +{runs}; sync ticks {ticks}; 0 refreshing lines")


def _same_instant(a: Any, b: Any) -> Optional[bool]:
    """Two renderings of one timestamp — ISO from the API, VARCHAR from
    DuckDB — compared as instants. None when either does not parse."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    pa, pb = _parse(a), _parse(b)
    if pa is None or pb is None:
        return None
    return abs((pa - pb).total_seconds()) < 0.001


def _parse(value: Any) -> Optional[datetime]:
    text = str(value).strip().replace(" ", "T", 1)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    # DuckDB renders "+03" where fromisoformat wants "+03:00".
    if len(text) >= 3 and text[-3] in "+-" and text[-2:].isdigit():
        text += ":00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def judge_p3(ev: Mapping[str, Any]) -> Verdict:
    """A kill inside the derivation; the owed state survives."""
    if not ev.get("flipped"):
        return UNKNOWN, "no flip"
    if not ev.get("seen_blocked"):
        return UNKNOWN, f"the blocked TRUNCATE gold.daily_revenue was not seen in time ({ev.get('why') or 'timeout'})"
    rk, bk = ev.get("requested"), ev.get("built")
    after = ev.get("after_kill") or {}
    if after.get("rows_above") not in (0, None):
        return UNKNOWN, ("a journal row appeared for the killed attempt — the statement "
                         "timeout beat the kill; retry")
    first = ev.get("first_after_restart")
    bad: List[str] = []
    if not (isinstance(rk, int) and isinstance(bk, int) and rk > bk):
        bad.append(f"at the kill requested={rk} built={bk}")
    if not (after.get("requested", -1) > after.get("built", 0)):
        bad.append(f"after the kill requested={after.get('requested')} built={after.get('built')}")
    if not first:
        bad.append("no derivation ran after the restart")
    else:
        if first.get("error") is not None:
            bad.append(f"first run errored: {first.get('error')}")
        if first.get("validation_passed") is not True:
            bad.append("first run did not validate")
        if not (isinstance(first.get("requested_seen"), int) and isinstance(rk, int)
                and first["requested_seen"] >= rk):
            bad.append(f"requested_seen={first.get('requested_seen')} < {rk}")
        if first.get("trigger") != "signal":
            bad.append(f"trigger={first.get('trigger')}")
    built_after = ev.get("built_after")
    if not (isinstance(built_after, int) and isinstance(rk, int) and built_after >= rk):
        bad.append(f"built after={built_after}")
    if bad:
        return FAIL, "; ".join(bad)
    return PASS, (f"killed with requested {rk} > built {bk} (Silver at v{ev.get('silver_version')}); "
                  f"owed survived; first run after restart #{first.get('id')} trigger=signal "
                  f"requested_seen={first.get('requested_seen')} validated; built {built_after}")


def judge_p4(ev: Mapping[str, Any], *, floor_s: int) -> Verdict:
    """The design's mutations are caught."""
    if not ev.get("flipped"):
        return UNKNOWN, "no flip"
    parts: List[Tuple[str, str, str]] = []

    a = ev.get("a") or {}
    if not a.get("deleted_id"):
        parts.append(("4a", UNKNOWN, a.get("why") or "no Silver row old enough to delete"))
    elif a.get("derivations_between"):
        parts.append(("4a", UNKNOWN, "a derivation ran between the delete and the integrity run"))
    elif not a.get("run"):
        parts.append(("4a", UNKNOWN, "the integrity run was not read"))
    else:
        found = [i for i in (a["run"].get("issues") or []) if i.get("check_name") == "pg_silver_missing_rows"]
        if found and a["deleted_id"] in (found[0].get("sample_ids") or []) and a.get("restored"):
            parts.append(("4a", PASS, f"pg_silver_missing_rows names {a['deleted_id']}, restored by a refresh"))
        else:
            parts.append(("4a", FAIL, f"pg_silver_missing_rows={found[:1]} restored={a.get('restored')}"))

    b = ev.get("b") or []
    if len(b) < 3:
        parts.append(("4b", UNKNOWN, f"{len(b)} of 3 versions landed"))
    else:
        bound = floor_s + 90
        lat, bad = [], []
        for v in b:
            run = v.get("run") or {}
            ok = (isinstance(v.get("requested_before"), int) and isinstance(v.get("requested_after"), int)
                  and v["requested_after"] > v["requested_before"] and run.get("error") is None
                  and run.get("validation_passed") is True
                  and (run.get("requested_seen") or 0) >= v["requested_after"]
                  and v.get("silver_status") == v.get("status")
                  and isinstance(v.get("latency_s"), (int, float)) and v["latency_s"] <= bound)
            lat.append(f"{v.get('latency_s')}s")
            if not ok:
                bad.append(f"v{v.get('k')}: {v}")
        parts.append(("4b", FAIL if bad else PASS,
                      ("; ".join(bad))[:600] if bad else f"marked and derived, latencies {'/'.join(lat)} (bound {bound}s)"))

    c = ev.get("c") or {}
    if not c.get("manager_id"):
        parts.append(("4c", UNKNOWN, c.get("why") or "no manager to give an unknown type"))
    else:
        w = c.get("with_trigger") or {}
        unknown = [u.get("sales_type") for u in w.get("unknown_sales_types") or []]
        ok = (w.get("partition_exhaustive") is False and "reh_unknown" in unknown
              and (c.get("log_alert") or 0) >= 1
              and (c.get("after_drop") or {}).get("partition_exhaustive") is True)
        parts.append(("4c", PASS if ok else FAIL,
                      f"partition_exhaustive=false naming {_fmt(unknown)}, alert lines {c.get('log_alert')}, "
                      f"true again after the drop" if ok else f"{c}"[:600]))

    verdicts = [v for _n, v, _d in parts]
    verdict = FAIL if FAIL in verdicts else UNKNOWN if UNKNOWN in verdicts else PASS
    return verdict, " | ".join(f"{n} {v}: {d}" for n, v, d in parts)


def judge_p5(ev: Mapping[str, Any], *, retired: Iterable[str], standalone_twins: Iterable[str]) -> Verdict:
    """The DQ jobs under the stand-down."""
    if not ev.get("flipped"):
        return UNKNOWN, "no flip"
    retired, twins = set(retired), set(standalone_twins)
    mirror, integrity = ev.get("mirror"), ev.get("integrity")
    if not mirror or not integrity:
        return UNKNOWN, f"runs not read (mirror={bool(mirror)}, integrity={bool(integrity)})"
    bad: List[str] = []
    m_issues = mirror.get("issues") or []
    if mirror.get("error_message") is not None:
        bad.append(f"mirror_landing error: {str(mirror.get('error_message'))[:200]}")
    names = {i.get("check_name") for i in m_issues}
    if "gold_rollup_mismatch" not in names:
        bad.append("gold_rollup_mismatch not filed")
    if names & set(RETIRED_GOLD):
        bad.append(f"retired Gold comparison ran: {sorted(names & set(RETIRED_GOLD))}")
    duck = [i for i in m_issues if str(i.get("check_name", "")).startswith("mirror_")
            and i.get("table_name") in ("silver.orders", "silver.order_utm")]
    if duck:
        bad.append(f"a DuckDB Silver/UTM comparison ran: {[i.get('check_name') for i in duck]}")
    if sorted(ev.get("stood_down") or []) != sorted(STOOD_DOWN):
        bad.append(f"stood_down_duckdb_checks={ev.get('stood_down')}")

    i_issues = integrity.get("issues") or []
    if integrity.get("error_message") is not None:
        bad.append(f"integrity error: {str(integrity.get('error_message'))[:200]}")
    hit = sorted({i.get("check_name") for i in i_issues} & retired)
    if hit:
        bad.append(f"a stood-down check filed: {hit}")
    pairing = next((i for i in i_issues if i.get("check_name") == "pg_twin_pairing"), None)
    filed = {i.get("check_name"): i.get("count") for i in i_issues}
    if pairing is None:
        bad.append("no pg_twin_pairing record")
    else:
        try:
            body = json.loads(pairing.get("description") or "{}")
        except ValueError:
            body = {}
        duck_side = body.get("duckdb") or {}
        looked = [k for k in ("silver_missing_rows", "silver_orphan_rows", "headline_vs_line_items",
                              "goods_shipped_without_sale") if duck_side.get(k) is not None]
        if looked:
            bad.append(f"pairing has DuckDB values for stood-down guards: {looked}")
        standalone = body.get("standalone") or {}
        for name, count in sorted(standalone.items()):
            if name in twins and count and filed.get(name) != count:
                bad.append(f"standalone {name}={count} filed as {filed.get(name)}")
        if "pg_silver_missing_rows" not in filed:
            bad.append("pg_silver_missing_rows not filed")
    if bad:
        return FAIL, "; ".join(bad)
    twins_filed = sorted(n for n in filed if n in twins)
    return PASS, (f"mirror_landing #{mirror.get('run_id')} clean, gold_rollup_mismatch filed, no DuckDB "
                  f"comparison; integrity #{integrity.get('run_id')} clean, no stood-down check, "
                  f"twins alone: {_fmt(twins_filed)}")


def judge_p6(ev: Mapping[str, Any]) -> Verdict:
    """Two restarts, one resolve."""
    if not ev.get("flipped"):
        return UNKNOWN, "no flip"
    t1 = ev.get("resolved_at")
    restarts = ev.get("restarts") or []
    if len(restarts) < 2:
        return UNKNOWN, f"{len(restarts)} of 2 restarts observed"
    bad: List[str] = []
    for n, r in enumerate(restarts, start=1):
        if not r.get("snapshot") or r["snapshot"].get("health_code") != 200:
            bad.append(f"restart {n}: no health")
            continue
        if _block(r["snapshot"]).get("mode") != "postgres":
            bad.append(f"restart {n}: mode={_block(r['snapshot']).get('mode')}")
        w = _cut(r["snapshot"]).get("writer") or {}
        if w.get("resolved_at") != t1:
            bad.append(f"restart {n}: resolved_at {w.get('resolved_at')} != {t1}")
        if r.get("resolved_events") != 1:
            bad.append(f"restart {n}: resolved events={r.get('resolved_events')}")
        if r.get("recorded_lines") != 1 or r.get("resolved_lines") != 1:
            bad.append(f"restart {n}: log recorded={r.get('recorded_lines')} resolved={r.get('resolved_lines')}")
    gate = ev.get("gate_delivered")
    if gate is None:
        bad.append("the gate file was not read")
    elif SEEDED_KEY in gate:
        bad.append("the seeded page is still delivered in the gate file")
    if bad:
        return FAIL, "; ".join(bad)
    return PASS, f"kill+start and stop+start: mode postgres, resolved_at {t1} unchanged, one resolve, gate clear"


def judge_p8(ev: Mapping[str, Any]) -> Verdict:
    """The way back."""
    if not ev.get("flipped"):
        return UNKNOWN, "no flip"
    if ev.get("oom"):
        return UNKNOWN, "reh-web was OOM-killed during the way back (1.5 GB cap against production's 7 GB)"
    start, final, dk = ev.get("start"), ev.get("final"), ev.get("dk")
    if not start or start.get("health_code") != 200:
        return UNKNOWN, "the way back never answered /api/health"
    bad: List[str] = []
    b = _block(start)
    if b.get("mode") != "duckdb" or b.get("held") is not True:
        bad.append(f"on start mode={b.get('mode')} held={b.get('held')}")
    want_reclassify = ev.get("expect_reclassify")
    if want_reclassify and b.get("reclassify_needed") is not True:
        bad.append("reclassify_needed not raised though the verdicts were re-parsed in Postgres alone")
    if sorted(_cut(start).get("stood_down_duckdb_checks") or []) != sorted(STOOD_DOWN):
        bad.append(f"held checks={_cut(start).get('stood_down_duckdb_checks')}")
    logs = ev.get("log") or {}
    for key in ("way_back", "full_tick", "released"):
        if not logs.get(key):
            bad.append(f"log {key} missing")
    if want_reclassify and not logs.get("emptied"):
        bad.append("log: silver_order_utm not emptied")
    if not final or final.get("health_code") != 200:
        bad.append("no final snapshot")
    else:
        fb, fs = _block(final), final.get("status") or {}
        if fb.get("held") is not False or fb.get("reclassify_needed") is not False:
            bad.append(f"final held={fb.get('held')} reclassify_needed={fb.get('reclassify_needed')}")
        if fs.get("last_trigger") != "dirty_flag" or fs.get("validation_passed") is not True:
            bad.append(f"final last_trigger={fs.get('last_trigger')} validation_passed={fs.get('validation_passed')}")
    if not dk:
        bad.append("the DuckDB copy was not read at the end")
    else:
        if (dk.get("writer") or {}).get("writer") != "duckdb":
            bad.append(f"writer={dk.get('writer')}")
        r0 = ev.get("r0")
        last = dk.get("last_refresh") or {}
        newer = _parse(last.get("refreshed_at")) if last else None
        base = _parse(r0) if r0 else None
        if not (last.get("validation_passed") is True and newer and (base is None or newer > base)):
            bad.append(f"last refresh {last} not a validated one after {r0}")
        if not dk.get("silver_order_utm"):
            bad.append("silver_order_utm empty")
        if dk.get("silver_orders") != dk.get("orders"):
            bad.append(f"silver_orders {dk.get('silver_orders')} != orders {dk.get('orders')}")
    if bad:
        return FAIL, "; ".join(bad)
    return PASS, (f"held with warehouse_dirty=full{' and reclassify_needed' if want_reclassify else ''}; "
                  f"one full tick (dirty_flag) validated and released; writer duckdb; "
                  f"silver_orders={dk.get('silver_orders')} == orders, utm rows {dk.get('silver_order_utm')}")


def judge_k0(ev: Mapping[str, Any]) -> Verdict:
    """KeyCRM never called."""
    if not ev:
        return UNKNOWN, "no evidence was collected"
    bad: List[str] = []
    if ev.get("internal") is not True:
        bad.append(f"reh-net internal={ev.get('internal')}")
    url = ev.get("base_url") or ""
    if not url.startswith("http://reh-keycrm:"):
        bad.append(f"base url {url or '?'}")
    stub = ev.get("stub_requests")
    if not isinstance(stub, int) or stub < 1:
        bad.append(f"stub requests={stub}")
    hits = ev.get("keycrm_app_lines")
    if hits != 0:
        bad.append(f"keycrm.app in web logs={hits}")
    if bad:
        return FAIL, "; ".join(bad)
    return PASS, f"internal net; base url {url}; stub requests {stub}; keycrm.app in web logs 0"


def judge_z0(ev: Mapping[str, Any]) -> Verdict:
    """Nothing else on the host moved (context)."""
    before, after = ev.get("before"), ev.get("after")
    mem = ev.get("min_mem_available_mib")
    mem_note = "memory watch skipped (local)" if mem is None else f"min MemAvailable {mem} MiB"
    if before is None or after is None:
        return UNKNOWN, f"container lists not read; {mem_note}"
    if sorted(before) != sorted(after):
        gone, new = sorted(set(before) - set(after)), sorted(set(after) - set(before))
        return UNKNOWN, f"other containers changed during the run (gone {len(gone)}, new {len(new)}); {mem_note}"
    return PASS, f"{len(before)} other containers unchanged; {mem_note}"


# ─── The table ───────────────────────────────────────────────────────────────

LABELS = (
    ("P1", "P1 flip with every precondition"),
    ("P2", "P2 DuckDB frozen while PG derives"),
    ("P3", "P3 kill inside the derivation"),
    ("P4", "P4 mutations caught"),
    ("P5", "P5 DQ jobs under the stand-down"),
    ("P6", "P6 two restarts, one resolve"),
    ("P7", "P7 unmet precondition pages"),
    ("P8", "P8 the way back"),
    ("K0", "K0 KeyCRM never called"),
    ("Z0", "Z0 nothing else moved"),
)


def _load(ev_dir: Path, name: str) -> Any:
    path = ev_dir / name
    try:
        text = path.read_text().strip()
    except FileNotFoundError:
        return None
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return None


def assemble(ev_dir: Path) -> Dict[str, Dict[str, Any]]:
    """The evidence files, as each judge reads them. A missing file is None,
    which every judge turns into UNKNOWN rather than PASS."""
    L = lambda name: _load(ev_dir, name)  # noqa: E731
    f1 = L("f1_snapshot.json")
    is_flipped = flipped(f1)
    d0, d1, dz = L("d0.json"), L("d1.json"), L("dz.json")
    runs = (d1 or {}).get("runs") or {}
    ids = L("f4_runs.json") or {}
    restarts = [r for r in (L("p6_restart1.json"), L("p6_restart2.json")) if r]
    p3 = L("p3.json") or {}
    # Every snapshot taken while the flipped process ran says what
    # `/api/warehouse/status` showed of DuckDB's last refresh: P2's samples.
    frozen = L("frozen_samples.json")
    if frozen is None:
        frozen = [((s.get("status") or {}).get("frozen_last_refresh"))
                  for s in (L(p.name) for p in sorted(ev_dir.glob("flip_snap_*.json")))
                  if isinstance(s, Mapping) and s.get("health_code") == 200]
    # P8's conditional branch: the reclassify door under postgres noted a
    # Postgres-only re-parse in the record, so the way back must empty DuckDB's
    # verdicts and say a reclassify is needed.
    record = (d1 or {}).get("writer") or {}
    p8 = L("p8.json") or {}
    if "expect_reclassify" not in p8:
        p8 = {**p8, "expect_reclassify": bool(record.get("utm_reparsed_at"))}
    return {
        "P7": {"snapshot": L("p7_snapshot.json"), "canary": L("p7_canary.json"),
               "log_unmet": (L("p7_log.json") or {}).get("unmet"), "d0": d0},
        "P1": {"snapshot": f1, "seeded": (L("seed_gate.json") or {}).get("seeded"),
               "log": L("f1_log.json"), "resolved_events": (L("f1_pg.json") or {}).get("resolved_events"),
               "d1": d1},
        "P2": {"flipped": is_flipped, "d0": d0, "d1": d1,
               "frozen_samples": frozen,
               "runs_ok": (L("p2_pg.json") or {}).get("runs_ok"),
               "refreshing_lines": (L("p2_log.json") or {}).get("refreshing"),
               "sync_ticks": (L("p2_log.json") or {}).get("sync_ticks")},
        "P3": {"flipped": is_flipped, **p3},
        "P4": {"flipped": is_flipped,
               "a": {**(L("p4a.json") or {}),
                     "run": runs.get(str(ids.get("integrity"))) if ids.get("integrity") else None},
               "b": L("p4b.json") or [], "c": L("p4c.json") or {}},
        "P5": {"flipped": is_flipped,
               "mirror": runs.get(str(ids.get("mirror_landing"))) if ids.get("mirror_landing") else None,
               "integrity": runs.get(str(ids.get("integrity"))) if ids.get("integrity") else None,
               "stood_down": ((L("f4_snapshot.json") or {}).get("status") or {}).get("cutover", {}).get("stood_down_duckdb_checks")},
        "P6": {"flipped": is_flipped, "resolved_at": (_cut(f1).get("writer") or {}).get("resolved_at"),
               "restarts": restarts, "gate_delivered": (d1 or {}).get("gate_delivered")},
        "P8": {**p8, "flipped": is_flipped, "dk": dz,
               "r0": ((d0 or {}).get("refreshes") or {}).get("max_refreshed_at")},
        "K0": L("k0.json") or {},
        "Z0": L("z0.json") or {},
    }


def judge_all(ev_dir: Path, *, floor_s: int, retired: Iterable[str],
              standalone_twins: Iterable[str]) -> List[Tuple[str, str, str]]:
    ev = assemble(ev_dir)
    judges = {
        "P1": judge_p1, "P2": judge_p2, "P3": judge_p3,
        "P4": lambda e: judge_p4(e, floor_s=floor_s),
        "P5": lambda e: judge_p5(e, retired=retired, standalone_twins=standalone_twins),
        "P6": judge_p6, "P7": judge_p7, "P8": judge_p8, "K0": judge_k0, "Z0": judge_z0,
    }
    rows = []
    for key, label in LABELS:
        try:
            verdict, detail = judges[key](ev[key])
        except Exception as exc:  # noqa: BLE001 — a judge that broke has not passed
            verdict, detail = UNKNOWN, f"the judge raised {type(exc).__name__}: {exc}"
        rows.append((label, verdict, " ".join(str(detail).split())))
    return rows


def _standalone_twins() -> List[str]:
    _app_path()
    from core.pg_warehouse_dq import GUARD_CONDITIONS

    return sorted(c for g in STANDALONE_GROUPS for c in GUARD_CONDITIONS.get(g, ()))


def _retired() -> List[str]:
    _app_path()
    from core.warehouse_cutover import retired_conditions

    return sorted(retired_conditions())


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="probe.py")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("api")
    p.add_argument("method")
    p.add_argument("path")
    p.add_argument("--timeout", type=float, default=600.0)
    sub.add_parser("snapshot")
    sub.add_parser("canary")
    p = sub.add_parser("wait-health")
    p.add_argument("--timeout", type=float, default=600.0)
    p = sub.add_parser("dq-last")
    p.add_argument("layer")
    p = sub.add_parser("trigger-wait")
    p.add_argument("job")
    p.add_argument("--timeout", type=float, default=900.0)
    p = sub.add_parser("wait-dq")
    p.add_argument("layer")
    p.add_argument("--after", type=int, default=0)
    p.add_argument("--timeout", type=float, default=900.0)
    p = sub.add_parser("fixture")
    p.add_argument("--id", type=int, required=True)
    p.add_argument("--version", type=int, required=True, choices=sorted(FIXTURE_VERSIONS))
    p.add_argument("--out", required=True)
    p.add_argument("--product-id", type=int, default=None)
    p = sub.add_parser("readers")
    p.add_argument("--list", choices=("readers", "chains"))
    sub.add_parser("keycrm-url")
    p = sub.add_parser("duckdb-facts")
    p.add_argument("--db", required=True)
    p.add_argument("--gate")
    p.add_argument("--run", type=int, action="append", default=[])
    p = sub.add_parser("seed-gate")
    p.add_argument("--gate", required=True)
    sub.add_parser("derive-latches")
    p = sub.add_parser("judge")
    p.add_argument("--evidence", required=True)
    p.add_argument("--floor", type=int, required=True)
    args = parser.parse_args(argv)

    out: Any
    if args.cmd == "api":
        status, body = api(args.method, args.path, timeout=args.timeout)
        print(json.dumps(body, default=str))
        return 0 if 200 <= status < 300 else 3
    if args.cmd == "snapshot":
        out = snapshot()
    elif args.cmd == "canary":
        out = canary()
    elif args.cmd == "wait-health":
        return 0 if wait_health(args.timeout) else 1
    elif args.cmd == "dq-last":
        out = dq_last(args.layer)
    elif args.cmd == "trigger-wait":
        out = trigger_and_wait(args.job, args.timeout)
        print(json.dumps(out, default=str))
        return 0 if out.get("done") else 1
    elif args.cmd == "wait-dq":
        out = wait_dq(args.layer, args.after, args.timeout)
        print(json.dumps(out, default=str))
        return 0 if out else 1
    elif args.cmd == "fixture":
        out = fixture(args.id, args.version, args.out, args.product_id)
    elif args.cmd == "readers":
        out = readers()
        if args.list:
            # One name per line, for a shell with no JSON parser: the reader
            # switches, or the write chains' flags.
            names = out["readers"] if args.list == "readers" else out["chain_envs"]
            print("\n".join(names))
            return 0
    elif args.cmd == "keycrm-url":
        print(keycrm_url())
        return 0
    elif args.cmd == "duckdb-facts":
        out = duckdb_facts(args.db, args.gate, args.run)
    elif args.cmd == "seed-gate":
        out = seed_gate(args.gate)
    elif args.cmd == "derive-latches":
        out = derive_latches()
    else:
        rows = judge_all(Path(args.evidence), floor_s=args.floor, retired=_retired(),
                         standalone_twins=_standalone_twins())
        for label, verdict, detail in rows:
            print(f"{label}|{verdict}|{detail}")
        return 0
    print(json.dumps(out, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
