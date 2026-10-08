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
- **Judges** (`judge_p1` … `judge_p8`, `judge_d1`, `judge_k0`, `judge_z0`): pure functions
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
# The three comparisons the switch stands down in dq_mirror_landing, and the
# one it asks in reconcile_gold's place (core.warehouse_cutover,
# core.scheduler): what the job's `checks_run` must and must not name.
RETIRED_COMPARISONS = ("reconcile_silver", "reconcile_order_utm", "reconcile_gold")
INTERNAL_CHECK = "pg_gold_internal_check"
# The job's completion line, which carries `checks_run` (core.scheduler).
MIRROR_COMPLETE = "Mirror reconciliation complete"
# What D1 calls a loss the rehearsal's kill made (`judge_d1`). The defect is
# DuckDB's and production's, measured apart from the rehearsal (CLAUDE.md,
# "The step-13 rehearsal"); the switch to Postgres writes no DuckDB index.
KNOWN_DEFECT = ("DuckDB 1.5.5's known defect, not a step-13 regression — index entries a "
                "SIGKILL left in the WAL, dropped by the first checkpoint DuckDB takes on its own "
                "after the restart; a CHECKPOINT of the replayed WAL as the store opens keeps them")


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
    out = {k: run.get(k) for k in ("run_id", "started_at", "ended_at", "status",
                                   "error_message", "critical_count", "warn_count")}
    # What the live process's own reader showed of the run (its first 20),
    # names only: context for a judge that later finds the copy's reader blind.
    issues = block.get("issues") if isinstance(block, Mapping) else None
    if isinstance(issues, list):
        out["issue_names"] = sorted(str(i.get("check_name")) for i in issues
                                    if isinstance(i, Mapping))
    return out


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

def _read_run(con: Any, run_id: int) -> Optional[Dict[str, Any]]:
    """One DQ run off the copy: its row and its findings by a scan, beside
    what the product's own reader returns of it. None when the run is not
    there.

    By the id's text, never `run_id = ?`: a comparison on the column takes
    the path through `idx_dqi_run`, and that index is what DuckDB 1.5.5 can
    lose rows from after a SIGKILL (`index_sweep`). The scan is the truth the
    product's reader is then held to."""
    row = con.execute("SELECT run_id, layer, status, error_message, CAST(started_at AS VARCHAR), "
                      "CAST(ended_at AS VARCHAR) FROM data_quality_runs "
                      "WHERE CAST(run_id AS VARCHAR) = ?", [str(run_id)]).fetchone()
    if row is None:
        return None
    issues = con.execute(
        "SELECT check_name, table_name, severity, count, sample_ids, description "
        "FROM data_quality_issues WHERE CAST(run_id AS VARCHAR) = ? ORDER BY check_name",
        [str(run_id)]).fetchall()
    return {
        # And what the product's own reader sees of the same run —
        # `/api/health/data-quality` and the digest ask exactly this. The
        # judges compare the two and fail when they differ: a finding only a
        # scan can find is one nobody is shown.
        "reader": _product_reader(con, run_id),
        "run_id": row[0], "layer": row[1], "status": row[2], "error_message": row[3],
        "started_at": row[4], "ended_at": row[5],
        "issues": [{
            "check_name": i[0], "table_name": i[1], "severity": i[2], "count": i[3],
            "sample_ids": json.loads(i[4]) if i[4] else [],
            # Only the pairing record's description is kept: it is the JSON
            # the judge parses. Others can quote data.
            "description": i[5] if i[0] == "pg_twin_pairing" else None,
        } for i in issues],
    }


def duckdb_facts(db: str, gate: Optional[str], runs: Sequence[int], *,
                 window_run: Optional[int] = None,
                 window_order: Optional[int] = None) -> Dict[str, Any]:
    """What the judges need out of the DuckDB copy. Read-only; never while
    the process under test holds the file. With a window run or order, the
    rows D1 asks of are read too (`window_rows`)."""
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
        # The newest DQ run this stop holds — at F5s, the floor F5w's run
        # must be above to have been written after the checkpoint. An
        # unfiltered max binds no index.
        max_run = one("SELECT max(run_id) FROM data_quality_runs")
        out_runs: Dict[str, Any] = {str(run_id): _read_run(con, run_id) for run_id in runs}
        window = (window_rows(con, window_run, window_order)
                  if window_run is not None or window_order is not None else None)
        indexes = index_sweep(con)
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
        "max_dq_run_id": max_run[0] if max_run else None,
        "runs": out_runs,
        "window": window,
        "indexes": indexes,
        "gate_delivered": delivered,
    }


# Lifted for the sweep alone, on its own read-only connection: by default
# DuckDB takes the index path only for a lookup of at most max(2048, 0.1 % of
# the table) rows, and a sweep that wants every row through the index must
# not fall back to the scan it is checked against.
SWEEP_SETTINGS = ("SET index_scan_max_count = 4000000000",
                  "SET index_scan_percentage = 1.0")


def _index_columns(expressions: Any) -> List[str]:
    """`duckdb_indexes().expressions` — `[k]`, `['"value"']`, `[a, b]` — as
    column names. An entry that is not a bare name comes back as itself and
    the caller skips it."""
    text = str(expressions or "").strip()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    cols = []
    for part in text.split(","):
        name = part.strip().strip("'").strip('"')
        if name:
            cols.append(name)
    return cols


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def index_sweep(con: Any) -> Dict[str, Any]:
    """Whether every index made by CREATE INDEX answers for every row it holds.

    DuckDB 1.5.5 drops from such an index the rows a SIGKILL caught in the
    WAL, when the first checkpoint after the restart is one DuckDB takes on
    its own — at close, or past `wal_autocheckpoint` — and nothing filtered
    or wrote the table in between: replay buffers those entries in the
    still-unbound index, and that checkpoint writes the index without them.
    PRIMARY KEY and UNIQUE constraints keep theirs. A single comparison on
    the column (`run_id = ?`, `layer = ?`, `k >= v`, a `DELETE ... WHERE k IN
    (v)`) then takes the index and misses those rows for good, while a scan
    returns them (reproduced on a fresh file; see CLAUDE.md "The step-13
    rehearsal").

    Each single-column index is asked for every non-NULL row in two ranges —
    `<=` the smallest value, `>=` the next — with the lookup limits lifted,
    and held to `count_if` over the whole table, which no index can serve.
    A composite index serves no read in this DuckDB, measured by what reads
    return rather than by EXPLAIN (which prints SEQ_SCAN for a lookup that
    demonstrably goes through a single-column index): after a kill costs a
    composite index its entries, a filter on its columns still returns every
    row a scan does, while a single-column index's filter misses them
    (`test_a_composite_index_serves_no_read_duckdb_1_5_5`). So a read cannot
    ask one; only a write that must take a row out of it can — D1's
    `window_delete`. A column with one value cannot be asked either — the
    optimizer drops a filter its statistics prove true — so both are listed,
    not asked."""
    swept: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    try:
        for stmt in SWEEP_SETTINGS:
            con.execute(stmt)
        indexes = con.execute(
            "SELECT table_name, index_name, expressions FROM duckdb_indexes() "
            "WHERE schema_name = 'main' ORDER BY table_name, index_name").fetchall()
        for table, index, expressions in indexes:
            name = f"{table}.{index}"
            cols = _index_columns(expressions)
            if len(cols) != 1:
                skipped.append({"index": name, "why": "composite"})
                continue
            col, tbl = _ident(cols[0]), _ident(table)
            values = [r[0] for r in con.execute(
                f"SELECT DISTINCT {col} FROM {tbl} ORDER BY 1 NULLS LAST LIMIT 2").fetchall()
                if r[0] is not None]
            if len(values) < 2:
                skipped.append({"index": name, "why": "empty" if not values else "one value"})
                continue
            rows = found = 0
            for op, value in (("<=", values[0]), (">=", values[1])):
                found += con.execute(f"SELECT count(*) FROM {tbl} WHERE {col} {op} ?",
                                     [value]).fetchone()[0]
                rows += con.execute(f"SELECT count_if({col} {op} ?) FROM {tbl}",
                                    [value]).fetchone()[0]
            swept.append({"index": name, "rows": rows, "missing": rows - found})
    except Exception as exc:  # noqa: BLE001 — reported, judged UNKNOWN
        return {"error": type(exc).__name__, "swept": swept, "skipped": skipped}
    return {"swept": swept, "skipped": skipped}


# The rows D1 asks of: what reh-web wrote after F5s's checkpoint and before
# F6's kill — F5w's integrity run, and F6's fourth version of the fixture
# order with its lines. A kill can cost an index only the rows its WAL held,
# so these are the rows it could cost. Children before parents, the order the
# end's DELETE takes them in. `(table, column, which id)`.
WINDOW_TABLES = (
    ("data_quality_issues", "run_id", "run"),
    ("data_quality_diffs", "run_id", "run"),
    ("data_quality_runs", "run_id", "run"),
    ("order_products", "order_id", "order"),
    ("orders", "id", "order"),
)

# Every window predicate compares the id's text with something appended: no
# index serves that expression, so the row is found by a scan whatever an
# index lost. A DELETE whose predicate goes through an index that lost the row
# finds nothing and reports success
# (`test_a_delete_through_a_lost_index_finds_nothing_duckdb_1_5_5`) — and a
# bare CAST of a VARCHAR column is folded away and does exactly that. The rows
# are counted with `count_if` over the whole table, which no index can serve
# whatever the predicate: the count a DELETE is held to.
_WINDOW_WHERE = "CAST({col} AS VARCHAR) || '' = ?"


def _window_targets(run_id: Optional[int], order_id: Optional[int]) -> List[Tuple[str, str, int]]:
    ids = {"run": run_id, "order": order_id}
    return [(table, col, ids[which]) for table, col, which in WINDOW_TABLES
            if ids[which] is not None]


def _held(con: Any, table: str, where: str, value: str, cols: List[str],
          known: Iterable[str]) -> Optional[int]:
    """How many of the window's rows in `table` an index on `cols` holds: those
    whose every key column is non-NULL. DuckDB 1.5.5 keeps no index entry for a
    row with a NULL in any key column — measured by losing every entry across a
    kill and deleting each row: only a row with all of them set is the FATAL
    (`test_an_index_holds_no_row_with_a_null_key_duckdb_1_5_5`) — so a DELETE
    asks an index only of the rows it holds. None when a key is not a column
    of the table (an expression index), which the caller cannot count."""
    known = set(known)
    if not cols or any(c not in known for c in cols):
        return None
    keyed = " AND ".join(f"{_ident(c)} IS NOT NULL" for c in cols)
    return con.execute(f"SELECT COALESCE(count_if({where} AND {keyed}), 0) FROM {_ident(table)}",
                       [value]).fetchone()[0]


def window_rows(con: Any, run_id: Optional[int], order_id: Optional[int]) -> Dict[str, Any]:
    """D1's window on the copy after the kill: the rows of each window table
    by a scan, the indexes on that table with how many of those rows each
    holds (`held`) — the end's DELETE makes an index give up only the rows it
    holds, and none holds a row with a NULL key: the fixture order has no
    buyer and no manager — and the window's run read as P4a's and P5's are,
    scan beside the product's reader. Never raises: a read that failed is
    reported by its class, judged UNKNOWN."""
    try:
        tables: Dict[str, Any] = {}
        for table, col, value in _window_targets(run_id, order_id):
            where = _WINDOW_WHERE.format(col=_ident(col))
            rows = con.execute(f"SELECT COALESCE(count_if({where}), 0) FROM {_ident(table)}",
                               [str(value)]).fetchone()[0]
            indexes = con.execute(
                "SELECT index_name, expressions FROM duckdb_indexes() "
                "WHERE schema_name = 'main' AND table_name = ? ORDER BY index_name",
                [table]).fetchall()
            known = [r[0] for r in con.execute(
                "SELECT column_name FROM duckdb_columns() "
                "WHERE schema_name = 'main' AND table_name = ?", [table]).fetchall()]
            entries = []
            for name, expr in indexes:
                cols = _index_columns(expr)
                entries.append({"index": name, "columns": len(cols),
                                "held": _held(con, table, where, str(value), cols, known)})
            tables[table] = {"rows": rows, "indexes": entries}
        status = None
        if order_id is not None:
            row = con.execute(f"SELECT status_id FROM orders WHERE {_WINDOW_WHERE.format(col='id')}",
                              [str(order_id)]).fetchone()
            status = row[0] if row else None
        return {"run_id": run_id, "order_id": order_id, "order_status": status,
                "run": _read_run(con, run_id) if run_id is not None else None,
                "tables": tables}
    except Exception as exc:  # noqa: BLE001 — reported, judged UNKNOWN
        return {"run_id": run_id, "order_id": order_id, "error": type(exc).__name__}


# One DELETE in a process of its own: a FATAL invalidates the DuckDB instance,
# and the process's instance cache hands the same dead one to the next
# `connect()` (measured on 1.5.5), so a second table asked in the same process
# would only repeat the first one's FATAL.
#
# Opened through the product's own guard (`core.duckdb_switch.open_file`, the
# one opener, from the image under test), never a bare read-write connect:
# behind a stop that left a WAL, a bare open replays it and its `close()` is
# DuckDB's lossy checkpoint, so the first table's process would cost the next
# table's index the WAL's entries and D1 would report a loss it made itself —
# measured on 1.5.5, a killed writer's rows in two tables: the first DELETE
# clean, the second the FATAL. The guard checkpoints first, which is how the
# product's next start opens the file, so what D1 finds is what the product
# left. On a file with no WAL (every graceful stop) the two opens are the
# same. An image without the guard cannot import it: D1 is then UNKNOWN,
# never a raw open — and so it is under KS_DUCKDB=off, which the opener
# refuses.
_DELETE_ONE = r'''
import json, sys
import duckdb
db, table, where, value, app = sys.argv[1:6]
sys.path.insert(0, app)
out = {"table": table}
try:
    from core.duckdb_switch import open_file
    con = open_file(db)
    con.execute("SET memory_limit='512MB'")
    scan = f"SELECT COALESCE(count_if({where}), 0) FROM {table}"
    out["scanned"] = con.execute(scan, [value]).fetchone()[0]
    out["deleted"] = con.execute(f"DELETE FROM {table} WHERE {where}", [value]).fetchone()[0]
    out["left"] = con.execute(scan, [value]).fetchone()[0]
    con.close()
except duckdb.FatalException as exc:
    first = str(exc).splitlines()[0] if str(exc) else ""
    # The index message carries counts only; anything else is named by class.
    out["fatal"] = first if "Failed to delete all rows from index" in first else type(exc).__name__
except Exception as exc:
    out["error"] = type(exc).__name__
print(json.dumps(out))
'''


def window_delete(db: str, run_id: Optional[int], order_id: Optional[int]) -> Dict[str, Any]:
    """D1's last question: the window's rows deleted from each window table,
    found by a scan, so every index on the table that holds them — composite
    ones, which no read can ask, included — must give each row up at commit
    (an index holds no row with a NULL key, `window_rows`). An entry the
    kill cost is DuckDB's FATAL "Failed to delete all rows from index"; a
    DELETE that reports fewer rows than the scan found, or leaves any, is a
    loss too. It changes the copy, so it runs only on one about to be removed."""
    import subprocess

    results: List[Dict[str, Any]] = []
    for table, col, value in _window_targets(run_id, order_id):
        where = _WINDOW_WHERE.format(col=_ident(col))
        try:
            proc = subprocess.run([sys.executable, "-c", _DELETE_ONE, db, _ident(table), where,
                                   str(value), APP_DIR], capture_output=True, text=True,
                                  timeout=600)
            lines = proc.stdout.strip().splitlines()
            result = json.loads(lines[-1]) if lines else {"error": f"exit {proc.returncode}"}
        except Exception as exc:  # noqa: BLE001 — reported, judged UNKNOWN
            result = {"error": type(exc).__name__}
        result["table"] = table
        results.append(result)
    return {"run_id": run_id, "order_id": order_id, "deletes": results}


def _product_reader(con: Any, run_id: int) -> Dict[str, Any]:
    """`core.data_quality.fetch_run_issues` on the copy: the check names it
    returns, or the class of what it raised. After a kill it can return
    nothing for runs a scan finds — DuckDB 1.5.5, see `index_sweep`."""
    try:
        _app_path()
        from core.data_quality import fetch_run_issues

        found = fetch_run_issues(con, int(run_id), limit=100000)
        return {"names": sorted(str(i.get("check_name")) for i in found)}
    except Exception as exc:  # noqa: BLE001 — reported, judged UNKNOWN
        return {"error": type(exc).__name__}


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


def writer_mode(text: str) -> Tuple[Optional[str], bool]:
    """`(mode, flipped)` out of a `snapshot` the script saved, read the way
    P1's judge reads it: `warehouse_writer_mode.mode` and nothing else. The
    script decides whether F2–B run on this, never on a grep of the file —
    `utm_parse` and `derivation` carry a `mode` too, and `KS_UTM_PARSE` is
    `postgres` before any flip, so a grep for `"mode": "postgres"` ran every
    phase after F1 over a process that had not flipped."""
    try:
        snap = json.loads(text or "null")
    except ValueError:
        return None, False
    if not isinstance(snap, Mapping):
        return None, False
    mode = _block(snap).get("mode")
    return (mode if isinstance(mode, str) else None), flipped(snap)


def judge_reader(run: Optional[Mapping[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """Whether the product's reader sees what the scan found in one DQ run.

    `(fail, unknown)`, at most one set. The scan reads the issues by the id's
    text; `fetch_run_issues` asks `WHERE run_id = ?`, the way the dashboard
    and the digest do, and that takes `idx_dqi_run` — the index DuckDB 1.5.5
    loses rows from after a SIGKILL (`index_sweep`). P4a and P5 read their
    runs at F5s, before the rehearsal's kill, where the two must agree; D1
    reads the run F5w wrote after F5s's checkpoint, after the kill — the one
    the kill could cost. A finding only the scan can see is not one the
    product shows anybody, so a judge that read around it would pass what
    nobody was told."""
    if not run:
        return None, None
    rid = run.get("run_id")
    reader = run.get("reader")
    if not isinstance(reader, Mapping):
        return None, f"the product's reader was not run on run #{rid}"
    if reader.get("error"):
        return None, f"the product's reader raised {reader.get('error')} on run #{rid}"
    scan = sorted(str(i.get("check_name")) for i in run.get("issues") or [])
    seen = sorted(str(n) for n in reader.get("names") or [])
    if seen == scan:
        return None, None
    live = run.get("live_names")
    when = (f"; the live process showed {len(live)} at F4"
            if isinstance(live, list) else "")
    return (f"fetch_run_issues returns {len(seen)} of the {len(scan)} findings a scan "
            f"finds in run #{rid} — the dashboard's and the digest's reader does not "
            f"see them{when}"), None


def mirror_checks(lines: Optional[Iterable[Any]], run_id: Any) -> Optional[List[str]]:
    """The `checks_run` the job's completion line names for `run_id`, or None
    when there is no such line, or it names none (an image older than the
    field). Either rendering — the human one's dict repr, or JSON."""
    import re

    if run_id is None:
        return None
    for line in lines or []:
        text = str(line)
        if MIRROR_COMPLETE not in text:
            continue
        m = re.search(r"""['"]run_id['"]:\s*(\d+)""", text)
        if not m or int(m.group(1)) != int(run_id):
            continue
        c = re.search(r"""['"]checks_run['"]:\s*\[([^\]]*)\]""", text)
        if not c:
            return None
        return re.findall(r"""['"]([A-Za-z0-9_]+)['"]""", c.group(1))
    return None


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
    # The DuckDB copy read at a stop after the flip — F7's, else F5s's. A
    # read that did not happen is evidence missing, never the product's
    # failure: UNKNOWN, unless something else already failed.
    d1 = ev.get("d1")
    unread = None
    if d1 is None:
        unread = "the DuckDB writer record was not read at any stop after the flip"
    else:
        rec = d1.get("writer") or {}
        if rec.get("writer") != "postgres" or rec.get("resolved") is not True:
            bad.append(f"sync_metadata.warehouse_writer={rec}")
    if bad:
        return FAIL, "; ".join(bad)
    if unread:
        return UNKNOWN, unread
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
    # Every stop after the flip — the one before the kill too, when it was
    # read — against the one before it.
    later = [("before the kill", ev.get("d_pre"))] if ev.get("d_pre") else []
    for when, dk in later + [("at F7", d1)]:
        if d0.get("warehouse_dirty") != dk.get("warehouse_dirty"):
            bad.append(f"warehouse_dirty moved {d0.get('warehouse_dirty')} -> "
                       f"{dk.get('warehouse_dirty')} {when}")
        if d0.get("refreshes") != dk.get("refreshes"):
            bad.append(f"warehouse_refreshes moved {d0.get('refreshes')} -> {dk.get('refreshes')} {when}")
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
                  f"unchanged at {len(later) + 1} stops; derivation runs +{runs}; sync ticks {ticks}; "
                  f"0 refreshing lines")


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


def _docker_time(value: Any) -> Optional[datetime]:
    """Docker's RFC 3339 with up to nine fractional digits, trailing zeros
    trimmed (`…:19.81198843Z`), as an instant. None when it does not parse."""
    import re

    m = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)",
                     str(value or "").strip())
    if not m:
        return None
    frac = (m.group(2) or "")[:6].ljust(6, "0")
    tz = "+00:00" if m.group(3) == "Z" else m.group(3)
    try:
        return datetime.fromisoformat(f"{m.group(1)}.{frac}{tz}")
    except ValueError:
        return None


def stop_kind(state: Any) -> Optional[str]:
    """What a stop was, from the container's own state after it (the
    script's `state_of`), never from what the script meant:

    - `graceful` — exited 0: SIGTERM, and DuckDB closed with a checkpoint;
    - `kill` — exit 137 after the rehearsal's `docker kill`;
    - `grace_expired` — exit 137 after a `docker stop`: the process outran
      the grace (production's, `STOP_GRACE_S`) and Docker killed it — the
      same state as a kill, told apart only by what was asked;
    - `oom` — the kernel's OOM killer;
    - `running` — it never stopped; `not_running` — there was nothing to
      stop; `exit N` — anything else. None without a state."""
    if not isinstance(state, Mapping):
        return None
    if state.get("was_running") is False:
        return "not_running"
    if state.get("running") is not False:
        return "running"
    if state.get("oom_killed") is True:
        return "oom"
    code, asked = state.get("exit_code"), state.get("asked")
    if code == 0 and asked != "kill":
        return "graceful"
    if code == 137 and asked == "kill":
        return "kill"
    if code == 137 and asked == "stop":
        return "grace_expired"
    return f"exit {code}"


def _describe_stop(state: Any) -> str:
    kind = stop_kind(state)
    if kind is None:
        return "the container's state after it was not read"
    if kind == "grace_expired":
        return (f"Docker killed it {state.get('stop_s')} s into a {state.get('grace_s')} s grace "
                f"(exit 137): the grace production's deploy gives web ran out")
    return {"running": "it never stopped", "not_running": "it was not running",
            "oom": "the kernel's OOM killer stopped it (exit 137)"}.get(
        kind, f"{kind}, asked {state.get('asked') or '?'}")


def restart_shown(record: Any) -> Tuple[Optional[str], Optional[str]]:
    """`(kind, why_not)` for one P6 record: the kind of the stop before the
    restart as the container showed it, or why the record shows no restart —
    no state recorded, the container never stopped, or no start after it
    (`docker start` on a running container changes nothing at all)."""
    stop = record.get("stop") if isinstance(record, Mapping) else None
    start = record.get("start") if isinstance(record, Mapping) else None
    if not isinstance(stop, Mapping) or not isinstance(start, Mapping):
        return None, "the container's state around it was not recorded"
    kind = stop_kind(stop)
    if kind in ("running", "not_running"):
        return None, _describe_stop(stop)
    if start.get("running") is not True:
        return None, "the container did not run after the start"
    stopped, began = _docker_time(stop.get("finished_at")), _docker_time(start.get("started_at"))
    if stopped is None or began is None or began <= stopped:
        return None, (f"no start after the stop (stopped {stop.get('finished_at')}, "
                      f"started {start.get('started_at')})")
    return kind, None


def unproven_kill(stop: Any, kill_stop: Any) -> Optional[str]:
    """Why a restart whose stop reads as the rehearsal's kill is not the kill
    P3's evidence records, or None when it is. The script writes one state
    file at F6 (`f6_kill_state.json`) into both `p3.json` and the restart
    record after it, so the two must show the same SIGKILL, at the same
    instant: a `kill` record beside a P3 that saw no blocked TRUNCATE is the
    fake-kill run, where P6 passed "graceful,kill,graceful" on a kill that was
    never sent."""
    if stop_kind(kill_stop) != "kill":
        return ("P3 records no kill" if kill_stop is None
                else f"P3's kill is not one: {_describe_stop(kill_stop)}")
    at, p3_at = _docker_time((stop or {}).get("finished_at")), _docker_time(kill_stop.get("finished_at"))
    if at is None or p3_at is None or at != p3_at:
        return (f"its SIGKILL (finished {(stop or {}).get('finished_at')}) is not P3's "
                f"(finished {kill_stop.get('finished_at')})")
    return None


def judge_p3(ev: Mapping[str, Any]) -> Verdict:
    """A kill inside the derivation; the owed state survives.

    The kill is what the container shows after it (`stop`), not that the
    script sent one: a container the kernel's OOM killer took first, or one
    still running, is no kill inside the derivation, and judging what
    followed it would blame the product for a kill that never landed."""
    if not ev.get("flipped"):
        return UNKNOWN, "no flip"
    if not ev.get("seen_blocked"):
        return UNKNOWN, f"the blocked TRUNCATE gold.daily_revenue was not seen in time ({ev.get('why') or 'timeout'})"
    if stop_kind(ev.get("stop")) != "kill":
        return UNKNOWN, (f"the blocked TRUNCATE was seen, but reh-web was not left SIGKILLed by the "
                         f"rehearsal: {_describe_stop(ev.get('stop'))}")
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
        blind, unread = judge_reader(a["run"])
        if not (found and a["deleted_id"] in (found[0].get("sample_ids") or []) and a.get("restored")):
            parts.append(("4a", FAIL, f"pg_silver_missing_rows={found[:1]} restored={a.get('restored')}"))
        elif blind:
            parts.append(("4a", FAIL, f"filed and restored, but {blind}"))
        elif unread:
            parts.append(("4a", UNKNOWN, f"filed and restored, but {unread}"))
        else:
            parts.append(("4a", PASS, f"pg_silver_missing_rows names {a['deleted_id']}, restored by a refresh"))

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
    if ev.get("derivations_after_bump") not in (0, None):
        return UNKNOWN, "a derivation rebuilt Gold between the bump and the mirror-landing run"
    bad: List[str] = []
    unknown: List[str] = []
    # What the job asked, read off its completion line. Absent findings
    # cannot prove a comparison stood down: `compare_gold` excuses every cell
    # whose orders synced inside its grace, and the fixture's landings put
    # the cells the bump touches inside it.
    asked = ev.get("checks_run")
    if asked is None:
        unknown.append(f"the job's completion line names no checks_run for run "
                       f"#{mirror.get('run_id')}, so a stand-down cannot be told "
                       f"from a comparison that found nothing")
    else:
        ran = sorted(set(asked) & set(RETIRED_COMPARISONS))
        if ran:
            bad.append(f"a retired comparison ran: {ran}")
        if INTERNAL_CHECK not in asked:
            bad.append(f"{INTERNAL_CHECK} was not asked")
    for run in (mirror, integrity):
        blind, unread = judge_reader(run)
        if blind:
            bad.append(blind)
        if unread:
            unknown.append(unread)
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
    if unknown:
        return UNKNOWN, "; ".join(unknown)
    twins_filed = sorted(n for n in filed if n in twins)
    # Context, not criterion: ClickHouse's own derivation is the one
    # independent check of Gold left after the switch (OD-08 (a)).
    ch = ("ClickHouse did not compare" if "gold_values_unwatched" in names else
          "ClickHouse caught the bump too" if "ch_engines_gold_mismatch" in names else
          "ClickHouse compared, found nothing")
    return PASS, (f"mirror_landing #{mirror.get('run_id')} without error, asked {INTERNAL_CHECK} "
                  f"and none of {_fmt(RETIRED_COMPARISONS)}, gold_rollup_mismatch filed ({ch}); "
                  f"integrity #{integrity.get('run_id')} without error, no stood-down check, "
                  f"twins alone: {_fmt(twins_filed)}; the product's reader sees both runs whole")


def judge_p6(ev: Mapping[str, Any]) -> Verdict:
    """Two restarts, one resolve — at least one of them after the kill.

    Each restart is the one the container shows, never the one the script
    says it made: the record carries the container's state after the stop
    before it and after the start (`restart_shown`), and the stop's kind is
    read from that state (`stop_kind`). A record without them shows no
    restart. F5s added a graceful restart before F6, so two graceful ones
    alone could reach the count with the restart P6 exists for — the one
    after the rehearsal's kill — missing. A graceful stop that outran
    production's grace is a kill a deploy would make: the restart after it
    still counts, but the row says so and is UNKNOWN until somebody has read
    why web did not stop in time. And the restart after the kill counts only
    when its SIGKILL is the one P3's evidence records (`unproven_kill`): the
    container's word on one file is not enough when the other says nothing
    was killed."""
    if not ev.get("flipped"):
        return UNKNOWN, "no flip"
    t1 = ev.get("resolved_at")
    shown: List[Tuple[int, Mapping[str, Any], str]] = []
    unknown: List[str] = []
    for n, r in enumerate(ev.get("restarts") or [], start=1):
        kind, why = restart_shown(r)
        if not why and kind == "kill":
            unproven = unproven_kill(r.get("stop"), ev.get("kill_stop"))
            if unproven:
                why = f"a SIGKILL P3 does not show ({unproven})"
        if why:
            unknown.append(f"restart {n}: {why}")
        else:
            shown.append((n, r, kind or "?"))
    kinds = [k for _n, _r, k in shown]
    if len(shown) < 2:
        return UNKNOWN, "; ".join([f"{len(shown)} of 2 restarts shown by the container"] + unknown)
    if "kill" not in kinds:
        return UNKNOWN, "; ".join([f"no restart after the rehearsal's kill shown ({_fmt(kinds)})"]
                                  + unknown)
    for n, r, kind in shown:
        if kind != "graceful" and kind != "kill":
            unknown.append(f"restart {n}: {_describe_stop(r.get('stop'))}")
    bad: List[str] = []
    for n, r, _kind in shown:
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
        unknown.append("the gate file was not read after the restarts")
    elif SEEDED_KEY in gate:
        bad.append("the seeded page is still delivered in the gate file")
    if bad:
        return FAIL, "; ".join(bad)
    if unknown:
        return UNKNOWN, "; ".join(unknown)
    return PASS, (f"{len(shown)} restarts the container shows ({_fmt(kinds)}): "
                  f"mode postgres, resolved_at {t1} unchanged, one resolve, gate clear")


def judge_p8(ev: Mapping[str, Any]) -> Verdict:
    """The way back.

    From the stop a deploy makes, to the stop after it: both are read from
    the container's state (`stop_kind`), as P6 reads its restarts. In
    production the way back is a deploy that unsets the variable, and its
    stop of the flipped web gets compose's grace; one that outran the grace
    here was the SIGKILL a deploy would have sent, and the way back then
    started from a kill — said, and UNKNOWN until somebody has read why web
    did not stop in time, as is the way back's own last stop, before the
    end's read of the copy. A stop not recorded is UNKNOWN too."""
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
    # Whether F3's reclassify noted a Postgres-only re-parse comes from the
    # DuckDB writer record read after it; None is "not read", which cannot
    # drop the branch's requirement on the quiet.
    want_reclassify = ev.get("expect_reclassify")
    unknown = ("the DuckDB writer record was not read after F3, so whether the way back owes "
               "the reclassify branch is not known" if want_reclassify is None else None)
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
    unread = [unknown] if unknown else []
    for what, state in (("the stop the way back started from", ev.get("pre_stop")),
                        ("the way back's own stop", ev.get("stop"))):
        if stop_kind(state) != "graceful":
            unread.append(f"{what}: {_describe_stop(state)}")
    if unread:
        return UNKNOWN, "; ".join(unread)
    return PASS, (f"held with warehouse_dirty=full{' and reclassify_needed' if want_reclassify else ''}; "
                  f"one full tick (dirty_flag) validated and released; writer duckdb; "
                  f"silver_orders={dk.get('silver_orders')} == orders, utm rows {dk.get('silver_order_utm')}")


def _sweep_losses(sweep: Any, before: Any = None) -> Tuple[Optional[List[str]], Optional[str]]:
    """`(losses, unread)` out of one `index_sweep`: each index short of its
    rows, or why the sweep cannot be judged. With `before`, an earlier sweep,
    only what an index lost since: a shortfall it already had there is that
    sweep's to report, not this one's."""
    if not isinstance(sweep, Mapping) or not isinstance(sweep.get("swept"), list):
        return None, "not swept"
    if sweep.get("error"):
        return None, f"the sweep raised {sweep.get('error')}"
    if not sweep["swept"]:
        return None, "no index could be asked"
    had = {s.get("index"): s.get("missing") or 0
           for s in ((before or {}).get("swept") or []) if isinstance(s, Mapping)} \
        if isinstance(before, Mapping) else {}
    out = []
    for s in sweep["swept"]:
        missing, already = s.get("missing") or 0, had.get(s.get("index"), 0)
        if missing > already:
            since = f" ({already} already before the kill)" if already else ""
            out.append(f"{s.get('index')} misses {missing} of {s.get('rows')}{since}")
    return out, None


def _window_reads(ev: Mapping[str, Any], pre: Mapping[str, Any], post: Mapping[str, Any],
                  bad: List[str], unknown: List[str]) -> Dict[str, Any]:
    """D1's window read after the kill: that it holds rows the kill could
    reach, and that the product's reader sees the window's run whole."""
    ids = ev.get("window_ids") or {}
    window = post.get("window")
    rid, oid = ids.get("run_id"), ids.get("order_id")
    seen: Dict[str, Any] = {"run": rid, "order": oid, "findings": None, "lines": None}
    if not isinstance(window, Mapping):
        unknown.append("the window was not read after the kill")
        return seen
    if window.get("error"):
        unknown.append(f"reading the window after the kill raised {window.get('error')}")
        return seen
    floor = pre.get("max_dq_run_id")
    if rid is None:
        unknown.append("F5w wrote no integrity run after F5s's checkpoint")
    elif not isinstance(floor, int):
        unknown.append("the newest run F5s's checkpoint holds was not read, so run "
                       f"#{rid} is not known to have been written after it")
    elif rid <= floor:
        unknown.append(f"run #{rid} was already at F5s's checkpoint (its newest run is "
                       f"#{floor}): out of the kill's reach")
    else:
        run = window.get("run")
        if not run:
            unknown.append(f"run #{rid} is not in the copy after the kill")
        elif not run.get("issues"):
            unknown.append(f"run #{rid} filed no finding, so idx_dqi_run was asked of no row")
        else:
            seen["findings"] = len(run["issues"])
            blind, unread = judge_reader(run)
            if blind:
                bad.append(f"after the kill: {blind}")
            elif unread:
                unknown.append(f"after the kill: {unread}")
    tables = window.get("tables") or {}
    if oid is None:
        unknown.append("the fixture order of the window is not known")
    elif (tables.get("orders") or {}).get("rows") != 1:
        unknown.append(f"order #{oid} is not in the copy after the kill")
    elif window.get("order_status") != ids.get("order_status"):
        unknown.append(f"order #{oid} is at status {window.get('order_status')}, not the fourth "
                       f"version's {ids.get('order_status')}: it did not land before the kill")
    elif not (tables.get("order_products") or {}).get("rows"):
        unknown.append(f"order #{oid} has no lines in the copy")
    else:
        seen["lines"] = tables["order_products"]["rows"]
    return seen


def _window_deletes(ev: Mapping[str, Any], post: Mapping[str, Any],
                    bad: List[str], unknown: List[str]) -> Dict[str, Any]:
    """The end's DELETE of the window's rows: every index on each window
    table that holds them, composite included, giving each row up. What was
    asked, counted by index: a table whose DELETE found no row asked none of
    its indexes, and an index holds no row with a NULL key (`window_rows`'
    `held`), so one holding none of the window's rows was not asked whatever
    its table — the fixture order, with no buyer and no manager, is in two of
    `orders`' four composite indexes. An index whose holding was not read
    cannot be counted either way.

    The DELETE comes after the way back, not straight after F7: the copy
    reaches it through two more stops of reh-web, the one the way back starts
    from and the way back's own. One that outran the grace was a SIGKILL of
    its own, and a loss the DELETE then finds may be that kill's rather than
    P3's — still DuckDB's defect, so still FAIL, but said beside the loss
    rather than laid at P3's door. A clean DELETE does not lean on them: what
    F7's checkpoint wrote down of P3's kill is still what the DELETE asks."""
    asked: Dict[str, Any] = {"tables": 0, "indexes": 0, "composite": 0, "not_held": []}
    done = ev.get("delete")
    if not isinstance(done, Mapping):
        unknown.append("the window's rows were not deleted at the end, so no composite index "
                       "was asked")
        return asked
    if done.get("skipped"):
        unknown.append(f"the window's rows were not deleted ({done.get('skipped')}), so no "
                       f"composite index was asked")
        return asked
    later = [f"{what} ({_describe_stop(state)})" for what, state in
             (("the stop the way back started from", ev.get("b_pre_stop")),
              ("the way back's own stop", ev.get("b_stop")))
             if stop_kind(state) != "graceful"]
    since = (f"; the copy reached the DELETE through {' and '.join(later)}, so a kill after "
             f"P3's may have cost it" if later else "")
    indexes = {t: v.get("indexes") or [] for t, v in
               (((post.get("window") or {}).get("tables")) or {}).items() if isinstance(v, Mapping)}
    answered = 0
    unread: List[str] = []
    for d in done.get("deletes") or []:
        table = d.get("table")
        # The suspects of a loss: an index holding none of the rows gave none up.
        names = _fmt(i.get("index") for i in indexes.get(table, []) if i.get("held") != 0)
        if d.get("fatal"):
            bad.append(f"deleting the window's rows from {table} is DuckDB's FATAL "
                       f"({d.get('fatal')}): an index on it lost them — one of [{names}]{since}")
        elif d.get("error") or not isinstance(d.get("scanned"), int):
            unknown.append(f"deleting the window's rows from {table} raised {d.get('error') or '?'}")
        elif d.get("deleted") != d["scanned"] or d.get("left"):
            bad.append(f"the DELETE from {table} took {d.get('deleted')} of the {d['scanned']} "
                       f"rows a scan finds, {d.get('left')} left: an index on it answered for "
                       f"fewer — one of [{names}]{since}")
        else:
            answered += 1
            if d["scanned"] > 0:
                asked["tables"] += 1
                for i in indexes.get(table, []):
                    held = i.get("held")
                    if not isinstance(held, int):
                        unread.append(f"{table}.{i.get('index')}")
                    elif held > 0:
                        asked["indexes"] += 1
                        asked["composite"] += (i.get("columns") or 0) > 1
                    else:
                        asked["not_held"].append(f"{table}.{i.get('index')}")
    if unread:
        unknown.append(f"which of [{_fmt(unread)}] hold the window's rows was not read, so "
                       f"whether the end's DELETE asked them is not known")
    if not (done.get("deletes") or []):
        unknown.append("the end deleted no window table")
    elif answered == len(done["deletes"]) and not asked["tables"]:
        unknown.append("the end's DELETE found none of the window's rows, so no index on their "
                       "tables was asked")
    return asked


def judge_d1(ev: Mapping[str, Any]) -> Verdict:
    """What P3's kill cost DuckDB's indexes.

    The rehearsal's F6 is the shape of an OOM kill of the live web, and
    DuckDB 1.5.5 can lose index entries across one (`index_sweep`). A kill
    can cost only what its WAL held — the rows written after the last
    checkpoint, which is F5s's graceful stop — so this row asks of exactly
    those: F5w's integrity run and F6's fourth version of the fixture order
    (`window_rows`). After the kill, the run read by the product's reader as
    the dashboard and the digest read it; at the end, the window's rows
    deleted through every index on their tables that holds them
    (`window_delete`), the one question a composite index answers. An index
    holds no row with a NULL key, and the fixture order has no buyer and no
    manager, so the buyer and manager indexes on `orders` — two of its four
    composite ones — are not asked, and the PASS names them. And every
    single-column index swept whole at both stops, for whatever else the
    window wrote.

    The read after the kill means something only after the checkpoint that
    follows the restart: a read-only open sees every row the WAL holds,
    whatever the index lost (`test_a_read_only_read_after_a_kill_sees_every_row`),
    so a stop at F7 that was not graceful leaves the row UNKNOWN. P4a and P5
    read at F5s before the kill, read-only, and need nothing of it.

    A loss after the kill is FAIL and is labelled for what it is: DuckDB
    1.5.5's known defect (`KNOWN_DEFECT`), which the rehearsal's kill
    exercises the way an OOM kill of the live web would — not something the
    switch to Postgres did, which writes no DuckDB index. Nothing the product
    runs at start touches the DQ journal, so on 1.5.5 as the store opens it
    today, this row is expected to fail on the window's run. A loss already
    there before the kill is the copy's own, and is not given that label."""
    if not ev.get("flipped"):
        return UNKNOWN, "no flip"
    if not ev.get("seen_blocked") or stop_kind(ev.get("kill_stop")) != "kill":
        return UNKNOWN, "no kill landed inside the derivation (see P3)"
    pre, post = ev.get("pre"), ev.get("post")
    if not pre or not post:
        return UNKNOWN, (f"the DuckDB copy was not read at both stops around the kill "
                         f"(before={bool(pre)}, after={bool(post)})")
    bad: List[str] = []      # a loss the rehearsal's kill did not make
    defect: List[str] = []   # what the kill cost: the known defect
    unknown: List[str] = []
    swept = 0
    for when, facts in (("before the kill", pre), ("after the kill", post)):
        # After the kill, only what an index lost since the stop before it:
        # a shortfall the copy already carried is reported once, as its own.
        losses, unread = _sweep_losses(
            facts.get("indexes"), pre.get("indexes") if when == "after the kill" else None)
        if unread:
            unknown.append(f"indexes {when}: {unread}")
            continue
        swept = max(swept, len(facts["indexes"]["swept"]))
        if not losses:
            continue
        if when == "after the kill":
            defect.append(f"read through their indexes: {'; '.join(losses)}")
            continue
        # Before the rehearsal's kill a loss is the copy's own — a kill
        # production's web took before the backup — or a stop of phase 0
        # that outran the grace; the state of that stop says which.
        why = ""
        if stop_kind(ev.get("p0_stop")) not in (None, "graceful"):
            why = f" (phase 0's stop: {_describe_stop(ev.get('p0_stop'))})"
        bad.append(f"{when}, read through their indexes: {'; '.join(losses)}{why}")
    seen = _window_reads(ev, pre, post, defect, unknown)
    asked = _window_deletes(ev, post, defect, unknown)
    f7 = stop_kind(ev.get("f7_stop"))
    if f7 != "graceful":
        unknown.append(f"F7's stop was no checkpoint ({_describe_stop(ev.get('f7_stop'))}): the "
                       f"read after it shows what the WAL holds, not what the kill cost")
    if bad or defect:
        return FAIL, "; ".join(bad + ([f"{KNOWN_DEFECT}: after the kill, {'; '.join(defect)}"]
                                      if defect else []))
    if unknown:
        return UNKNOWN, "; ".join(unknown)
    window_tables = {t for t, _c, _w in WINDOW_TABLES}
    skipped = (post.get("indexes") or {}).get("skipped") or []
    elsewhere = sum(1 for s in skipped if s.get("why") == "composite"
                    and str(s.get("index")).split(".")[0] not in window_tables)
    rest = sum(1 for s in skipped if s.get("why") != "composite")
    not_held = asked["not_held"]
    nulls = (f"{len(not_held)} indexes on those tables that hold none of them, a NULL key in "
             f"each [{_fmt(not_held)}]; " if not_held else "")
    return PASS, (f"nothing lost of what D1 asks after the kill: run #{seen['run']} "
                  f"({seen['findings']} findings) read whole by fetch_run_issues; its rows and "
                  f"order #{seen['order']}'s ({seen['lines']} lines) given up at the end by the "
                  f"{asked['indexes']} indexes holding them on the {asked['tables']} tables they "
                  f"are in, {asked['composite']} composite among them; {swept} single-column "
                  f"indexes answer for every row at both stops. Not asked: {nulls}{elsewhere} "
                  f"composite indexes on tables outside the window, {rest} on an empty or "
                  f"one-valued column")


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


def _containers(lines: Optional[Iterable[Any]]) -> Dict[str, Dict[str, Any]]:
    """`others()`'s lines — id, name, StartedAt, RestartCount, OOMKilled —
    by id. A line carrying an id and a name alone reads the rest as None."""
    out: Dict[str, Dict[str, Any]] = {}
    for line in lines or []:
        f = str(line).split()
        if not f:
            continue
        out[f[0]] = {"name": f[1] if len(f) > 1 else "?",
                     "started": f[2] if len(f) > 2 else None,
                     "restarts": f[3] if len(f) > 3 else None,
                     "oom": (f[4] == "true") if len(f) > 4 else None}
    return out


def judge_z0(ev: Mapping[str, Any]) -> Verdict:
    """Nothing else on the host moved.

    A container that was there before and after but started again in
    between — a restart policy after a crash or an OOM kill keeps its id and
    its name — is FAIL: the one thing the caps and the watchdog exist to
    prevent is the live stack starved, and until somebody has read why it
    restarted, the rehearsal is the suspect. A container gone or new is
    UNKNOWN: a deploy recreates, a cron one-off comes and goes, and on a
    laptop other work does both."""
    before, after = ev.get("before"), ev.get("after")
    mem = ev.get("min_mem_available_mib")
    mem_note = "memory watch skipped (local)" if mem is None else f"min MemAvailable {mem} MiB"
    if before is None or after is None:
        return UNKNOWN, f"container lists not read; {mem_note}"
    b, a = _containers(before), _containers(after)
    both = [i for i in b if i in a]
    oom = sorted(a[i]["name"] for i in both if a[i]["oom"] is True and b[i]["oom"] is not True)
    restarted = sorted(a[i]["name"] for i in both
                       if (a[i]["started"], a[i]["restarts"]) != (b[i]["started"], b[i]["restarts"]))
    if oom or restarted:
        parts = []
        if oom:
            parts.append(f"OOM-killed during the run: {_fmt(oom)}")
        if restarted:
            parts.append(f"started again during the run: {_fmt(restarted)}")
        return FAIL, "; ".join(parts) + f"; {mem_note}"
    gone = sorted(b[i]["name"] for i in b if i not in a)
    new = sorted(a[i]["name"] for i in a if i not in b)
    if gone or new:
        return UNKNOWN, (f"other containers changed during the run (gone {len(gone)}, new {len(new)}); "
                         f"none of those there throughout restarted; {mem_note}")
    return PASS, f"{len(b)} other containers unchanged, none restarted or OOM-killed; {mem_note}"


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
    ("D1", "D1 DuckDB indexes survive the kill"),
    ("K0", "K0 KeyCRM never called"),
    ("Z0", "Z0 nothing else moved"),
)


def _lines(ev_dir: Path, name: str) -> Optional[List[str]]:
    try:
        return (ev_dir / name).read_text().splitlines()
    except FileNotFoundError:
        return None


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
    # The DQ runs P4a and P5 judge, read at the graceful stop before the kill
    # (`d_pre.json`); `d1.json` is the stop after it, D1's.
    d_pre = L("d_pre.json")
    runs = dict((d_pre or {}).get("runs") or {})
    ids = L("f4_runs.json") or {}
    restarts = [r for r in (L(p.name) for p in sorted(ev_dir.glob("p6_restart*.json"))) if r]
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
    # What the live process's reader showed of each run right after it ran:
    # context for `judge_reader`.
    for run_key, live_file in (("integrity", "f4_integrity_live.json"),
                               ("mirror_landing", "f4_mirror_live.json")):
        rid, live = ids.get(run_key), L(live_file)
        if rid and isinstance(runs.get(str(rid)), Mapping) and isinstance(live, Mapping) \
                and live.get("run_id") == rid and isinstance(live.get("issue_names"), list):
            runs[str(rid)] = {**runs[str(rid)], "live_names": live["issue_names"]}
    # The writer record as the last stop after F3 read it — F7's, else F5s's.
    # Neither read leaves the branch unknown (None), never quietly not owed.
    after_f3 = d1 or d_pre
    p8 = L("p8.json") or {}
    if "expect_reclassify" not in p8:
        p8 = {**p8, "expect_reclassify": None if after_f3 is None else
              bool((after_f3.get("writer") or {}).get("utm_reparsed_at"))}
    return {
        "P7": {"snapshot": L("p7_snapshot.json"), "canary": L("p7_canary.json"),
               "log_unmet": (L("p7_log.json") or {}).get("unmet"), "d0": d0},
        "P1": {"snapshot": f1, "seeded": (L("seed_gate.json") or {}).get("seeded"),
               "log": L("f1_log.json"), "resolved_events": (L("f1_pg.json") or {}).get("resolved_events"),
               "d1": d1 or d_pre},
        "P2": {"flipped": is_flipped, "d0": d0, "d_pre": d_pre, "d1": d1,
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
               "stood_down": ((L("f4_snapshot.json") or {}).get("status") or {}).get("cutover", {}).get("stood_down_duckdb_checks"),
               "checks_run": mirror_checks(_lines(ev_dir, "mirror_complete.log"),
                                           ids.get("mirror_landing")),
               "derivations_after_bump": (L("p4a.json") or {}).get("derivations_after_bump")},
        "P6": {"flipped": is_flipped, "resolved_at": (_cut(f1).get("writer") or {}).get("resolved_at"),
               "restarts": restarts, "gate_delivered": (d1 or {}).get("gate_delivered"),
               # The kill P3 records, which the restart after it must show.
               "kill_stop": p3.get("stop") if p3.get("seen_blocked") is True else None},
        "P8": {**p8, "flipped": is_flipped, "dk": dz,
               "r0": ((d0 or {}).get("refreshes") or {}).get("max_refreshed_at"),
               # The stop the way back starts from, and its own last one.
               "pre_stop": L("b_pre_stop_state.json"), "stop": L("b_stop_state.json")},
        "D1": {"flipped": is_flipped, "seen_blocked": p3.get("seen_blocked") is True,
               "kill_stop": p3.get("stop"), "pre": d_pre, "post": d1,
               "f7_stop": L("f7_stop_state.json"), "p0_stop": L("p0_stop_state.json"),
               "window_ids": L("d1_window.json"),
               "delete": L("d1_delete.json"),
               # The two stops between F7's read and the end's DELETE.
               "b_pre_stop": L("b_pre_stop_state.json"), "b_stop": L("b_stop_state.json")},
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
        "P6": judge_p6, "P7": judge_p7, "P8": judge_p8, "D1": judge_d1,
        "K0": judge_k0, "Z0": judge_z0,
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

def build_parser() -> argparse.ArgumentParser:
    """The CLI the script calls. Every call the script makes is parsed with
    this in `tests/unit/test_step13_rehearsal_script.py`, so a flag the
    script passes and the probe lacks fails there — not as an exit 2 an hour
    into a run, swallowed by an `|| true` that leaves the evidence empty."""
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
    p.add_argument("--window-run", type=int, default=None)
    p.add_argument("--window-order", type=int, default=None)
    p = sub.add_parser("window-delete")
    p.add_argument("--db", required=True)
    p.add_argument("--window-run", type=int, default=None)
    p.add_argument("--window-order", type=int, default=None)
    # A saved snapshot on stdin: prints the writer's mode, exit 0 when flipped.
    sub.add_parser("writer-mode")
    p = sub.add_parser("seed-gate")
    p.add_argument("--gate", required=True)
    sub.add_parser("derive-latches")
    p = sub.add_parser("judge")
    p.add_argument("--evidence", required=True)
    p.add_argument("--floor", type=int, required=True)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)

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
    elif args.cmd == "writer-mode":
        mode, is_flipped = writer_mode(sys.stdin.read())
        print(mode or "")
        return 0 if is_flipped else 1
    elif args.cmd == "duckdb-facts":
        out = duckdb_facts(args.db, args.gate, args.run, window_run=args.window_run,
                           window_order=args.window_order)
    elif args.cmd == "window-delete":
        out = window_delete(args.db, args.window_run, args.window_order)
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
