"""Stage 5's sweep: under `KS_DUCKDB=off`, which paths still reach for the file.

The week of silence (OD-17 (a)) starts only when web runs a week with
`KS_DUCKDB=off` and nothing opens the DuckDB file. Stage 5's decoupling
(`.planning/DUCKDB_EXIT_STAGE5_DECOUPLING.md`) gets there PR by PR, and this
is the instrument every one of them is measured with: web's real startup, the
scheduler's start, every job it registers and every GET route under `/api/`,
run in the state production will be in that week, with every site the switch
refused recorded per step.

`KNOWN_RESIDUAL` is what is left, exactly. **A site that appears and is not
in it fails** — a path nobody decoupled, or a new one. **A site in it that no
longer appears fails too** — a PR that fixed a path takes its row out in the
same change, so the list never claims a debt that was paid. PR-9's bar is the
empty set.

THE STATE IS DERIVED, NOT WRITTEN DOWN

`apply_target_state` reads it out of the registries: every `KS_READ_*` switch
the fallback walk finds, at the engine it names; every chain in
`WRITE_CHAINS` at `postgres` and latched; every `STORE_SWITCHES` name at
`postgres`; every precondition of step 13 met (`MET_ENV`, which
`tests/unit/test_warehouse_writer.py` keeps true); `KS_READ_FALLBACK=off`;
and `KS_DUCKDB=off`. It is applied through the real `configure_modes()`, and
`assert_target_state` then asks every cached verdict whether it got there. A
registry that grows a precondition this state does not meet fails here,
naming it, rather than sweeping a different state in silence.

TWO FLAVOURS OF POSTGRES

- `down`: Postgres refuses every connection. It runs everywhere, and it asks
  the question an outage asks: does a failed Postgres send anything back to
  the file?
- `live`: an empty Postgres at head — the one CI starts, or any throwaway at
  `KS_PG_DSN`. It reaches what only a Postgres that answers reaches: the
  shadow writes into DuckDB after a Postgres write, the watermarks inherited
  from DuckDB when Postgres holds no key yet, the copies of what only DuckDB
  holds. The database is emptied first (`meta.alembic_version` kept), so the
  sweep meets one state wherever it runs, and put back row for row after,
  checked by hash. Skipped without `KS_PG_DSN`; CI fails a run in which it
  skipped.

Each row of `KNOWN_RESIDUAL` names the flavours it shows in. The GET routes
are swept in `down` only, so what a route reaches only once Postgres has
answered is not here: that is PR-9's integration variant, on a seeded
Postgres. Each is called with its required parameters, and again per value of
every optional parameter that can pick a branch and with the header's filters
together (`optional_variants`): `/api/summary?source_id=3` reaches the file
where `/api/summary` does not.

WHAT A STEP IS RUN AS

Every job runs as the tick production runs once the first minutes have
passed: the boot's sync tick arms the sync service's backoff and retry
windows, and a job run seconds later read nothing behind them — so each job
gets a fresh sync service. A refusal under `get_last_sync_time` carries the
key it was asked for (`name_the_key`): the order step and the catalogue step
are the same site otherwise, and one row would stand for two reaches.

The sweep runs in a fresh interpreter (`tests/unit/duckdb_off_sweep_run.py`
says why) and judges nothing there; the judging is here.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import types
import typing
from pathlib import Path
from typing import Dict, FrozenSet, Optional, Tuple
from unittest.mock import AsyncMock

import pytest

from core import duckdb_switch
from tests.unit.test_read_fallback_sites import fresh_read_fallback  # noqa: F401 — fixture

REPO = Path(__file__).resolve().parents[2]
RUNNER = Path(__file__).with_name("duckdb_off_sweep_run.py")

FLAVOR_ENV = "KS_SWEEP_FLAVOR"
OUT_ENV = "KS_SWEEP_OUT"
DOWN, LIVE = "down", "live"
DOWNS, LIVES, BOTH = frozenset({DOWN}), frozenset({LIVE}), frozenset({DOWN, LIVE})
STEP_TIMEOUT_S = 60

DSN = os.getenv("KS_PG_DSN")

# ─── What is left ────────────────────────────────────────────────────────────
#
# (step, site) → the flavours it shows in. A step is `boot` (web's
# `startup_event` outside the two below), `boot:init_and_sync`,
# `boot:scheduler_start` (`BackgroundScheduler.start`), `job:<id>` for every
# job `_register_jobs()` registers, or `GET <path>` — `GET <path>?<query>`
# for a request with optional values (`variant_key`). A site is
# `duckdb_switch._site()`'s name for who reached for the file, with
# `[last_sync_<key>]` after it under `get_last_sync_time`. The PR that takes
# each row out is the plan's (section 3); it is a note, not a rule.

KNOWN_RESIDUAL: Dict[Tuple[str, str], FrozenSet[str]] = {
    # ── web's boot ──
    # The warehouse writer's record is DuckDB's `sync_metadata` row (PR-6).
    ("boot:init_and_sync", "core.warehouse_cutover:read_writer"): BOTH,
    ("boot:scheduler_start", "core.warehouse_cutover:read_writer"): BOTH,
    # A watermark absent from Postgres is inherited from DuckDB, read in a
    # task of its own by the order step and by the catalogue step (PR-5) —
    # in the boot's sync tick, and in every tick after it. Live only: refused
    # Postgres stops each step first. One row per key (`name_the_key`).
    ("boot:init_and_sync",
     "core.duckdb_store:get_last_sync_time (task) [last_sync_orders]"): LIVES,
    ("boot:init_and_sync",
     "core.duckdb_store:get_last_sync_time (task) [last_sync_products]"): LIVES,
    ("job:incremental_sync",
     "core.duckdb_store:get_last_sync_time (task) [last_sync_orders]"): LIVES,
    ("job:incremental_sync",
     "core.duckdb_store:get_last_sync_time (task) [last_sync_products]"): LIVES,
    # The catch-up reads DuckDB's ages before Postgres replaces them (PR-7).
    ("boot:scheduler_start", "core.scheduler:_schedule_catchup_runs"): BOTH,
    # ── the jobs ──
    ("job:db_backup", "core.scheduler:_run_backup"): BOTH,                      # PR-7
    ("job:dq_integrity_check", "core.scheduler:_run_dq_integrity"): BOTH,       # PR-7
    ("job:dq_mirror_landing", "core.scheduler:_landing"): BOTH,                 # PR-7
    ("job:pg_warehouse_derive", "core.warehouse_cutover:read_writer"): BOTH,    # PR-6
    # The shadow chains' DuckDB copy, written after Postgres (PR-4).
    ("job:disk_watchdog", "core.shadow_writes:into_duckdb"): LIVES,
    ("job:dq_integrity_check", "core.shadow_writes:into_duckdb"): LIVES,
    ("job:dq_mirror_landing", "core.shadow_writes:into_duckdb"): LIVES,
    ("job:memory_monitor", "core.shadow_writes:into_duckdb"): LIVES,
    # What only DuckDB holds, copied and compared (PR-4, PR-7).
    ("job:dq_mirror_landing", "core.mirror_reconciliation:reconcile_operational"): LIVES,
    ("job:replicate_operational", "core.pg_operational:replicate_operational"): LIVES,
    # The digest marker and the reports' adopted weeks (PR-5).
    ("job:dq_digest", "core.dq_journal:digest_inputs"): LIVES,
    ("job:traffic_report", "core.report_ledger:already_sent"): LIVES,
    ("job:weekly_report", "core.report_ledger:already_sent"): LIVES,
    # ── the GET routes ──
    ("GET /api/duckdb/stats", "web.routes.api.health:get_duckdb_stats"): DOWNS,       # PR-3
    ("GET /api/health", "web.routes.api.health:health_check"): DOWNS,                 # PR-3
    ("GET /api/health/detailed", "web.routes.api.health:detailed_health_check"): DOWNS,  # PR-3
    ("GET /api/warehouse/status", "web.routes.api.admin:get_warehouse_status"): DOWNS,   # PR-8
    # Opencart has no column in Gold: Postgres is not asked, DuckDB answers
    # the zeros (design 2.7). An optional parameter's branch (`optional_variants`).
    ("GET /api/summary?source_id=3", "core.repositories.revenue:get_summary_stats"): DOWNS,  # PR-8
}


# ─── The state ───────────────────────────────────────────────────────────────

def apply_target_state(monkeypatch, tmp_path, *, live: bool) -> None:
    """Set the state production will be in for the week of silence, and read
    it the way web's startup does. `live` reads step 13's Postgres facts from
    the database at `KS_PG_DSN`; otherwise they are answered as met."""
    from core import chain_latch, warehouse_cutover as wc
    from core.duckdb_table_fates import STORE_SWITCHES
    from core.pg import REQUIRED_REVISION
    from core.runtime_modes import configure_modes
    from core.write_chains import WRITE_CHAINS, chain_name
    from tests.unit.test_read_fallback_http import _every_switch_on
    from tests.unit.test_warehouse_writer import MET_ENV

    for name, value in MET_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("KS_MIRROR_LANDING", raising=False)
    if live:
        monkeypatch.setenv("KS_PG_DSN", DSN)
    _every_switch_on(monkeypatch)
    for name in STORE_SWITCHES:
        monkeypatch.setenv(name, "postgres")
    # Production's. Its fallback is SQLite, never DuckDB, so it is not one of
    # the store switches the table fates ask about.
    monkeypatch.setenv("KS_BOT_STORE", "postgres")
    for chain in WRITE_CHAINS:
        monkeypatch.setenv(chain.WRITE_ENV, "postgres")
        chain_latch.latch(chain_name(chain), chain.WRITE_ENV)
    monkeypatch.setenv(duckdb_switch.ENV, "off")
    data = tmp_path / "data"
    monkeypatch.setattr("core.duckdb_store.DB_DIR", data)
    monkeypatch.setattr("core.duckdb_store.DB_PATH", data / "analytics.duckdb")
    monkeypatch.setattr(wc, "REVISION_RETRY_DELAYS_S", (0.0, 0.0))
    if not live:
        monkeypatch.setattr(wc, "_revision_on_its_own_connection",
                            AsyncMock(return_value=REQUIRED_REVISION))
        monkeypatch.setattr(wc, "_expenses_backfilled_on_its_own_connection",
                            AsyncMock(return_value=True))
        monkeypatch.setattr(wc, "_expenses_backfilled", AsyncMock(return_value=True))
    # The process-wide caches the modes are read into, put back afterwards.
    from core import pg_derivation, pg_utm_parse

    for module, names in ((pg_derivation, ("_mode", "_mode_error")),
                          (pg_utm_parse, ("_mode", "_mode_error"))):
        for name in names:
            monkeypatch.setattr(module, name, getattr(module, name))
    monkeypatch.setattr("core.sync_service._sync_service", None)
    configure_modes()
    assert_target_state()


def assert_target_state() -> None:
    """Every cached verdict says the state was reached — and if one does not,
    which, rather than a sweep of some other state."""
    from core import pg_orders_write, read_fallback, warehouse_cutover as wc
    from core.write_chains import chain_modes

    assert duckdb_switch.is_off() and duckdb_switch.mode_error() is None
    unmet = [u.key for u in wc.preconditions_unmet()]
    assert wc.writes_postgres() and not unmet, f"step 13 unmet: {unmet}"
    assert read_fallback.mode() == "off"
    assert read_fallback.misconfigured() == [] and read_fallback.no_engine_routes() == []
    # Latched, so writing Postgres whatever else holds (OD-19 (a)). What a
    # latched chain still publishes as unmet — chain 3's host backup evidence
    # on a machine with no PITR drill — is a page, not a route.
    not_moved = {name: state for name, state in chain_modes().items()
                 if state["mode"] != "postgres" or not state["latched"]
                 or state["mismatch"] or state["error"]}
    assert not not_moved, not_moved
    assert pg_orders_write.mode() == "postgres", "chain 3 held on DuckDB"


@pytest.fixture
def target_state(monkeypatch, tmp_path, fresh_read_fallback):  # noqa: F811
    apply_target_state(monkeypatch, tmp_path, live=False)


class FrozenFile:
    """The file production keeps through the week — present, never opened.
    Its bytes are not a DuckDB database: nothing may read them, and a reader
    that tried would fail loudly rather than quietly agree."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"frozen at the start of the week of silence\n")
        self._before = self._state()

    def _state(self) -> dict:
        """The file's bytes and time, and every name DuckDB would leave beside
        it: the WAL, the spill directory, a backup's copy. Other files in the
        directory are web's own — the disk watchdog's heartbeat — and move.

        `backups/` is its listing when it exists and None when it does not:
        a directory the backup made and left empty is a change beside the
        file too, and an empty listing could not tell it from no directory
        (review of PR-1 — `db_backup` made it before it was refused)."""
        directory = self.path.parent
        backups = directory / "backups"
        return {"sha256": hashlib.sha256(self.path.read_bytes()).hexdigest(),
                "mtime_ns": self.path.stat().st_mtime_ns,
                "duckdb_entries": sorted(
                    p.name for p in directory.iterdir()
                    if p.name != self.path.name
                    and (p.name.startswith(self.path.name) or p.name == "duckdb_tmp")),
                "backups": (sorted(p.name for p in backups.iterdir())
                            if backups.is_dir() else None)}

    def unchanged(self) -> dict:
        """Empty when nothing moved; otherwise what did."""
        after = self._state()
        return {k: [self._before[k], after[k]] for k in after if after[k] != self._before[k]}


# ─── Which watermark ─────────────────────────────────────────────────────────
#
# `get_last_sync_time` inherits from DuckDB for more than one key, and its
# refusal is one site whichever key it was asked: the order step and the
# catalogue step each read theirs in a task of its own, and both are
# `core.duckdb_store:get_last_sync_time (task)`. A row naming that site would
# stay matched with one of the two inheritances fixed and the other not. So,
# in the sweep, a refusal under that method carries the key it was asked for.

def _watermark_key(frame) -> Optional[str]:
    """The `last_sync_*` key of the `get_last_sync_time` call on the stack
    from `frame` outwards, or None when there is none."""
    from core.duckdb_store import DuckDBStore

    code = DuckDBStore.get_last_sync_time.__code__
    while frame is not None:
        if frame.f_code is code:
            return f"last_sync_{frame.f_locals.get('key')}"
        frame = frame.f_back
    return None


def name_the_key(monkeypatch) -> None:
    """Count every refusal under `get_last_sync_time` as its site plus
    `[last_sync_<key>]`. The site itself is still `_site()`'s: the count is
    taken after it is named, by `_tally`, which is what this wraps."""
    real_tally = duckdb_switch._tally

    def _tally(site: str) -> int:
        key = _watermark_key(sys._getframe(1))
        return real_tally(f"{site} [{key}]" if key else site)

    monkeypatch.setattr(duckdb_switch, "_tally", _tally)


# ─── The optional parameters ─────────────────────────────────────────────────
#
# A route's base request carries its required parameters only
# (`test_read_fallback_http._request_for`), so a branch an optional one picks
# is never taken there — and `/api/summary?source_id=3` reaches the file where
# `/api/summary` does not: Opencart has no column in Gold, so Postgres is not
# asked and DuckDB answers (design 2.7). Every route is called again, once per
# value of each optional parameter that can pick a branch — the domains below,
# read from the validators, and every boolean turned from its default — and
# once with the header's filters together.

# The header's filters, each at a value its validator takes. `source_id` and
# `sales_type` are here too, so the filters are also tried together.
FILTERS_TOGETHER = {"period": "month", "source_id": 1, "category_id": 1,
                    "brand": "ab1", "promocode": "ab1", "sales_type": "all"}


def _variant_domains() -> Dict[str, list]:
    from core.duckdb_constants import KNOWN_SALES_TYPES
    from core.validators import VALID_SOURCE_IDS

    return {"source_id": sorted(VALID_SOURCE_IDS),
            "sales_type": [*KNOWN_SALES_TYPES, "all"]}


def _is_bool(param) -> bool:
    annotation = param.field_info.annotation
    return annotation is bool or (
        typing.get_origin(annotation) in (typing.Union, types.UnionType)
        and bool in typing.get_args(annotation))


def _optional_query_params(dependant) -> list:
    """The optional query parameters of a route, its dependencies' included,
    each once."""
    from tests.routes_helper import is_required

    found, seen = [], set()

    def walk(node) -> None:
        for param in getattr(node, "query_params", ()) or ():
            if param.name not in seen and not is_required(param):
                seen.add(param.name)
                found.append(param)
        for sub in getattr(node, "dependencies", ()) or ():
            walk(sub)

    walk(dependant)
    return found


def optional_variants(endpoint) -> list:
    """The optional parameters to add to a route's base request, one dict per
    request: each value of each branching parameter alone, then the header's
    filters together with every boolean turned."""
    domains = _variant_domains()
    variants, together = [], {}
    for param in _optional_query_params(endpoint.route.dependant):
        default = param.field_info.default
        if param.name in domains:
            values = domains[param.name]
        elif _is_bool(param):
            values = [not bool(default)]
            together[param.name] = values[0]
        else:
            values = []
        variants.extend({param.name: v} for v in values if v != default)
        if param.name in FILTERS_TOGETHER:
            together[param.name] = FILTERS_TOGETHER[param.name]
    if len(together) > 1:
        variants.append(together)
    return variants


def variant_key(path: str, variant: dict) -> str:
    """`/api/summary?source_id=3` — the step a variant is recorded under."""
    def text(value) -> str:
        return str(value).lower() if isinstance(value, bool) else str(value)

    return path + "?" + "&".join(f"{k}={text(v)}" for k, v in sorted(variant.items()))


# ─── The fixture holds ───────────────────────────────────────────────────────

class TestTheState:
    def test_the_target_state_is_reached(self, target_state):
        """In-process and cheap: the registries' state is what the cached
        verdicts answer, and reading it opened nothing."""
        assert duckdb_switch.opened() == {}

    def test_every_chain_is_latched(self, target_state):
        from core import chain_latch
        from core.write_chains import WRITE_CHAINS, chain_name

        assert all(chain_latch.latched(chain_name(c)) for c in WRITE_CHAINS)

    def test_a_registry_the_state_does_not_meet_fails_by_name(
        self, monkeypatch, tmp_path, fresh_read_fallback,  # noqa: F811
    ):
        """The self-check is the reason the state can be derived: a new
        precondition the state does not meet is named, not swept past.
        Mutation: drop `assert_target_state()` from `apply_target_state`."""
        from core import warehouse_cutover as wc

        monkeypatch.setattr(wc, "OD10_DOORS", (("web.routes.api.x", "door"),))
        with pytest.raises(AssertionError, match="od10_doors"):
            apply_target_state(monkeypatch, tmp_path, live=False)


# ─── The sweep ───────────────────────────────────────────────────────────────

# The sweeps run once per flavour, in a fresh interpreter each, and every
# test below reads what they wrote.

PROCESS, ROUTES = "process", "routes"
# Non-vacuity, so a sweep that silently reached nothing cannot pass: when this
# was written the scheduler registered 24 jobs and the app 91 GET routes.
MIN_JOBS, MIN_ROUTES = 20, 90
# And ~320 requests again with optional values (`optional_variants`).
MIN_VARIANTS = 250
# Steps that are not themselves swept: the moment before the first, and the
# control at the end, whose one refusal is the tripwire proving it stands.
NOT_SWEPT = {"before", "control"}


def run_sweep(flavor: str, sweeps, out: Path) -> Dict[str, dict]:
    out.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, FLAVOR_ENV: flavor, OUT_ENV: str(out), "TZ": "UTC"}
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
         *[f"{RUNNER}::test_{sweep}" for sweep in sweeps]],
        cwd=REPO, env=env, capture_output=True, text=True, timeout=1800)
    written = {sweep: out / f"{sweep}.json" for sweep in sweeps}
    assert proc.returncode == 0 and all(p.exists() for p in written.values()), (
        f"the {flavor} sweep did not run (exit {proc.returncode}):\n"
        f"{proc.stdout[-8000:]}\n{proc.stderr[-3000:]}")
    return {sweep: json.loads(path.read_text(encoding="utf-8"))
            for sweep, path in written.items()}


@pytest.fixture(scope="module")
def down(tmp_path_factory):
    return run_sweep(DOWN, (PROCESS, ROUTES), tmp_path_factory.mktemp("down"))


@pytest.fixture(scope="module")
def live(tmp_path_factory):
    if not DSN:
        pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
    return run_sweep(LIVE, (PROCESS,), tmp_path_factory.mktemp("live"))


def observed(result: dict) -> set:
    return {(step, site) for step, entry in result["steps"].items()
            if step not in NOT_SWEPT for site in entry["sites"]}


def expected(flavor: str, sweep: str) -> set:
    routes = sweep == ROUTES
    return {key for key, flavors in KNOWN_RESIDUAL.items()
            if flavor in flavors and key[0].startswith("GET ") == routes}


def _judge(flavor: str, sweep: str, result: dict) -> None:
    seen, known = observed(result), expected(flavor, sweep)
    new, paid = sorted(seen - known), sorted(known - seen)
    assert not new and not paid, (
        f"{flavor} {sweep} sweep against KNOWN_RESIDUAL —\n"
        f"  reaching for the file and not in the list (a path nobody "
        f"decoupled, or a new one): {new}\n"
        f"  in the list and no longer reaching (take the row out with the "
        f"change that fixed it): {paid}")


# What the runner records for a job `_register_jobs()` registered and the
# scheduler no longer holds: a step whose body never ran.
NOT_SCHEDULED = "not scheduled"


def _judge_the_jobs(result: dict) -> None:
    """Every job `_register_jobs()` registered was run. A step exists for
    each, and none of them is a body that never ran: a job taken off the
    scheduler would otherwise be swept by name only, and its reaches for
    the file would leave the list without anybody fixing them."""
    assert len(result["jobs"]) >= MIN_JOBS, result["jobs"]
    steps = result["steps"]
    missing = sorted(j for j in result["jobs"] if f"job:{j}" not in steps)
    unrun = sorted(j for j in result["jobs"]
                   if steps.get(f"job:{j}", {}).get("status") == NOT_SCHEDULED)
    assert not missing and not unrun, (
        f"registered and not run — no step: {missing}; taken off the "
        f"scheduler ({NOT_SCHEDULED}): {unrun}")


def _judge_the_order_tick(result: dict) -> None:
    """The sync tick ran rather than waited out a window the boot armed. Its
    skips are its adaptive backoff and nothing else, so any skip here is the
    sweep reading less than production's every tick reads. Other jobs' skips
    are the state's own answers — no ClickHouse address, a derivation not
    owned — and are recorded, not judged."""
    step = result["steps"]["job:incremental_sync"]
    assert step["status"] == "ok" and not step.get("skipped"), step


def _judge_the_run(result: dict) -> None:
    """What every run must have done, whatever it found."""
    assert result["file"] == {}, (
        f"the frozen file or DuckDB's companions moved: {result['file']}")
    control = result["steps"]["control"]
    assert control["sites"] == ["tests.unit.duckdb_off_sweep_run:control"], control
    assert control["status"] == "DuckDBOpenedWhileOff", control
    assert result["steps"]["before"]["sites"] == []


class TestTheSweepUnderARefusingPostgres:
    def test_the_process_reaches_exactly_the_known_residual(self, down):
        _judge(DOWN, PROCESS, down[PROCESS])

    def test_the_routes_reach_exactly_the_known_residual(self, down):
        _judge(DOWN, ROUTES, down[ROUTES])

    def test_the_boot_survives_a_boot_sync_that_failed(self, down):
        """Postgres refuses, so the boot sync fails — and web's startup goes
        on: under off, nothing in the handler asks DuckDB (PR-1)."""
        steps = down[PROCESS]["steps"]
        assert steps["boot:init_and_sync"]["status"] != "ok"
        assert steps["boot"]["status"] == "ok"
        assert steps["boot:scheduler_start"]["status"] == "ok"

    def test_every_job_was_run(self, down):
        result = down[PROCESS]
        _judge_the_jobs(result)
        _judge_the_run(result)

    def test_the_order_tick_ran_rather_than_waited(self, down):
        _judge_the_order_tick(down[PROCESS])

    def test_every_route_was_called(self, down):
        result = down[ROUTES]
        assert len(result["routes"]) >= MIN_ROUTES, len(result["routes"])
        assert {f"GET {p}" for p in result["routes"]} <= set(result["steps"])
        assert result["file"] == {}

    def test_every_route_was_called_again_with_its_optional_values(self, down):
        """And the variants were requests the routes took: a 422 is FastAPI
        refusing the parameters, a variant that swept nothing."""
        result = down[ROUTES]
        variants = result["variants"]
        assert len(variants) >= MIN_VARIANTS, len(variants)
        assert {f"GET {key}" for key in variants} <= set(result["steps"])
        assert not [key for key, status in variants.items() if status == 422]

    def test_the_routes_left_answer_as_they_do_today(self, down):
        """Of the routes left, the two whose refusal reaches web's handler —
        the warehouse status, and the summary for a source with no column in
        Gold — answer 503 naming `duckdb` (PR-1) rather than the 500 they fell
        through to; the three that swallow it answer 200, degraded, until
        PR-3."""
        result = down[ROUTES]
        answered = {**result["routes"], **result["variants"]}
        surfaces = result["surfaces"]
        for key in ("/api/warehouse/status", "/api/summary?source_id=3"):
            assert (answered[key], surfaces[key]) == (503, "duckdb"), key
        for path in ("/api/health", "/api/health/detailed", "/api/duckdb/stats"):
            assert answered[path] == 200, path
        assert sorted(k for k, s in surfaces.items() if s == "duckdb") == [
            "/api/summary?source_id=3", "/api/warehouse/status"]


class TestTheSweepUnderALivePostgres:
    def test_the_process_reaches_exactly_the_known_residual(self, live):
        _judge(LIVE, PROCESS, live[PROCESS])

    def test_the_boot_sync_runs_through(self, live):
        """The PR's proof: under off the boot passes `init_and_sync` on the
        unconnected store, with history in Postgres and KeyCRM unreachable."""
        steps = live[PROCESS]["steps"]
        assert steps["boot:init_and_sync"]["status"] == "ok", steps["boot:init_and_sync"]
        assert steps["boot"]["status"] == "ok"

    def test_every_job_was_run(self, live):
        result = live[PROCESS]
        _judge_the_jobs(result)
        _judge_the_run(result)

    def test_the_order_tick_ran_rather_than_waited(self, live):
        """The review's case: the boot's tick armed the backoff, and the job
        a few seconds later skipped and read nothing (review of PR-1).
        Mutation: drop the fresh sync service in the runner's job loop."""
        _judge_the_order_tick(live[PROCESS])


class TestTheList:
    """The list judges itself where it can without running anything."""

    def test_every_row_names_a_flavour_and_a_step_kind(self):
        for (step, site), flavors in KNOWN_RESIDUAL.items():
            assert flavors and flavors <= BOTH, (step, site)
            assert step.split(":")[0] in {"boot", "job"} or step.startswith("GET /api/"), step
            if step.startswith("GET "):
                assert flavors == DOWNS, f"routes are swept with Postgres refusing: {step}"

    def test_a_row_names_a_step_the_sweep_ran(self, down):
        """A job renamed or a route removed leaves its row naming nothing —
        said as that, rather than as a debt paid. The steps are what the
        sweep ran: the jobs `_register_jobs()` registered and the routes read
        off the app."""
        ran = set(down[PROCESS]["steps"]) | set(down[ROUTES]["steps"])
        stale = sorted({step for step, _site in KNOWN_RESIDUAL} - ran)
        assert not stale, f"rows naming a step nothing runs any more: {stale}"


class TestTheInstruments:
    """The sweep's own instruments, held in-process: each is what makes a
    finding visible, so each is checked against the case it exists for."""

    def test_the_frozen_file_sees_a_directory_made_beside_it_and_left_empty(
        self, tmp_path,
    ):
        """`db_backup` made `backups/` before it was refused, and an empty
        listing read the same as no directory (review of PR-1). Mutation:
        read a missing `backups/` as `[]`."""
        frozen = FrozenFile(tmp_path / "data" / "analytics.duckdb")
        assert frozen.unchanged() == {}
        (tmp_path / "data" / "backups").mkdir()
        assert frozen.unchanged() == {"backups": [None, []]}

    def test_the_frozen_file_still_lets_web_s_own_files_move(self, tmp_path):
        frozen = FrozenFile(tmp_path / "data" / "analytics.duckdb")
        (tmp_path / "data" / "disk_watchdog_heartbeat").write_text("now")
        assert frozen.unchanged() == {}

    def test_a_job_taken_off_the_scheduler_fails_the_sweep(self):
        """A registered job the scheduler no longer holds is a step whose
        body never ran; recorded as such by the runner, and judged here as
        the debt it is rather than as a job that was swept (review of
        PR-1). Mutation: judge only that every job has a step."""
        jobs = [f"j{i}" for i in range(MIN_JOBS)]
        ran = {f"job:{j}": {"status": "ok", "sites": []} for j in jobs}
        _judge_the_jobs({"jobs": jobs, "steps": ran})
        ran["job:j3"] = {"status": NOT_SCHEDULED, "sites": []}
        with pytest.raises(AssertionError, match="j3"):
            _judge_the_jobs({"jobs": jobs, "steps": ran})

    def test_a_watermark_inherited_from_duckdb_is_named_with_its_key(
        self, monkeypatch, duckdb_off,
    ):
        """The order step and the catalogue step each read their watermark in
        a task of its own, and both refusals are the same site — one row
        stood for two reaches, and fixing one key's inheritance would have
        left it matched (review of PR-1). Mutation: drop the key from
        `name_the_key`'s site."""
        from core.duckdb_store import DuckDBStore

        name_the_key(monkeypatch)
        store = DuckDBStore()

        async def tick():
            for key in ("orders", "products"):
                with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
                    await asyncio.ensure_future(store.get_last_sync_time(key))
            async with store.connection():  # no watermark in hand: unnamed
                pass

        with pytest.raises(duckdb_switch.DuckDBOpenedWhileOff):
            asyncio.run(tick())
        assert sorted(duckdb_switch.opened()) == [
            "core.duckdb_store:get_last_sync_time (task) [last_sync_orders]",
            "core.duckdb_store:get_last_sync_time (task) [last_sync_products]",
            f"{__name__}:tick",
        ]
        assert not duckdb_off.exists()

    def test_a_route_is_also_called_with_the_optional_values_that_pick_a_branch(self):
        """`_request_for` sends the required parameters only, and
        `/api/summary?source_id=3` reaches the file where `/api/summary` does
        not (review of PR-1, design 2.7). Mutation: return no variants."""
        from web.main import app
        from tests.unit.test_read_fallback_http import swept_routes

        by_path = {e.path: e for e in swept_routes(app)}
        summary = optional_variants(by_path["/api/summary"])
        for source_id in (1, 2, 3, 4):
            assert {"source_id": source_id} in summary
        assert {"sales_type": "all"} in summary and {"sales_type": "retail"} not in summary
        assert any({"category_id", "brand", "period"} <= set(v) for v in summary)
        trend = optional_variants(by_path["/api/revenue/trend"])
        assert {"include_forecast": True} in trend
        assert optional_variants(by_path["/api/me"]) == []

