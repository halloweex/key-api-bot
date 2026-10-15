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
and `KS_DUCKDB=off`. It is applied through the real `configure_modes()` as web
calls it, so off's own coherence verdict (`core/duckdb_off.py`, PR-2) is
reached too, and `assert_target_state` then asks every cached verdict whether
it got there — off's last, which must hold: a held-back process runs `on`, and
the sweep would be measuring a web that opens the file on purpose. A registry
that grows a precondition this state does not meet fails here, naming it,
rather than sweeping a different state in silence.

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
together (`optional_variants`): `/api/summary?source_id=3` reached the file
where `/api/summary` did not, until PR-8.

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
# (step, site) → the flavours it shows in. A step is the process sweep's
# `boot` (web's `startup_event` outside the two below), `boot:init_and_sync`,
# `boot:scheduler_start` (`BackgroundScheduler.start`) or `job:<id>` for every
# job `_register_jobs()` registers; the route sweep's `GET <path>` —
# `GET <path>?<query>` for a request with optional values (`variant_key`) —
# or `routes:derive`; or one of the doors sweep's (PR-9, `sweep_of` places
# each): `<METHOD> <path>` for a route under /api/ that is not a GET,
# `<METHOD> <path> as <who>` outside it, `viewer GET <path>`, `admin GET
# /api/chat/stream`, `WS <path> as <who>`, `event:<name>`, `tool:<name>`,
# `setup…`, `events:open|close`, `shutdown`. A site is
# `duckdb_switch._site()`'s name for who reached for the file, with
# `[last_sync_<key>]` after it under `get_last_sync_time`. The PR that took
# each row out is the plan's (section 3); it is a note, not a rule.

KNOWN_RESIDUAL: Dict[Tuple[str, str], FrozenSet[str]] = {
    # ── web's boot ──
    # None since PR-6: the warehouse writer's record is read from its frozen
    # copy in Postgres.
    # ── the jobs ──
    # None since PR-4, PR-5, PR-6 and PR-7 together: nothing copied or
    # compared out of DuckDB (the shadow chains' tables stand down under off
    # since PR-4, the history tables beside them since PR-7), the inherited
    # watermarks and marks read from the copy (PR-5), the writer's record
    # from its copy (PR-6).
    # ── the GET routes ──
    # None since PR-8: `/api/warehouse/status` reads DuckDB's last refresh
    # out of its Postgres copy, and `/api/summary?source_id=3` — Opencart,
    # no column in Gold — answers DuckDB's zeros without a connection.
    # ── the doors (PR-9) ──
    # None, in either flavour, the first time they were opened: every write
    # under /api/, the pages and sign-ins, the sockets, the events, the
    # assistant's tools and routes, a real viewer's session and web's stop.
}

# ─── The DuckDB levers (OD-S5-8) ─────────────────────────────────────────────
#
# A door whose subject is the DuckDB file itself may reach it under off, and
# no other: such a reach is not a debt to pay but the lever doing what it is
# for — so it is listed here with its reason instead, and **refused loudly**:
# answered 503 naming `duckdb`, and counted and paged by the tripwire like any
# reach, because using a DuckDB lever during the week breaks the week. Exact
# like the residual — a lever that stops reaching leaves the list — and
# derived where it can be: only a door under `LEVER_PREFIX`, the one OD-S5-8
# keeps under its old name until the code is deleted, may be listed at all.
#
# (step, site) → reason. Empty: under the chains every `/api/duckdb/` door
# routes its sync to Postgres, and none reaches the file (`TestTheLevers`).
LEVER_PREFIX = "/api/duckdb/"
DUCKDB_LEVERS: Dict[Tuple[str, str], str] = {}


# ─── The state ───────────────────────────────────────────────────────────────

def apply_target_state(monkeypatch, tmp_path, *, live: bool,
                       breaking: Optional[Dict[str, str]] = None) -> None:
    """Set the state production will be in for the week of silence, and read
    it the way web's startup does. `live` reads step 13's Postgres facts from
    the database at `KS_PG_DSN`; otherwise they are answered as met.

    `breaking` sets variables over that state just before it is read, and
    then nothing is asserted about it: the caller judges what the start
    made of the state with one thing wrong."""
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
        # Off's two Postgres facts, as production's copy holds them (design
        # 4.1): step 13's record resolved, and copied after the last latch.
        from core import duckdb_off

        monkeypatch.setattr(duckdb_off, "_frozen_facts_on_its_own_connection",
                            AsyncMock(return_value=frozen_facts_met()))
    # The process-wide caches the modes are read into, put back afterwards.
    from core import pg_derivation, pg_utm_parse

    for module, names in ((pg_derivation, ("_mode", "_mode_error")),
                          (pg_utm_parse, ("_mode", "_mode_error"))):
        for name in names:
            monkeypatch.setattr(module, name, getattr(module, name))
    monkeypatch.setattr("core.sync_service._sync_service", None)
    for name, value in (breaking or {}).items():
        monkeypatch.setenv(name, value)
    configure_modes(web=True)
    if breaking is None:
        assert_target_state()


# What `core.duckdb_off` reads from Postgres when off's verdict is met.
WRITER_RESOLVED = {"writer": "postgres", "since": "2026-10-01T06:00:00+00:00",
                   "resolved": True, "resolved_at": "2026-10-01T06:00:01+00:00"}


def frozen_facts_met() -> dict:
    """Step 13's record resolved, an owner row for every table of every
    chain, and the copy shipped an hour after them."""
    from datetime import datetime, timedelta, timezone

    from core.write_chains import WRITE_CHAINS

    now = datetime.now(timezone.utc)
    return {"writer": dict(WRITER_RESOLVED), "copied_at": now,
            "owners": {table: now - timedelta(hours=1)
                       for chain in WRITE_CHAINS for table in chain.CHAIN_TABLES}}


def assert_target_state() -> None:
    """Every cached verdict says the state was reached — and if one does not,
    which, rather than a sweep of some other state. Off's own verdict last:
    it reads the others, and a held-back web names them by key."""
    from core import duckdb_off, pg_orders_write, read_fallback, warehouse_cutover as wc
    from core.write_chains import chain_modes

    assert duckdb_switch.value() == "off" and duckdb_switch.mode_error() is None
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
    assert duckdb_switch.held_back() == (), (
        f"off held back: {[(u.key, u.detail) for u in duckdb_off.unmet()]}")
    assert duckdb_switch.is_off()


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
# is never taken there — and `/api/summary?source_id=3` reached the file where
# `/api/summary` did not: Opencart has no column in Gold, so Postgres is not
# asked and DuckDB answered (design 2.7), until PR-8 answered its zeros
# without it. Every route is called again, once per
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

# The sweeps run once per flavour, each in a fresh interpreter, and every
# test below reads what they wrote.

PROCESS, ROUTES, DOORS = "process", "routes", "doors"
SWEEPS = (PROCESS, ROUTES, DOORS)
# Non-vacuity, so a sweep that silently reached nothing cannot pass: when this
# was written the scheduler registered 24 jobs and the app 91 GET routes.
MIN_JOBS, MIN_ROUTES = 20, 90
# And ~320 requests again with optional values (`optional_variants`).
MIN_VARIANTS = 250
# PR-9's doors, when it was written: 40 routes under /api/ that are not GETs,
# 15 outside it, 2 WebSocket routes, 15 events, 15 tools.
MIN_WRITES, MIN_OUTSIDE, MIN_SOCKETS, MIN_EVENTS, MIN_TOOLS = 35, 12, 2, 12, 12
# What a Postgres that answers lets through, when PR-9 was written: 407 of
# the 415 GET requests answered 200, 30-odd of the 40 doors 2xx, and the
# viewer ~60 of 92 routes (the rest are admin-only, or another tab's). A
# floor well under each, so a seeded state that stopped seeding fails here.
MIN_LIVE_GET_200, MIN_LIVE_WRITES_2XX, MIN_LIVE_VIEWER_200 = 350, 25, 45
# Steps that are not themselves swept: the moment before the first, and the
# control at the end, whose one refusal is the tripwire proving it stands.
NOT_SWEPT = {"before", "control"}


def run_sweep(flavor: str, sweeps, out: Path) -> Dict[str, dict]:
    """Each sweep in an interpreter of its own (`duckdb_off_sweep_run.py` says
    why), one after the other, and what each wrote."""
    out.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, FLAVOR_ENV: flavor, OUT_ENV: str(out), "TZ": "UTC"}
    found = {}
    for sweep in sweeps:
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
             f"{RUNNER}::test_{sweep}"],
            cwd=REPO, env=env, capture_output=True, text=True, timeout=1800)
        written = out / f"{sweep}.json"
        assert proc.returncode == 0 and written.exists(), (
            f"the {flavor} {sweep} sweep did not run (exit {proc.returncode}):\n"
            f"{proc.stdout[-8000:]}\n{proc.stderr[-3000:]}")
        found[sweep] = json.loads(written.read_text(encoding="utf-8"))
    return found


@pytest.fixture(scope="module")
def down(tmp_path_factory):
    return run_sweep(DOWN, SWEEPS, tmp_path_factory.mktemp("down"))


@pytest.fixture(scope="module")
def live(tmp_path_factory):
    if not DSN:
        pytest.skip("needs a live PostgreSQL at KS_PG_DSN")
    return run_sweep(LIVE, SWEEPS, tmp_path_factory.mktemp("live"))


def sweep_of(step: str) -> str:
    """Which sweep a step belongs to, by its name alone: the boot and the
    jobs are the process's; a GET under `/api/`, and the route sweep's own
    setup (`routes:…`), the admin's route sweep; every other step is one of
    PR-9's doors, none of whose names starts either way (`viewer GET …`,
    `admin GET /api/chat/stream`, `setup…`, `WS …`, `tool:…`)."""
    if step.split(":")[0] in ("boot", "job"):
        return PROCESS
    if step.startswith(("GET /api/", "routes:")):
        return ROUTES
    return DOORS


def observed(result: dict) -> set:
    return {(step, site) for step, entry in result["steps"].items()
            if step not in NOT_SWEPT for site in entry["sites"]}


def expected(flavor: str, sweep: str) -> set:
    return {key for key, flavors in KNOWN_RESIDUAL.items()
            if flavor in flavors and sweep_of(key[0]) == sweep}


def levers(sweep: str) -> set:
    return {key for key in DUCKDB_LEVERS if sweep_of(key[0]) == sweep}


def answered(result: dict, step: str) -> Tuple[Optional[int], Optional[str]]:
    """What an HTTP step answered — its status and the surface it named —
    from either sweep's record of it."""
    if step.startswith("GET /api/") and "routes" in result:
        key = step[len("GET "):]
        status = {**result["routes"], **result["variants"]}.get(key)
        return status, result["surfaces"].get(key)
    return result.get("statuses", {}).get(step), result.get("surfaces", {}).get(step)


def _judge(flavor: str, sweep: str, result: dict) -> None:
    seen, known = observed(result), expected(flavor, sweep)
    misplaced = sorted(step for step, _site in seen if sweep_of(step) != sweep)
    assert not misplaced, f"steps {sweep_of.__name__} does not place in {sweep}: {misplaced}"
    # A lever's reach is its own, and only refused loudly is it allowed.
    pulled = seen & levers(sweep)
    quiet = sorted(step for step, _site in pulled
                   if answered(result, step) != (503, "duckdb"))
    assert not quiet, f"DuckDB levers that reached the file and were not refused loudly: {quiet}"
    idle = sorted(levers(sweep) - pulled)
    assert not idle, f"DuckDB levers listed and no longer reaching (take them out): {idle}"
    seen = seen - pulled
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
    assert control["sites"] == ["tests.unit.duckdb_off_sweep_run:reach"], control
    assert control["status"] == "DuckDBOpenedWhileOff", control
    assert result["steps"]["before"]["sites"] == []


def _judge_the_routes(result: dict) -> None:
    assert len(result["routes"]) >= MIN_ROUTES, len(result["routes"])
    assert {f"GET {p}" for p in result["routes"]} <= set(result["steps"])
    variants = result["variants"]
    assert len(variants) >= MIN_VARIANTS, len(variants)
    assert {f"GET {key}" for key in variants} <= set(result["steps"])
    # A 422 is FastAPI refusing the parameters: a variant that swept nothing.
    assert not [key for key, status in variants.items() if status == 422]
    assert result["file"] == {}


def _judge_the_doors(result: dict) -> None:
    """Every door PR-9 added was opened, none of them refused as malformed,
    and web stopped as it does at a deploy. What each door is made of is
    `duckdb_off_sweep_doors.py`; what they reach is `_judge`'s."""
    steps, statuses = result["steps"], result["statuses"]
    writes, outside = result["writes"], result["outside"]
    assert len(writes) >= MIN_WRITES and len(outside) >= MIN_OUTSIDE, (writes, outside)
    assert len(result["socket_routes"]) >= MIN_SOCKETS, result["socket_routes"]
    assert set(writes) <= set(steps), sorted(set(writes) - set(steps))
    for door in outside:
        for who in ("nobody", "viewer", "admin"):
            assert f"{door} as {who}" in steps, (door, who)
    for path in result["socket_routes"]:
        for who in ("nobody", "viewer", "admin"):
            assert f"WS {path} as {who}" in steps, (path, who)
    # The GET routes the admin's GET sweep leaves out for their network, each
    # called here as an admin — every one `NETWORK_ROUTES` names.
    from tests.unit.test_read_fallback_http import NETWORK_ROUTES

    assert sorted(result["network"]) == sorted(NETWORK_ROUTES), result["network"]
    for path in result["network"]:
        assert f"admin GET {path}" in steps, path
    # Malformed: FastAPI's 422 ends a request at its parameters, before
    # anything the door does — a door that swept nothing.
    malformed = sorted(step for step, status in statuses.items() if status == 422)
    assert not malformed, f"doors refused as malformed, swept nothing: {malformed}"
    # Every event the bus carries, and every tool, each a step.
    assert len(result["handlers"]) >= MIN_EVENTS, result["handlers"]
    assert {f"event:{e}" for e in result["handlers"]} <= set(steps)
    assert len(result["tool_names"]) >= MIN_TOOLS, result["tool_names"]
    assert {f"tool:{t}" for t in result["tool_names"]} <= set(steps)
    from tests.unit.duckdb_off_sweep_doors import DOORS_FRAME

    for frame in DOORS_FRAME:
        assert frame in steps, frame
    assert steps["shutdown"]["status"] == "ok", steps["shutdown"]
    _judge_the_run(result)


def _judge_the_assistant(result: dict) -> None:
    """Both chat routes ran every tool: the model was handed a result for
    each, once per route, and the stream sent one `tool_result` per tool."""
    tools = sorted(result["tool_names"])
    chat = result["chat"]
    assert result["statuses"]["POST /api/chat"] == 200, result["details"].get("POST /api/chat")
    assert result["statuses"]["admin GET /api/chat/stream"] == 200
    assert chat["results_seen"] == [tools, tools], chat["results_seen"]
    assert chat["stream"].count("tool_result") == len(tools), chat["stream"]
    assert chat["stream"][-1] == "end", chat["stream"]


class TestTheSweepUnderARefusingPostgres:
    @pytest.mark.parametrize("sweep", SWEEPS)
    def test_each_sweep_reaches_exactly_the_known_residual(self, down, sweep):
        _judge(DOWN, sweep, down[sweep])

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

    def test_every_route_was_called_with_its_optional_values(self, down):
        """And the variants were requests the routes took: a 422 is FastAPI
        refusing the parameters, a variant that swept nothing."""
        _judge_the_routes(down[ROUTES])

    def test_every_door_was_opened(self, down):
        _judge_the_doors(down[DOORS])

    def test_the_assistant_ran_every_tool_through_both_routes(self, down):
        """With Postgres refusing, every tool answers what it can — a named
        `data_unavailable`, an error — and nothing reaches the file."""
        _judge_the_assistant(down[DOORS])

    def test_nobody_and_a_viewer_are_refused_before_the_store(self, down):
        """A viewer's session is read from the user list, in Postgres, and
        an unreadable list refuses the request (401) rather than guessing a
        role; the admin sockets are the hardcoded admin's and open."""
        from web.routes.auth import PUBLIC_API_PATHS

        result = down[DOORS]
        viewer = {s[len("viewer GET "):]: v for s, v in result["statuses"].items()
                  if s.startswith("viewer GET ")}
        gated = {p: v for p, v in viewer.items() if p not in PUBLIC_API_PATHS}
        assert gated and set(gated.values()) == {401}, sorted(
            (p, v) for p, v in gated.items() if v != 401)
        sockets = result["sockets"]
        for path in result["socket_routes"]:
            assert sockets[f"WS {path} as admin"]["accepted"], path
            assert sockets[f"WS {path} as nobody"]["close_code"] == 4001, path

    def test_the_routes_pr8_paid_answer_without_the_file(self, down):
        """PR-8, in the week's own state. The two routes that reached the
        file answered 503 naming `duckdb` until it (PR-1's handler); now the
        summary for a source with no column in Gold answers DuckDB's zeros,
        and the warehouse status nests DuckDB's last refresh as `off` — from
        its Postgres copy, so here, with Postgres refusing, as the error's
        class. No route's refusal names `duckdb` any more — nor any door's.
        Mutation: drop either `off` branch — the route is back in the list,
        and here."""
        from tests.unit.duckdb_off_sweep_run import PAID_BY_PR8

        result = down[ROUTES]
        answered = {**result["routes"], **result["variants"]}
        for key in PAID_BY_PR8:
            assert answered[key] == 200, key
        assert [k for k, s in result["surfaces"].items() if s == "duckdb"] == []
        assert [k for k, s in down[DOORS]["surfaces"].items() if s == "duckdb"] == []
        summary = result["bodies"]["/api/summary?source_id=3"]
        assert {k: summary[k] for k in ("totalOrders", "totalRevenue", "avgCheck",
                                        "totalReturns", "returnsRevenue")} == {
            "totalOrders": 0, "totalRevenue": 0, "avgCheck": 0,
            "totalReturns": 0, "returnsRevenue": 0}
        frozen = result["bodies"]["/api/warehouse/status"]["duckdb_frozen"]
        assert frozen["status"] == "off" and frozen["error"], frozen

    def test_health_says_off_and_is_healthy(self, down):
        """PR-3, in the week's own state: the three health routes reach
        nothing (none is in the list), `/api/health` reads healthy with no
        cause, and DuckDB's block and ledger say `off` — with Postgres
        refusing, so nothing else degrades it either. Mutation: drop the
        `off` branch of `health_check`'s stats block — `degraded_by` would
        read `["duckdb"]` here, beside its site in the list."""
        result = down[ROUTES]
        for path in ("/api/health", "/api/health/detailed", "/api/duckdb/stats"):
            assert result["routes"][path] == 200, path
        assert result["health"] == {
            "status": "healthy", "degraded_by": [],
            "duckdb": "off", "migrations": {"status": "off"}}


class TestTheSweepUnderALivePostgres:
    """The integration variant the design asked PR-9 for: Postgres answers,
    holding a small history in Bronze that Postgres derived into Silver, Gold
    and the customer profile itself — so the routes and the doors read rows,
    and what only a Postgres that answers reaches is in the run."""

    @pytest.mark.parametrize("sweep", SWEEPS)
    def test_each_sweep_reaches_exactly_the_known_residual(self, live, sweep):
        _judge(LIVE, sweep, live[sweep])

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

    def test_the_shadow_writes_were_reached_and_stood_down(self, live):
        """PR-4's rows left the list because the DuckDB half stands down,
        not because nothing reached it: with Postgres answering, the journal
        (chain 9) and the samples (chain 10) wrote Postgres and stood their
        shadow down, and no shadow failed. Mutation: drop the `off` return in
        `into_duckdb` — the four rows come back, and the failures with them."""
        result = live[PROCESS]
        assert {"pg_dq_journal_write", "pg_watchdog_write"} <= set(
            result["shadow_stood_down"]), result["shadow_stood_down"]
        assert result["shadow_failures"] == []

    def test_the_seeded_history_was_derived_and_read(self, live):
        """The derivation ran in each HTTP sweep before its first request,
        and the routes answered from what it wrote: almost every GET is a
        200 — the summary among them, with the seeded orders in it."""
        routes, doors = live[ROUTES], live[DOORS]
        assert routes["steps"]["routes:derive"]["status"] == "ok"
        assert doors["steps"]["setup:derive"]["status"] == "ok"
        _judge_the_routes(routes)
        answered = {**routes["routes"], **routes["variants"]}
        assert sum(1 for s in answered.values() if s == 200) >= MIN_LIVE_GET_200, (
            sorted((k, s) for k, s in answered.items() if s != 200))
        summary = routes["bodies"]["/api/summary?period=month"]
        assert summary and summary["totalOrders"] > 0, summary

    def test_every_door_was_opened_and_most_went_through(self, live):
        result = live[DOORS]
        _judge_the_doors(result)
        statuses = result["statuses"]
        through = sorted(d for d in result["writes"] if 200 <= statuses.get(d, 0) < 300)
        assert len(through) >= MIN_LIVE_WRITES_2XX, (
            through, {d: result["details"].get(d) for d in result["writes"]
                      if d not in through})

    def test_the_assistant_ran_every_tool_through_both_routes(self, live):
        _judge_the_assistant(live[DOORS])

    def test_a_real_viewer_session_is_a_viewer(self, live):
        """`_resolve_session` reads the viewer's row — nothing patched it —
        so no route answered 401: the viewer sees what its tabs open and is
        refused (403) the rest, the admin pages among them."""
        statuses = live[DOORS]["statuses"]
        viewer = {s[len("viewer GET "):]: v for s, v in statuses.items()
                  if s.startswith("viewer GET ")}
        assert 401 not in viewer.values(), sorted(p for p, v in viewer.items() if v == 401)
        assert sum(1 for v in viewer.values() if v == 200) >= MIN_LIVE_VIEWER_200
        assert viewer["/api/admin/users"] == 403 and viewer["/api/summary"] == 200

    def test_the_sockets_open_for_whom_they_should_and_carry_the_events(self, live):
        """The handshake reads the session the same way: the admin opens
        both sockets, the viewer the dashboard's and not the admin's, nobody
        neither — and with them open, every handler web registered at its
        start broadcast to its room."""
        result = live[DOORS]
        sockets = result["sockets"]
        assert sockets["WS /ws/dashboard as viewer"]["accepted"]
        assert sockets["WS /ws/admin as viewer"]["close_code"] == 4003
        for path in result["socket_routes"]:
            assert sockets[f"WS {path} as admin"]["accepted"], path
            assert sockets[f"WS {path} as nobody"]["close_code"] == 4001, path
            assert "pong" in sockets[f"WS {path} as admin"]["events"], path
        heard = result["events"]
        assert "sync_failed" in heard["/ws/admin as admin"], heard
        for event in ("orders_synced", "products_synced", "inventory_updated",
                      "goal_progress"):
            assert event in heard["/ws/dashboard as admin"], (event, heard)
            assert event in heard["/ws/dashboard as viewer"], (event, heard)

    def test_the_sign_ins_reach_the_bot_store_and_sign_in(self, live):
        """The bot's store is web's too: the Login Widget's callback and the
        Mini App's sign-in check the account there, and both set a session
        for the viewer."""
        statuses = live[DOORS]["statuses"]
        for who in ("nobody", "viewer", "admin"):
            assert statuses[f"GET /auth/telegram/callback as {who}"] == 302
            assert statuses[f"POST /auth/webapp as {who}"] == 200


def door_path(step: str) -> Tuple[Optional[str], Optional[str]]:
    """(method, path) of an HTTP step, whoever it was sent as: `POST /a?x=1`,
    `viewer GET /a`, `GET /login as nobody`. (None, None) for any other."""
    words = step.split(" ")
    if words[0] in ("viewer", "admin"):
        words = words[1:]
    if len(words) < 2 or words[0] not in ("GET", "POST", "PUT", "PATCH", "DELETE"):
        return None, None
    return words[0], words[1].split("?")[0]


class TestTheLevers:
    """OD-S5-8's doors keep their DuckDB name until the code is deleted. What
    the sweep says of them, both ways: listed levers are such doors, and
    every such door was opened and reached exactly what is listed — today
    nothing."""

    def test_only_a_door_under_the_prefix_may_be_listed(self):
        from tests.routes_helper import iter_endpoints
        from web.main import app

        doors = {(m, e.path) for e in iter_endpoints(app) for m in e.methods}
        for (step, site), reason in DUCKDB_LEVERS.items():
            method, path = door_path(step)
            assert (method, path) in doors and path.startswith(LEVER_PREFIX), step
            assert reason and site, step

    @pytest.mark.parametrize("flavor", [DOWN, LIVE])
    def test_every_lever_door_was_opened_and_reached_only_what_is_listed(
        self, flavor, request,
    ):
        from tests.routes_helper import iter_endpoints
        from web.main import app

        result = request.getfixturevalue(flavor)
        doors = {(m, e.path) for e in iter_endpoints(app)
                 if e.path.startswith(LEVER_PREFIX) for m in e.methods - {"HEAD"}}
        assert len(doors) >= 5, doors          # stats, resync, refresh, two syncs
        reached, opened = set(), set()
        for sweep in (ROUTES, DOORS):
            for step, entry in result[sweep]["steps"].items():
                if door_path(step) in doors:
                    opened.add(door_path(step))
                    reached.update((step, site) for site in entry["sites"])
        assert opened == doors, sorted(doors - opened)
        assert reached == set(DUCKDB_LEVERS), sorted(reached ^ set(DUCKDB_LEVERS))

    def test_door_path_reads_every_step_shape(self):
        assert door_path("POST /api/duckdb/resync") == ("POST", "/api/duckdb/resync")
        assert door_path("POST /api/x?background=false") == ("POST", "/api/x")
        assert door_path("viewer GET /api/summary") == ("GET", "/api/summary")
        assert door_path("admin GET /api/chat/stream") == ("GET", "/api/chat/stream")
        assert door_path("GET /login as nobody") == ("GET", "/login")
        assert door_path("tool:get_revenue_summary") == (None, None)
        assert door_path("WS /ws/admin as admin") == (None, None)


class TestTheList:
    """The list judges itself where it can without running anything."""

    def test_every_row_names_a_flavour_and_a_sweep(self):
        for (step, site), flavors in KNOWN_RESIDUAL.items():
            assert flavors and flavors <= BOTH, (step, site)
            assert sweep_of(step) in SWEEPS, step

    def test_a_row_names_a_step_the_sweep_ran(self, down):
        """A job renamed or a route removed leaves its row naming nothing —
        said as that, rather than as a debt paid. The steps are what the
        sweep ran: the jobs `_register_jobs()` registered and the doors read
        off the app."""
        ran = set().union(*(set(down[sweep]["steps"]) for sweep in SWEEPS))
        stale = sorted({step for step, _site in KNOWN_RESIDUAL} - ran)
        assert not stale, f"rows naming a step nothing runs any more: {stale}"

    @pytest.mark.parametrize("step,sweep", [
        ("boot", PROCESS), ("boot:init_and_sync", PROCESS), ("job:dq_digest", PROCESS),
        ("GET /api/summary", ROUTES), ("GET /api/summary?source_id=3", ROUTES),
        ("routes:derive", ROUTES), ("setup:derive", DOORS), ("setup", DOORS),
        ("POST /api/expenses", DOORS), ("viewer GET /api/summary", DOORS),
        ("admin GET /api/chat/stream", DOORS), ("GET /login as nobody", DOORS),
        ("WS /ws/admin as admin", DOORS), ("event:orders.synced", DOORS),
        ("tool:get_revenue_summary", DOORS), ("shutdown", DOORS)])
    def test_a_step_is_placed_in_its_sweep_by_its_name(self, step, sweep):
        assert sweep_of(step) == sweep


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
        `/api/summary?source_id=3` reached the file where `/api/summary` did
        not (review of PR-1, design 2.7; PR-8 paid it). Mutation: return no
        variants."""
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



class TestTheDoors:
    """PR-9's doors, held in-process where nothing has to run: each is read
    off the app, so a door added tomorrow is swept the day it is registered,
    and what a door is sent is checked against what it takes."""

    def test_every_door_is_read_off_the_app_and_none_falls_between(self):
        """The GET routes under /api/ (the routes sweep), the rest under
        /api/, the routes outside it and the sockets (the doors sweep) are
        every endpoint the app has, each in one sweep."""
        from tests.routes_helper import iter_endpoints
        from tests.unit import duckdb_off_sweep_doors as doors
        from tests.unit.test_read_fallback_http import swept_routes
        from web.main import app

        every = set()
        for e in iter_endpoints(app):
            if doors.is_socket(e):
                every.add(("WS", e.path))
            every.update((m, e.path) for m in e.methods - {"HEAD"})
        swept = {("GET", e.path) for e in swept_routes(app)}
        # What the doors sweep calls as an admin, not what is named for it.
        swept |= {("GET", e.path) for e in doors.network_routes(app)}
        swept |= {(m, e.path) for m, e in doors.write_routes(app)}
        swept |= {(m, e.path) for m, e in doors.outside_routes(app)}
        swept |= {("WS", e.path) for e in doors.socket_routes(app)}
        assert swept == every, (sorted(every - swept), sorted(swept - every))
        assert {e.path for e in doors.get_api_routes(app)} == {
            p for m, p in every if m == "GET" and p.startswith("/api/")}

    def test_a_route_named_for_its_network_is_called_as_an_admin(self, monkeypatch):
        """A route `NETWORK_ROUTES` names leaves the admin's GET sweep; the
        doors sweep calls each one it names as an admin, read off the app.
        The call used to be one literal path, the stream's, so a second
        route named there was opened only by the viewer its permission
        refuses, and the partition above still passed (review of PR-9).
        Mutation: return only `/api/chat/stream` from `network_routes`."""
        from tests.unit import duckdb_off_sweep_doors as doors
        from tests.unit import test_read_fallback_http as fallback_http
        from web.main import app

        monkeypatch.setitem(fallback_http.NETWORK_ROUTES, "/api/admin/users", "a sweep's own")
        named = {e.path for e in doors.network_routes(app)}
        assert named == set(fallback_http.NETWORK_ROUTES), named
        assert "/api/admin/users" not in {e.path for e in fallback_http.swept_routes(app)}
        path, params, _ = doors.example_request(
            "GET", next(e for e in doors.network_routes(app) if e.path == "/api/chat/stream"))
        assert params == {"message": "How are sales this week?"}, params

    def test_every_door_that_reads_a_body_is_given_one(self):
        """A door that declares a body, or reads one itself, is sent the
        example written for it — and every example names a door the app
        has, so one left behind by a renamed route fails here."""
        from tests.unit import duckdb_off_sweep_doors as doors
        from web.main import app

        pairs = doors.write_routes(app) + doors.outside_routes(app)
        keys = {doors.door_key(m, e) for m, e in pairs}
        keys |= {doors.door_key("GET", e) for e in doors.network_routes(app)}
        reading = {doors.door_key(m, e) for m, e in pairs if doors.reads_a_body(e)}
        assert reading <= set(doors.EXAMPLE_BODIES), sorted(reading - set(doors.EXAMPLE_BODIES))
        assert set(doors.EXAMPLE_BODIES) <= keys, sorted(set(doors.EXAMPLE_BODIES) - keys)
        assert set(doors.EXAMPLE_QUERIES) <= keys, sorted(set(doors.EXAMPLE_QUERIES) - keys)

    def test_a_body_is_found_declared_or_read(self):
        """Both kinds: a model in the signature, and `await request.json()`
        in the body. Mutation: read only the declared ones."""
        from tests.routes_helper import find_endpoint
        from tests.unit.duckdb_off_sweep_doors import reads_a_body
        from web.main import app

        assert reads_a_body(find_endpoint(app, "/api/expenses", "POST"))
        assert reads_a_body(find_endpoint(app, "/api/me/preferences", "PATCH"))
        assert reads_a_body(find_endpoint(app, "/auth/webapp", "POST"))
        assert not reads_a_body(find_endpoint(app, "/api/traffic/reclassify", "POST"))

    def test_the_signed_doors_verify(self):
        """The sign-ins and the webhook are sent what Telegram and the gateway
        send, so each is swept past its signature: a door refused at its
        signature checked nothing behind it."""
        from core.turbosms import match_webhook_signature
        from tests.unit import duckdb_off_sweep_doors as doors
        from web.services.auth_service import verify_telegram_auth, verify_webapp_auth

        assert verify_telegram_auth(doors.telegram_login_query(doors.VIEWER_ID))
        user = verify_webapp_auth(doors.webapp_body(doors.VIEWER_ID)["initData"])
        assert user and user["id"] == doors.VIEWER_ID
        body = doors.webhook_body()
        assert match_webhook_signature(body["id"], body["signature"], doors.WEBHOOK_SECRET)

    def test_every_tool_is_called_with_every_value_of_its_enums(self):
        from core.chat_tools import TOOLS
        from tests.unit.duckdb_off_sweep_doors import tool_inputs, tool_key

        by_name = {t["name"]: t for t in TOOLS}
        inputs = tool_inputs(by_name["get_top_products"])
        assert {i["by"] for i in inputs} == {"revenue", "quantity"}
        assert {i["period"] for i in inputs} == set(
            by_name["get_top_products"]["input_schema"]["properties"]["period"]["enum"])
        assert tool_key("get_top_products", inputs[0], inputs[0]) == "tool:get_top_products"
        dates = tool_inputs(by_name["compare_date_ranges"])[0]
        assert all(len(dates[k]) == 10 for k in dates), dates
        expense = tool_inputs(by_name["add_expenses"])[0]["expenses"][0]
        assert {"category", "type", "amount"} <= set(expense)

    def test_the_model_asks_for_every_tool_and_then_answers(self):
        """The fake model's two turns, and the results it is handed counted —
        which is what lets the outer test say a route ran every tool."""
        from core.chat_tools import TOOLS
        from tests.unit.duckdb_off_sweep_doors import FakeModel

        model = FakeModel(TOOLS)

        async def turns():
            first = await model.chat([{"role": "user", "content": "q"}])
            results = [{"type": "tool_result", "tool_use_id": c["id"], "content": "{}"}
                       for c in first["tool_calls"]]
            second = await model.chat([{"role": "user", "content": results}])
            return first, second

        first, second = asyncio.run(turns())
        assert [c["name"] for c in first["tool_calls"]] == [t["name"] for t in TOOLS]
        assert second["content"] == "done" and not second["tool_calls"]
        assert model.results_seen == [sorted(t["name"] for t in TOOLS)]
