"""`KS_DUCKDB=off`'s coherence verdict: may this web run without the file?

Stage 5's week of silence (OD-17 (a)) runs web with `KS_DUCKDB=off`, and off
refuses every open of the DuckDB file. That is only coherent when nothing web
does still needs the file, and most of what decides that is configuration
somebody can get wrong: a cached mode left at `duckdb` is not one refused open
but one a minute, for ever — the warehouse tick, a chain's writer, every
session read. So, like step 13's switch, off is a claim that is checked at the
start rather than trusted (stage 5's PR-2, `.planning/DUCKDB_EXIT_STAGE5_
DECOUPLING.md` 2.2).

THE KEYS, in the order they are reported:

- `warehouse_postgres` — step 13 is **in force**: `KS_WRITE_WAREHOUSE=postgres`
  with every one of its preconditions met, so Postgres alone derives. That
  covers every read switch, the SMS store, the Postgres twins, the UTM parse,
  `KS_READ_FALLBACK=off` and the cohorts on ClickHouse: all of them are its
  preconditions. **`writes_postgres()` alone is not that**: with chain 3 or 5
  latched a start that would run as `duckdb` — a precondition unmet, the
  variable unset, a typo — has its way back refused and stays `postgres`
  (`warehouse_cutover.way_back_refused()`), with whatever was unmet still
  unmet. Off would hold over a reader left on DuckDB, a refusal on every
  request (review of PR-2).
- `chain:<name>`, one per chain in `core.write_chains.WRITE_CHAINS` —
  **derived from the registry, never listed**: the chain writes Postgres
  (`chain_modes()[name]["mode"]`, the latch or the flag with its own
  preconditions). A chain added to the registry is a key here the day it is.
- one per store switch step 13 does not hold, named after it (`user_store`
  for `KS_USER_STORE`) — **derived too**: `core.duckdb_table_fates.
  STORE_SWITCHES`, the switches whose other side is DuckDB, less the ones
  `warehouse_postgres` already requires (`KS_WRITE_WAREHOUSE` itself and
  step 13's `WAREHOUSE_READERS`, `KS_SMS_STORE` among them). Today that is
  the user list alone: the session read of every request goes to the store
  it names. A store switch registered later is a key the day it is.
- `writer_record` — the frozen copy of step 13's record in Postgres,
  `app.sync_metadata`'s `warehouse_writer`, says `postgres`, resolved: the
  settle under off reads it there and writes nothing (OD-S5-2 (a)).
- `sync_metadata_copied` — the values the chains froze in DuckDB's
  `sync_metadata` reached its copy, which is what off reads in their place:
  every chain that keeps such a value (`CHAIN_SYNC_KEYS` or
  `CHAIN_MARKER_KEYS`, the copy-back's `_carried_keys`) has an `owner:` row,
  and `meta.mirror_state.last_ok_at` for the copy is later than the first
  one of each. **Only those chains**, and **every one latched**: the key
  must hold for the whole week, and under off nothing moves the copy. A
  chain that keeps nothing there — chain 8, flagged with no expense typed —
  latches mid-week on its first write, and counting its owner row held the
  next start back (a deploy), which opened the file and lost the week. A
  chain that keeps a value there and has not latched would do the same, so
  it holds the first start instead, while waiting costs nothing (review of
  PR-2).
- `retired_pages_clear` — no delivered page stands under a condition only a
  check off stands down reports (`retired_conditions`, and the pages of the
  three tables only DuckDB writes, `HISTORY_MARK`). Step 13's
  `retired_conditions_clear`, for off: the first run under off would announce
  such a page "✅ Resolved" with no check looking at it.

The two Postgres facts are asked only once step 13 is in force in this start —
so Postgres answered its revision seconds earlier — on a connection of their
own, in a worker, inside step 13's bound and asked again on a failed read
the same way (`warehouse_cutover.REVISION_READ_*`). A start that could not
reach Postgres has already paid that bound for step 13 and names
`warehouse_postgres` alone — a refused way back included, which is
`postgres` without being in force.

WHAT AN UNMET KEY DOES — OD-S5-1 (a), the owner's, 2026-10-10

The process runs `on`: it opens the file, order intake goes on, and the week
of silence does not start. `/api/health` publishes the keys (never the
details, which quote the environment; those are in web's log) under
`duckdb_switch.held_back`, the canary pages `duckdb_off_held_back` CRITICAL,
and the week-of-silence soak check counts the page as a breach — not the
parallel period's, since DuckDB did not become a writer of anything
(OD-S5-7). OD-09 (b)'s answer, and step 13's: web is the only syncer, so an
incoherent off must not become a stopped order intake.

The verdict is reached once per process and kept: web's startup reaches it
and its scheduler, which configures again, keeps it — a boot that ran `on`
must not hand a scheduler that runs `off`, nor the other way round.

**Only web.** `configure_modes(web=True)` is web's startup and its scheduler;
a script under off reaches no verdict and stays off, refusing every open — the
safe direction for a one-off, which is not the syncer, and a blip in its own
read of Postgres must not open the file in the middle of a week web is
keeping silent.
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import ModuleType
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

WAREHOUSE_POSTGRES = "warehouse_postgres"
CHAIN_PREFIX = "chain:"
USER_STORE = "user_store"
WRITER_RECORD = "writer_record"
SYNC_METADATA_COPIED = "sync_metadata_copied"
RETIRED_PAGES_CLEAR = "retired_pages_clear"

PG_DSN = "KS_PG_DSN"

# The copy off reads in DuckDB's place, its watermark, and every owner row —
# `updated_at` is Postgres's `now()` at the claim, the copy's clock.
_COPIED_SQL = "SELECT last_ok_at FROM meta.mirror_state WHERE table_name = $1"
_OWNERS_SQL = "SELECT key, updated_at FROM meta.chain_watermarks WHERE key LIKE $1"


def chain_key(chain: ModuleType) -> str:
    """`chain:<name>` for a module of `WRITE_CHAINS`."""
    from core.write_chains import chain_name

    return f"{CHAIN_PREFIX}{chain_name(chain)}"


def store_switches() -> Tuple[str, ...]:
    """The store switches off judges itself, in the registry's order:
    `duckdb_table_fates.STORE_SWITCHES` less those `warehouse_postgres`
    already requires at postgres. Read each time, so a switch registered
    later is one here too."""
    from core import warehouse_cutover as wc
    from core.duckdb_table_fates import STORE_SWITCHES

    covered = {wc.ENV, *wc.WAREHOUSE_READERS}
    return tuple(switch for switch in STORE_SWITCHES if switch not in covered)


def store_key(switch: str) -> str:
    """`KS_USER_STORE` → `user_store`: the key a store switch is published
    under — its name without the prefix, and never its value."""
    name = switch[3:] if switch.startswith("KS_") else switch
    return name.lower()


def keys() -> Tuple[str, ...]:
    """Every key of the verdict, in the order it reports them. The chain and
    store keys are read off their registries each time, so one registered
    later is a key here too."""
    from core.write_chains import WRITE_CHAINS

    return (WAREHOUSE_POSTGRES, *(chain_key(c) for c in WRITE_CHAINS),
            *(store_key(s) for s in store_switches()),
            WRITER_RECORD, SYNC_METADATA_COPIED, RETIRED_PAGES_CLEAR)


def chains_keeping_sync_metadata() -> Tuple[ModuleType, ...]:
    """The registered chains whose latch froze a value in DuckDB's
    `sync_metadata` that off then reads from its copy: a `last_sync_*`
    watermark or a marker. The copy-back's own helper says which, so the
    keys off reads in the copy are the keys a way back carries."""
    from core.chain_transfer import _carried_keys
    from core.write_chains import WRITE_CHAINS

    return tuple(chain for chain in WRITE_CHAINS if _carried_keys(chain))


# ─── What off stands down, and the pages it would leave behind ───────────────
#
# Under off three DuckDB-side verdicts stop (the decoupling's PR-4 and PR-7):
# the primary-key checks over the three landing tables no chain judges in
# Postgres — Postgres holds `PRIMARY KEY (id)` on each (OD-S5-5) — the shadow
# comparison, which reads DuckDB's copy of every shadow chain's table, and the
# comparison of the three tables only DuckDB writes, which freeze as history
# (OD-S5-4). A stood-down check is not a raised one: it files nothing and its
# conditions are not held, so on the first run under off a page it delivered
# resolves with nothing looking. Off waits instead, while DuckDB's checks still
# run and can clear it — step 13's answer to the same problem
# (`warehouse_cutover`, What the stood-down checks leave behind).

# Every condition `compare_shadow` reports; a test reads them out of it.
SHADOW_CONDITIONS: FrozenSet[str] = frozenset({
    "shadow_pruned_rows",
    "shadow_duckdb_only_rows",
    "shadow_missing_in_duckdb",
    "shadow_row_values",
})

# The comparison of the three tables only DuckDB writes reports under the
# `mirror_*` names every comparison in `dq_mirror_landing` shares, and the Gate
# keys a page by its condition and its group alone — `mirror_row_values` on
# `app.sync_metadata` and on `bronze.offers` is one page. Retiring the names
# would hold off over every mirror page, and a restart in the middle of the
# week — a deploy — over one about a table off does not touch would open the
# file. So, as step 13 does for its comparisons (`COMPARISON_MARK`), the job
# says whose page it is: every run that compared them marks the delivered
# pages their findings were still reporting, and unmarks the rest. Only a run
# under `on` compares them, so a page delivered under off is never marked.
HISTORY_MARK = "duckdb_history"


def history_tables() -> Tuple[str, ...]:
    """Every table the daily comparison of what only DuckDB holds sets
    against Postgres and no write chain declares — read off the two
    registries, so it is never a list somebody keeps. Today exactly
    `app.sync_metadata`, `app.warehouse_refreshes` and `app.reconciliation_log`
    (a test pins that, so a fourth is a decision): their DuckDB writers are
    dead in the target state (step 13, OD-10, the chains' watermarks), and
    under off they freeze as history (OD-S5-4)."""
    from core.mirror_reconciliation import APPEND_ONLY_TABLES, OPERATIONAL_TABLES
    from core.write_chains import WRITE_CHAINS

    owned = {table for chain in WRITE_CHAINS for table in chain.CHAIN_TABLES}
    return tuple(spec.pg_table for spec in (*OPERATIONAL_TABLES, *APPEND_ONLY_TABLES)
                 if spec.pg_table not in owned)


def retired_conditions() -> FrozenSet[str]:
    """The conditions only a check off stands down reports, by name: the
    integrity checks of `data_quality.DUCKDB_OFF_STOOD_DOWN` — one list, held
    to the scan by a test — and `compare_shadow`'s."""
    from core.data_quality import DUCKDB_OFF_STOOD_DOWN

    return DUCKDB_OFF_STOOD_DOWN | SHADOW_CONDITIONS


def note_history_reported(found: Sequence[Any]) -> None:
    """After a `dq_mirror_landing` run in which `reconcile_operational`
    reached a verdict under `on`: mark the delivered pages of the job's group
    that its findings over the three history tables were still reporting, and
    unmark the rest — it compared all three. Local state, no I/O; only off's
    verdict reads the mark."""
    from core.alerting import mark_delivered
    from core.data_quality import Severity
    from core.mirror_reconciliation import MIRROR_LAYER

    tables = set(history_tables())
    reported = sorted({i.check_name for i in found
                       if i.severity == Severity.CRITICAL and i.table_name in tables})
    mark_delivered(f"dq:{MIRROR_LAYER}", HISTORY_MARK, reported, complete=True)


def open_retired_pages() -> Dict[str, Optional[str]]:
    """`{condition: group}` for every page delivered and not yet announced
    resolved, in this process's Alert Gate, under a retired condition or
    carrying `HISTORY_MARK`. Local state, no I/O; raises only if the gate
    cannot be read, which the verdict reports as unmet."""
    from core.alerting import delivered_conditions, delivered_marked

    retired = retired_conditions()
    return {**{key: group for key, group in delivered_conditions().items()
               if key in retired},
            **delivered_marked(HISTORY_MARK)}


# ─── The facts and the verdict ───────────────────────────────────────────────

@dataclass(frozen=True)
class Unmet:
    key: str
    detail: str


@dataclass(frozen=True)
class Facts:
    """What the verdict reads beyond the environment.

    `writes_postgres`, `warehouse_mode`, `warehouse_value`,
    `warehouse_unmet` and `way_back_refused` are step 13's cached verdict
    (`in_force` reads them). `chains` is `chain_modes()`, None with
    `chains_error` when it raised. The Postgres half is asked only of a start
    in which step 13 is in force (`pg_asked`); `pg_error` says why a read
    that was asked has no answer. `writer` is the frozen record as
    `warehouse_cutover.parse_record` reads it, `copied_at` the copy's
    `last_ok_at` (None: never copied), `owners` every `owner:` row as
    `{table: updated_at}`. `open_retired` is `open_retired_pages()`, None
    with `open_retired_error` when the gate could not be read."""
    writes_postgres: bool = False
    warehouse_mode: str = "duckdb"
    warehouse_value: Optional[str] = None
    warehouse_unmet: Tuple[str, ...] = ()
    way_back_refused: Tuple[str, ...] = ()
    chains: Optional[Mapping[str, Mapping[str, Any]]] = field(default_factory=dict)
    chains_error: Optional[str] = None
    open_retired: Optional[Mapping[str, Optional[str]]] = field(default_factory=dict)
    open_retired_error: Optional[str] = None
    pg_asked: bool = False
    pg_error: Optional[str] = None
    writer: Optional[Mapping[str, Any]] = None
    copied_at: Optional[datetime] = None
    owners: Mapping[str, datetime] = field(default_factory=dict)


def in_force(writes_postgres: bool, way_back_refused: Sequence[str],
             warehouse_unmet: Sequence[str]) -> bool:
    """Step 13 is in force: Postgres alone derives **because** the switch is
    on with every precondition met — not because a latched chain refused the
    way back (`warehouse_cutover._the_way_back`), which leaves the process
    `postgres` over whatever was unmet. Pure."""
    return writes_postgres and not way_back_refused and not warehouse_unmet


def _instant(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _first_latches(owners: Mapping[str, datetime]
                   ) -> Tuple[Dict[str, datetime], Tuple[str, ...]]:
    """`({chain: its first owner row}, (chain with none, ...))` over the
    chains that keep a value in DuckDB's `sync_metadata`. The first: the
    chain's values stopped moving in DuckDB when its writes moved, at or
    before its first claim, and a table it claims later says nothing about
    them. Pure but for the registry."""
    from core.write_chains import chain_name

    latched: Dict[str, datetime] = {}
    unlatched: List[str] = []
    for chain in chains_keeping_sync_metadata():
        stamps = [_instant(owners[t]) for t in chain.CHAIN_TABLES if owners.get(t)]
        if stamps:
            latched[chain_name(chain)] = min(stamps)
        else:
            unlatched.append(chain_name(chain))
    return latched, tuple(unlatched)


def evaluate(env: Mapping[str, str], facts: Facts) -> List[Unmet]:
    """Every key `env` and `facts` leave unmet, in `keys()` order, each
    naming what it found. Empty is off. Pure over what it is handed and the
    write-chain registry."""
    from core import warehouse_cutover as wc
    from core.pg_operational import SYNC_METADATA_TABLE
    from core.write_chains import WRITE_CHAINS, chain_name

    unmet: List[Unmet] = []

    def need(key: str, ok: bool, detail: str) -> None:
        if not ok:
            unmet.append(Unmet(key, detail))

    unmet13 = (f", unmet: {', '.join(facts.warehouse_unmet)}"
               if facts.warehouse_unmet else "")
    if facts.way_back_refused:
        detail = (f"{wc.ENV}={facts.warehouse_value!r} runs as 'postgres' only "
                  "because the way back is refused — latched write chain(s) "
                  f"{', '.join(facts.way_back_refused)} own what DuckDB derives "
                  f"from{unmet13}. Step 13 is not in force, so its preconditions "
                  "(every read switch, the SMS store, the twins, the UTM parse, "
                  f"{wc.READ_FALLBACK}=off, the cohorts on ClickHouse) are not "
                  "known to hold: a read left on DuckDB would be refused on every "
                  "request")
    else:
        detail = (f"{wc.ENV} runs as {facts.warehouse_mode!r}{unmet13}"
                  + (" — DuckDB still derives Silver, Gold and the UTM verdicts: "
                     "warehouse_refresh is registered and the DuckDB checks over "
                     "them run" if not facts.writes_postgres else
                     " — step 13 is not in force"))
    need(WAREHOUSE_POSTGRES, in_force(facts.writes_postgres, facts.way_back_refused,
                                      facts.warehouse_unmet), detail)

    for chain in WRITE_CHAINS:
        name = chain_name(chain)
        if facts.chains is None:
            need(chain_key(chain), False,
                 f"the write chains could not be read: {facts.chains_error}")
            continue
        state = facts.chains.get(name) or {}
        mode = state.get("mode")
        why = state.get("unmet_precondition") or state.get("error")
        # The flag's value comes from `env`: `chain_modes()` publishes `env`
        # as the variable's *name* (`write_chains._chain_state`), and a held
        # chain's line — the one the canary's lever sends the operator to —
        # must say what the flag is set to (review of PR-2).
        flag = env.get(chain.WRITE_ENV)
        need(chain_key(chain), mode == "postgres",
             (f"{chain.WRITE_ENV}={flag!r}" if flag is not None
              else f"{chain.WRITE_ENV} is unset")
             + f", and the chain writes {mode or 'nowhere it can say'}"
             + (f" ({why})" if why else "")
             + ": its writers would open the DuckDB file")

    for switch in store_switches():
        store = (env.get(switch) or "duckdb").strip().lower() or "duckdb"
        need(store_key(switch), store == "postgres",
             f"{switch} is {store!r}, needs 'postgres': what it stores would be "
             "read and written in DuckDB"
             + (" — every session read" if switch == "KS_USER_STORE" else ""))

    if facts.pg_asked:
        record = facts.writer
        if facts.pg_error:
            detail = f"{SYNC_METADATA_TABLE} could not be read: {facts.pg_error}"
        elif record is None:
            detail = (f"{SYNC_METADATA_TABLE} holds no {wc.WRITER_KEY}: the hourly "
                      "copy never carried step 13's record, and off settles by "
                      "reading it there")
        elif record.get("unreadable"):
            detail = (f"{SYNC_METADATA_TABLE}.{wc.WRITER_KEY} holds a value this "
                      "build did not write")
        elif record.get("writer") != wc.POSTGRES:
            detail = (f"{SYNC_METADATA_TABLE}.{wc.WRITER_KEY} says "
                      f"{record.get('writer')!r}, not postgres")
        else:
            detail = (f"{SYNC_METADATA_TABLE}.{wc.WRITER_KEY} says postgres, not "
                      "resolved: what resolves it writes DuckDB's record, which "
                      "off cannot — let a start under on settle it and the "
                      "hourly copy carry it")
        need(WRITER_RECORD, not facts.pg_error and record is not None
             and not record.get("unreadable") and record.get("writer") == wc.POSTGRES
             and bool(record.get("resolved")), detail)

        copied = _instant(facts.copied_at)
        latched, unlatched = _first_latches(facts.owners or {})
        late = sorted(name for name, at in latched.items()
                      if copied is not None and not copied > at)
        if facts.pg_error:
            ok, detail = False, f"meta.mirror_state could not be read: {facts.pg_error}"
        elif copied is None:
            ok, detail = False, (f"meta.mirror_state has no successful copy of "
                                 f"{SYNC_METADATA_TABLE}: off reads the chains' "
                                 "frozen watermarks there")
        elif unlatched:
            ok, detail = False, (
                f"write chain(s) {', '.join(unlatched)} keep a value in DuckDB's "
                "sync_metadata and have no owner row: the first write latches "
                "each, and under off that is after a copy nothing can move — "
                "the next start, a deploy in the middle of the week, would be "
                "held back here. Let each write once under on, then the hourly "
                "copy run (POST /api/jobs/replicate_operational/trigger)")
        else:
            ok = not late
            detail = (f"the last copy of {SYNC_METADATA_TABLE} ({copied.isoformat()}) "
                      f"is not later than the first latch of {', '.join(late)} "
                      f"({', '.join(latched[n].isoformat() for n in late)}): it may "
                      "not hold the values those chains froze in DuckDB. Under on, "
                      "POST /api/jobs/replicate_operational/trigger")
        need(SYNC_METADATA_COPIED, ok, detail)

    if facts.open_retired is None:
        need(RETIRED_PAGES_CLEAR, False,
             f"the Alert Gate could not be read: {facts.open_retired_error}")
    else:
        pages = ", ".join(f"{key} ({group or 'no group'})"
                          for key, group in sorted(facts.open_retired.items()))
        need(RETIRED_PAGES_CLEAR, not facts.open_retired,
             f"page(s) open under a check off stands down — {pages}. The first "
             "run under off would announce them resolved with no check "
             "looking; let the DuckDB check clear them first")
    return unmet


# ─── Reading the facts ───────────────────────────────────────────────────────

def _local_facts() -> Dict[str, Any]:
    """What this process holds itself. Never raises."""
    from core import warehouse_cutover as wc

    facts: Dict[str, Any] = {
        "writes_postgres": wc.writes_postgres(),
        "warehouse_mode": wc.mode(),
        "warehouse_value": wc.value(),
        "warehouse_unmet": tuple(u.key for u in wc.preconditions_unmet()),
        "way_back_refused": tuple(wc.way_back_refused()),
    }
    try:
        from core.write_chains import chain_modes

        facts["chains"] = chain_modes()
    except Exception as exc:  # noqa: BLE001 — reported as unmet, by class
        logger.error("off's verdict: the write chains could not be read: %s: %s",
                     type(exc).__name__, exc)
        facts["chains"], facts["chains_error"] = None, type(exc).__name__
    try:
        facts["open_retired"] = open_retired_pages()
    except Exception as exc:  # noqa: BLE001 — reported as unmet, by class
        logger.error("off's verdict: the Alert Gate could not be read: %s: %s",
                     type(exc).__name__, exc)
        facts["open_retired"], facts["open_retired_error"] = None, type(exc).__name__
    return facts


async def read_frozen_facts(conn) -> Dict[str, Any]:
    """`{"writer", "copied_at", "owners"}` on `conn`, `owners` being every
    `owner:` row as `{table: updated_at}`. Raises what the reads raise."""
    from core import warehouse_cutover as wc
    from core.chain_latch import OWNER_PREFIX
    from core.pg_operational import SYNC_METADATA_TABLE

    rows = await conn.fetch(_OWNERS_SQL, f"{OWNER_PREFIX}%")
    return {"writer": await wc.read_frozen_writer(conn),
            "copied_at": await conn.fetchval(_COPIED_SQL, SYNC_METADATA_TABLE),
            "owners": {row["key"][len(OWNER_PREFIX):]: row["updated_at"]
                       for row in rows}}


async def _frozen_facts_on_its_own_connection(dsn: str) -> Dict[str, Any]:
    """On a connection opened for it and closed after — never the
    application's pool, which belongs to the loop that opened it."""
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    try:
        return await read_frozen_facts(conn)
    finally:
        await conn.close()


async def _read_postgres(dsn: str) -> Dict[str, Any]:
    """The Postgres facts and `unread` — a read that failed, as against an
    answer. Never raises; an exception is published by its class alone."""
    from core import warehouse_cutover as wc

    timeout = wc.REVISION_READ_TIMEOUT_S
    try:
        done = await asyncio.wait_for(_frozen_facts_on_its_own_connection(dsn),
                                      timeout=timeout)
    except asyncio.TimeoutError:
        return {"pg_error": f"Postgres did not answer in {timeout:g} s", "unread": True}
    except Exception as exc:  # noqa: BLE001 — reported as unmet, by class
        logger.error("off's verdict: Postgres could not be read: %s: %s",
                     type(exc).__name__, exc)
        return {"pg_error": type(exc).__name__, "unread": True}
    return {**done, "pg_error": None, "unread": False}


def _read_postgres_in_a_worker(dsn: str) -> Dict[str, Any]:
    """`_read_postgres` from synchronous code, bounded — step 13's
    `_read_postgres_in_a_worker`, for its reasons. Never raises."""
    import concurrent.futures

    from core import warehouse_cutover as wc

    bound = 2 * wc.REVISION_READ_TIMEOUT_S
    pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="duckdb-off")
    try:
        return pool.submit(lambda: asyncio.run(_read_postgres(dsn))).result(timeout=bound)
    except concurrent.futures.TimeoutError:
        logger.error("off's verdict: Postgres was not read within %g s", bound)
        return {"pg_error": f"Postgres did not answer in {bound:g} s", "unread": True}
    except Exception as exc:  # noqa: BLE001 — _read_postgres promises not to
        logger.error("off's verdict: Postgres could not be read: %s: %s",
                     type(exc).__name__, exc)
        return {"pg_error": type(exc).__name__, "unread": True}
    finally:
        pool.shutdown(wait=False)


def gather_facts(env: Optional[Mapping[str, str]] = None) -> Facts:
    """The facts, from synchronous code. Never raises. Postgres is asked only
    when step 13 is in force (`in_force` — a refused way back is not), and a
    read that failed is asked again before the verdict, step 13's way and
    inside its bound."""
    import time

    from core import warehouse_cutover as wc

    env = os.environ if env is None else env
    facts = _local_facts()
    dsn = (env.get(PG_DSN) or "").strip()
    if in_force(facts["writes_postgres"], facts["way_back_refused"],
                facts["warehouse_unmet"]) and dsn:
        read = _read_postgres_in_a_worker(dsn)
        delays = wc.REVISION_RETRY_DELAYS_S
        for attempt in range(1, wc.REVISION_READ_ATTEMPTS):
            if not read["unread"]:
                break
            delay = delays[min(attempt - 1, len(delays) - 1)]
            logger.warning("off's verdict: Postgres was not read (%s); asking again "
                           "in %g s, %d of %d", read["pg_error"], delay, attempt + 1,
                           wc.REVISION_READ_ATTEMPTS)
            time.sleep(delay)
            read = _read_postgres_in_a_worker(dsn)
        read.pop("unread")
        facts.update(pg_asked=True, **read)
    return Facts(**facts)


# ─── The verdict ─────────────────────────────────────────────────────────────

_unmet: Tuple[Unmet, ...] = ()


def settle(env: Optional[Mapping[str, str]] = None) -> str:
    """Reach the verdict on `KS_DUCKDB=off` for this process, once, and
    return the mode it leaves. Called by `configure_modes(web=True)` last,
    after step 13's verdict and chain 3's start hold, which it reads.

    Under any value but `off` it reads nothing and returns the mode. Under
    `off`, the first call gathers the facts and hands the unmet keys to
    `duckdb_switch.hold`; every later call keeps what the first decided.
    Never raises: a verdict that cannot be reached holds `on`, the direction
    that keeps order intake."""
    from core import duckdb_switch

    global _unmet
    if duckdb_switch.value() != duckdb_switch.OFF or duckdb_switch.held_back() is not None:
        return duckdb_switch.mode()
    env = os.environ if env is None else env
    try:
        unmet = tuple(evaluate(env, gather_facts(env)))
    except Exception as exc:  # noqa: BLE001 — held, and said why
        logger.exception("off's verdict could not be reached")
        unmet = tuple(Unmet(key, f"not judged: the verdict raised {type(exc).__name__}")
                      for key in keys())
    _unmet = unmet
    mode = duckdb_switch.hold([u.key for u in unmet])
    if unmet:
        logger.critical(
            "%s=off, but %d precondition(s) of off are unmet — this process runs "
            "as on: it opens the DuckDB file, and the week of silence does not "
            "start (OD-S5-1 (a)): %s", duckdb_switch.ENV, len(unmet),
            "; ".join(f"{u.key}: {u.detail}" for u in unmet))
    else:
        logger.warning("%s=off and every precondition of off holds: this process "
                       "does not open the DuckDB file", duckdb_switch.ENV)
    return mode


def unmet() -> Tuple[Unmet, ...]:
    """What this process's verdict found unmet, with the details the log
    carries; empty when it holds or none was reached."""
    return _unmet


def reset() -> None:
    """Forget the details. Tests only; `duckdb_switch.reset()` forgets the
    verdict itself."""
    global _unmet
    _unmet = ()
