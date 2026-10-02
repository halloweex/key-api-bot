"""Standing watch on the tables a write chain has taken to Postgres.

WHAT STOPS WATCHING THE MOMENT A CHAIN SWITCHES

Stage 4 moves writes chain by chain, and when a chain switches, two jobs stand
down for its tables and neither says so: the hourly `replicate_operational`
must not ship them, and the daily `reconcile_operational` must not compare them
(`core/write_chains.py` has the reasons). The comparison's stand-down is a
`continue` with no finding — so from the switch onward the only checks these
tables had are gone, and nothing replaces them. For `app.manual_expenses` today
that is one table; for chain 1's six it is the only record there will ever be
of a stock change.

The comparison could not be kept: it compares against a DuckDB that has stopped
receiving these rows, so every new row would be a discrepancy it created
itself. What can be kept is everything that is true of the Postgres copy on its
own — an allocator that must stay above the ids in the table, a column the
writer must supply, a daily snapshot that must be taken, a delta base that must
not be lost. Those are invariants, not comparisons, and none of them needs a
second store.

WHAT THIS WATCHES, AND WHY EACH ONE

Expenses (chain 8):

* **The allocator.** Revision 0031 created `app.manual_expenses_id_seq` with a
  `setval` read from a table that was empty at migration time, and
  `_ensure_expense_id_floor` raises it inside every writing transaction. If
  both were somehow skipped the next typed expense collides with a row that is
  already there, and `id` is what the form edits and deletes by.
* **`created_at`.** The Postgres column has no default — revision 0031 says so
  deliberately, because DuckDB defaults it and the writer supplies it. A NULL
  therefore means a writer that stopped supplying it.

Inventory (chain 1):

* **The allocator**, revision 0030's restated rule: exactly one store writes
  `stock_movements` and that store allocates the id. A sequence at or below
  `MAX(id)` is one movement about to be given a second name.
* **`recorded_at` and `source`**, the same shape as `created_at` one table
  over, and sharper: a movement with a NULL `recorded_at` is invisible to every
  time-windowed read there is — `last_stock_out_at`, the burst check below, and
  anybody looking for when a quantity moved.
* **A burst of `initial` movements.** `plan_stock_movements` labels an offer
  `initial` when the delta base does not hold it, so an `offer_stocks` that was
  emptied, unshipped or read outside the writing transaction re-labels the
  whole catalogue at once. That is not a cosmetic mislabel: the movements of
  that sync are then all wrong, against a zero base, and no later sync can work
  out what the real deltas were.
* **`first_seen_at` moving.** The status rebuild carries it forward out of
  the table it is about to delete; lose the carry-forward and every SKU's
  first-seen date becomes its first order date, or today if it never sold.
  `core/pg_inventory_write.py` names this as the column that makes the table
  irreplaceable. What gives the loss away is a date later than a day the offer
  was already photographed in `app.inventory_sku_history` — an offer cannot be
  first seen after it was seen — and that is exact where a share of "recent"
  dates is not: new SKUs keep arriving for as long as the shop sells.
* **The daily snapshots.** `app.inventory_history` must gain one row a day and
  `app.inventory_sku_history` about one row per offer. The integrity scan
  already watches for a *missing* day in the per-SKU table
  (`inventory_snapshot_gaps`, which follows the writer through a pre-read
  calendar); nothing watches the rolled-up table at all, and nothing watches a
  day that was taken but came out short because the status table was empty
  underneath it.
* **The sync watermarks.** They moved to `meta.chain_watermarks` with the chain
  (revision 0032). `_freshness_check` judges them at 48 h, which was the right
  limit while a stale watermark meant a stale dashboard; a stalled chain now
  means stock changes nobody records one by one, so this watches the same two
  keys at 90 minutes (`WATERMARK_MAX_AGE_MIN` has the arithmetic).

Goals (chain 7a, DN-25):

* **`updated_at` and `is_custom`.** `app.revenue_goals` has no default on
  either — revision 0017 built it to receive DuckDB's values — so the writer
  supplies both — and refuses a write without them — so after its first
  write a NULL is a writer that stopped, and before it one the replication
  carried across; the finding says which (`_null_issues`). Each one costs
  something specific: `updated_at` is the clock the copy-back's handover
  orders two versions of a goal by, so a NULL leaves it nothing to order with;
  and `get_smart_goals` keeps a stored goal only `if is_custom`, so a NULL
  there silently replaces the number a human typed with the suggestion. No
  allocator and no watermark: the table is keyed on `period_type` and has no
  sync behind it.

The expense-type dictionary (chain 6a):

* **Not empty.** `/expenses` names every order-level cost by joining this
  table; with no rows the breakdown collapses into one "Other" and the filter
  empties. The writer upserts and never deletes, so nothing in this chain
  empties it — a table found empty is somebody else's statement, or a flip
  onto a Postgres the hourly copy never reached, and the only path KeyCRM
  serves the dictionary to is the weekly full sync.
* **Every name resolved.** KeyCRM serves some names as localisation keys
  (`dictionaries.expense_types.delivery`) and `core.landing_rows` turns them
  into display names. A key standing in the table is a writer that went round
  that parse — and once the chain moves there is no comparison left to notice
  the two stores naming an expense differently.
* **Not its watermark.** `last_sync_expense_types` moves once a week, so the
  90-minute limit above would page every run; the chain declares
  `CHAIN_WATERMARK_MAX_AGE_MIN = None` and `_freshness_check` judges it at
  192 h from `meta.chain_watermarks`, alone — and from DuckDB's frozen stamp
  until the first full sync under the flag writes one there
  (`CHAIN_WATERMARK_INHERITS_DUCKDB`).

The buyers (chain 4):

* **No orphans, once the chain has written.** A contact or a verdict whose
  buyer `bronze.buyers` does not hold. Every writer of the three tables writes
  a buyer's row, its contacts and its verdict in one transaction, and nothing
  deletes a buyer, so after the handover an orphan is a write that went round
  them. Before it, the verdicts arrive by the hourly full replace out of
  DuckDB and the buyers by the per-tick mirror, and a buyer the mirror has not
  shipped yet is not a defect — so it is judged from the latch on. Production
  held none of either on 2026-09-30.
* **`full_name`.** Postgres allows NULL there and DuckDB does not, so a NULL
  is a buyer `scripts/chain_copy_back.py` refuses to carry back
  (`handover_rows_unwritable`). The shared parse writes `'Unknown'` for a
  blank name; a NULL is a write that went round `landing_rows.buyer_row`.
* **Every buyer's phone and email are in its contact list.** The parse takes
  the buyer's `phone`/`email` columns and its contact rows from the same
  KeyCRM list, so one without the other is a contact list that was deleted and
  not written again — what the SMS audience reads phone numbers from. WARN:
  the buyer is still there. Production held none on 2026-09-30.
* **Not its watermark.** `buyer_sync_stalled` pages on the step's own state at
  90 minutes, and a second limit on the stamp the same step writes would say
  one stall twice (`pg_buyers_write.CHAIN_WATERMARK_MAX_AGE_MIN`).

WHO IS WATCHED: THE CHAIN'S OWN ANSWER, NOT A SECOND ONE

A chain is watched when `core.write_chains.chain_modes()` says its writes go to
Postgres — latched (`core/chain_latch.py`) or flagged, in that order of
authority. Asking the environment here would re-derive a state that already has
one home, and it would get a latched chain with a flag flipped back exactly
backwards.

**With no chain there, nothing is read at all**: `read_facts` returns before
it asks for a pool, so a system that has moved nothing pays a dictionary lookup
per integrity run and cannot fail on a store it is not using.

A chain that is **flagged but not yet latched** gets the invariants that are
true of the Postgres copy whatever wrote it — the allocator, the NULL columns,
the watermarks. The rest are all "since the handover", and a chain that has
written nothing has not had one.

**Production today is that second state, not the first** (2026-09-18):
`KS_WRITE_EXPENSES=postgres` has been live since 09-17 with no expense typed,
so chain 8 is flagged and unlatched, and `KS_WRITE_INVENTORY` is off. Every
integrity run therefore takes one connection and reads `app.manual_expenses`'
allocator and NULL count — silent on zero rows — and a Postgres it cannot read
files `chain_invariants_unwatched` (WARN). Chain 1's reads begin when its flag
does.

A chain that is watched and has **no invariants written here** is reported
unwatched, never judged clean: `_reader_groups` is the list, and a test fails
for any member of `WRITE_CHAINS` missing from it.

STRUCTURE: AN ASYNC READ AND A PURE VERDICT, BESIDE THE POSTGRES TWINS

The facts are read by the integrity job and judged by a pure function, and
both happen in `_run_dq_integrity` next to `core/pg_warehouse_dq.py` — not
inside `check_internal_integrity`. That function runs under the DuckDB
connection, and these are facts about tables DuckDB no longer writes. Judged
there, a DuckDB file that would not open, or any raise in the scan, would lose
every chain finding of the run, and the CRITICAL page — gated on the DuckDB
half having finished — would wait for DuckDB to recover or for the canary's
12-hour run-age limit, about the one set of tables nothing else checks. So
this part keeps the twins' three properties: its own read, its own held
conditions (`unverified_conditions` below), and a page that is not gated on
the DuckDB half. Its conditions all begin with `PREFIX`, which is how the job
announces them cleared on a run whose DuckDB half failed.

Blindness is reported rather than returned as an empty list, `pg_warehouse_dq`'s
rule: a run that could not look must hold its conditions, not announce that a
page cleared. `judge` holds the verdict to the same rule — a raise while
judging is blindness too, not a job that dies before it persists anything.

REPORT-ONLY, LIKE EVERYTHING ELSE ON THIS LAYER

Not one of these findings repairs anything, and two of them must never be
"repaired" by a job at all: an initial burst has already recorded the wrong
deltas, and a reset `first_seen_at` has already lost the dates. A human decides
what the rows should say.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, FrozenSet, List, Mapping, Optional, Tuple, Union

logger = logging.getLogger(__name__)

STATEMENT_TIMEOUT = "15s"
ACQUIRE_TIMEOUT_S = 20

def _window_days() -> int:
    """How far back the per-day snapshot invariants look.

    The same number and the same reasoning as
    `data_quality.INVENTORY_CONTINUITY_WINDOW_DAYS`: a missed day cannot be
    taken again, so reporting it for ever is noise rather than news. Read from
    there rather than typed again, so the two windows over the same tables
    cannot drift — and imported inside the function, like every name this
    module takes from `data_quality`: nothing here needs it at import time.
    """
    from core.data_quality import INVENTORY_CONTINUITY_WINDOW_DAYS

    return INVENTORY_CONTINUITY_WINDOW_DAYS


# How old `last_sync_offers` / `last_sync_stocks` may be. A healthy watermark
# is not "at most an hour" old, and the limit has to sit above the real peak:
#
#     3600 s  the gate — `incremental_sync` runs either sync only once its
#             watermark is past an hour (`core/sync_service.py`), and stamps
#             it after the fetch succeeds;
#   + ~360 s  the tick. After a quiet spell, and always from 23:00 to 07:00
#             (the 01:00 integrity run is inside that window),
#             `_should_skip_sync` refuses to run until 300 s after the
#             previous sync *ended* — `_update_backoff` stamps
#             `_last_sync_time` at the end, not the start — and the job that
#             asks is an `IntervalTrigger(seconds=60)`, so the next attempt
#             lands up to 60 s after that: up to 300 + 60 = 360 s between
#             two syncs, not 300;
#   + ~1-2 min `_heavy_job_lock`, which the sync shares with the two-minute
#             warehouse refresh, and the fetch itself.
#
# So a healthy watermark already reaches ~68 minutes at night, and the 75 this
# started at left about seven: one failed six-minute retry and a slow page.
# At 90, a KeyCRM outage or a web recreate has to outlast ~22 minutes of
# retries before a sample can call the chain stalled.
#
# Since DN-24 the Postgres path stamps `last_sync_stocks` only after the
# rebuild and both snapshots have committed too, and a failed attempt waits
# `core.sync_service.INVENTORY_RETRY_AFTER_S` (10 min) before the next. One
# failure still lands inside 90 (60 + 10 + the tick); a second in a row is a
# fault that has lasted past the retry, which is what this is for.
#
# Not two hours, and not "two consecutive stale runs". The integrity runs are
# six hours apart and a WARN reaches a human at the 09:00 digest, which reads
# the 07:00 run; a stall makes that morning's digest only if its watermark last
# moved before 07:00 minus this limit — 05:30 at 90 minutes. Each half hour
# added moves that back by half an hour and buys only silence about outages
# that were over before anyone could read of them. Two consecutive runs would
# mean a six-hour stall, and state carried between runs that this check does
# not keep. Still about thirty times tighter than the 48 h the freshness check
# allows the same two keys: that limit is about a stale number on a page, this
# one about a chain that has stopped recording.
WATERMARK_MAX_AGE_MIN = 90

# A burst is the whole catalogue re-labelled, so the threshold only has to
# separate "some new SKUs arrived" from "the delta base was gone". Reasoned
# from production's shape — ~900 offers, a handful genuinely new in a day —
# and not back-tested: an order of magnitude sits between the two, so the
# exact number is not what the check rests on.
INITIAL_BURST_PCT = 5.0

# A day's per-SKU snapshot against the current number of tracked offers. Half
# is not a tolerance, it is the gap between "the catalogue changed" and "the
# status table was empty when the photograph was taken" — a catalogue that
# genuinely halved inside the window is worth the look this would cost.
SNAPSHOT_MIN_RATIO = 0.5


# ─── Facts ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Unwatched:
    reason: str


@dataclass(frozen=True)
class Allocator:
    """Where the sequence would start against what the table already holds."""
    table: str
    sequence: str
    next_id: Optional[int]
    max_id: Optional[int]


@dataclass(frozen=True)
class Nulls:
    """`{column: rows with NULL}` for the columns the writer must supply."""
    table: str
    counts: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class WatermarkAge:
    chain: str
    key: str
    raw: Optional[str]
    age_s: Optional[float]
    # The chain's own limit (`watermark_limit_min`), carried with the reading
    # so the pure verdict judges each key against the number its chain chose.
    limit_min: int = WATERMARK_MAX_AGE_MIN


@dataclass(frozen=True)
class Expenses:
    allocator: Allocator
    nulls: Nulls
    # None for a chain that is flagged but has never written here: whatever
    # its table holds arrived before the handover. `_null_issues` says so.
    latched_at: Optional[datetime] = None


@dataclass(frozen=True)
class Goals:
    nulls: Nulls
    latched_at: Optional[datetime] = None


@dataclass(frozen=True)
class ExpenseTypes:
    """The dictionary's size and the names still carrying a localisation key."""
    rows: int
    unresolved: int = 0
    unresolved_sample: Tuple[int, ...] = ()


@dataclass(frozen=True)
class Inventory:
    allocator: Allocator
    nulls: Nulls
    # Everything below is "since the handover" and is None for a chain that is
    # flagged but has never written here — there is no handover to date it from.
    latched_at: Optional[datetime] = None
    # The handover as the warehouse dates it — see `_read_inventory`.
    latch_day: Optional[date] = None
    offers: int = 0
    status_rows: int = 0
    initial_24h: int = 0
    first_seen_moved: int = 0
    first_seen_sample: Tuple[int, ...] = ()
    rollup_missing: Tuple[date, ...] = ()
    snapshot_days: Tuple[Tuple[date, int], ...] = ()
    window_start: Optional[date] = None
    window_end: Optional[date] = None


@dataclass(frozen=True)
class Buyers:
    """What is true of the three buyer tables on their own. The orphan counts
    are read only once the chain has written here (`latched_at`)."""
    nulls: Nulls
    latched_at: Optional[datetime] = None
    buyers: int = 0
    orphan_contacts: int = 0
    orphan_verdicts: int = 0
    orphan_sample: Tuple[int, ...] = ()
    contact_missing: int = 0
    contact_missing_sample: Tuple[int, ...] = ()


@dataclass(frozen=True)
class CatalogueTable:
    """One catalogue table against the last full write's instant (OD-15 (a)).

    `last_ok_at` is `meta.mirror_state.last_ok_at` — the instant the last full
    write (the chain's, or the mirror's before it) stamped on every row it
    carried — and `last_rows` how many distinct rows it carried. `current`,
    `retired` and `around` split the table by `mirrored_at` against it: equal,
    earlier (KeyCRM stopped serving the row) and written round the chain —
    later than it, or, once the chain has written (`recorded`), at or after
    the latch at an instant that is none of the chain's recorded writes
    (`pg_catalogue_write`, "THE CHAIN KEEPS A RECORD OF ITS OWN INSTANTS"). A
    later full write never turns that second kind into retired. None for
    `last_ok_at` is a table no full write has ever stamped."""
    table: str
    rows: int = 0
    last_ok_at: Optional[datetime] = None
    last_rows: Optional[int] = None
    current: int = 0
    retired: int = 0
    around: int = 0
    retired_sample: Tuple[int, ...] = ()
    around_sample: Tuple[int, ...] = ()
    recorded: bool = False
    # Rows the full write before the last one carried and the last one did
    # not: still at that write's instant (the record's `previous`). What a
    # truncated catalogue leaves; 0 until the chain has written.
    dropped: int = 0
    dropped_sample: Tuple[int, ...] = ()


@dataclass(frozen=True)
class Catalogue:
    """Chain 6's two tables. `latched_at` None is a chain flagged that has not
    written here yet: "lost" is not judged then, because the last full write
    was the mirror's, whose `last_rows` counts a repeated payload id twice."""
    tables: Tuple[CatalogueTable, ...] = ()
    latched_at: Optional[datetime] = None


@dataclass(frozen=True)
class Journal:
    """Chain 9 (OD-02 (c)): findings whose run Postgres does not hold. The
    daily comparison still compares the journal — DuckDB is its shadow — so
    this is only what a comparison of two copies cannot see: the property a
    run and its findings land in one transaction, checked where it now
    lives."""
    orphan_issues: int = 0
    orphan_diffs: int = 0
    orphan_sample: Tuple[int, ...] = ()


@dataclass(frozen=True)
class Watchdogs:
    """Chain 10 (OD-02 (c)): per sample table, its newest and oldest
    `sampled_at` — `(table, newest, oldest)`, None for an empty table."""
    spans: Tuple[Tuple[str, Optional[datetime], Optional[datetime]], ...] = ()


@dataclass(frozen=True)
class Ledger:
    """Chains 11a/11b (OD-02 (c)): one report's send ledger. `due` is whether
    the last complete week should have gone out by now — after Wednesday
    00:00 Kyiv, and not before the report's first week."""
    nulls: Nulls
    latched_at: Optional[datetime] = None
    week_start: Optional[date] = None
    due: bool = False
    has_week: bool = True


Group = Union[Expenses, ExpenseTypes, Inventory, Goals, Buyers, Catalogue,
              Journal, Watchdogs, Ledger, Unwatched, None]


@dataclass(frozen=True)
class Facts:
    """Everything the pure checks judge, from one snapshot.

    `watched` is what makes an empty `Facts` readable: no chain watched and no
    blindness is a system that has moved nothing, and it must not look the
    same as a read that came back with nothing. It is not production's state —
    chain 8 is flagged there (module docstring).
    """
    watched: Tuple[str, ...] = ()
    whole: Optional[Unwatched] = None
    now: Optional[datetime] = None
    expenses: Group = None
    inventory: Group = None
    goals: Group = None
    expense_types: Group = None
    buyers: Group = None
    catalogue: Group = None
    # The shadow chains (OD-02 (c)), appended last.
    journal: Group = None
    watchdogs: Group = None
    weekly_ledger: Group = None
    traffic_ledger: Group = None
    watermarks: Tuple[WatermarkAge, ...] = ()
    watermarks_unread: Optional[Unwatched] = None
    # Watched chains with no reader in `_reader_groups` — moved, and nothing
    # here knows what is true of their tables.
    unread: Tuple[str, ...] = ()

    @classmethod
    def blind(cls, watched: Tuple[str, ...], reason: str) -> "Facts":
        return cls(watched=watched, whole=Unwatched(reason))


# ─── Conditions ───────────────────────────────────────────────────────────────

SEQUENCE_BEHIND = "chain_sequence_behind"
COLUMN_NULL = "chain_required_column_null"
INITIAL_BURST = "chain_initial_movement_burst"
FIRST_SEEN_RESET = "chain_first_seen_reset"
ROLLUP_MISSING = "chain_daily_rollup_missing"
SNAPSHOT_SHORT = "chain_snapshot_rows_short"
WATERMARK_STALE = "chain_watermark_stale"
DICTIONARY_EMPTY = "chain_dictionary_empty"
NAME_UNRESOLVED = "chain_name_unresolved"
BUYER_ORPHANS = "chain_buyer_orphan_rows"
CONTACT_MISSING = "chain_buyer_contact_missing"
# Chain 6, the catalogue (OD-15 (a)): `CatalogueTable`'s three counts judged.
CATALOGUE_EMPTY = "chain_catalogue_empty"
CATALOGUE_WRITTEN_AROUND = "chain_catalogue_written_around"
CATALOGUE_ROWS_LOST = "chain_catalogue_rows_lost"
CATALOGUE_RETIRED = "chain_catalogue_retired"
CATALOGUE_SHORT_WRITE = "chain_catalogue_short_write"
# The shadow chains (OD-02 (c)).
JOURNAL_ORPHANS = "chain_orphan_children"
SAMPLES_STALE = "chain_samples_stale"
RETENTION_UNBOUNDED = "chain_retention_unbounded"
REPORT_WEEK_MISSING = "chain_report_week_missing"
UNWATCHED = "chain_invariants_unwatched"

# What a blind run holds rather than resolves — `data_quality`'s
# GUARDED_CHECK_CONDITIONS shape, and `unverified_conditions` below reads it.
CONDITIONS: Tuple[str, ...] = (
    SEQUENCE_BEHIND, COLUMN_NULL, INITIAL_BURST, FIRST_SEEN_RESET,
    ROLLUP_MISSING, SNAPSHOT_SHORT, WATERMARK_STALE,
    DICTIONARY_EMPTY, NAME_UNRESOLVED, BUYER_ORPHANS, CONTACT_MISSING,
    JOURNAL_ORPHANS, SAMPLES_STALE, RETENTION_UNBOUNDED, REPORT_WEEK_MISSING,
)
CONDITIONS += (CATALOGUE_EMPTY, CATALOGUE_WRITTEN_AROUND, CATALOGUE_ROWS_LOST,
               CATALOGUE_RETIRED, CATALOGUE_SHORT_WRITE)

# The family name every condition above carries, and the key the integrity
# job resolves them by when its DuckDB half failed (`resolve_group(...,
# only_prefix=PREFIX)`, the twins' `pg_` arrangement). A prefix is only as
# exact as the names under it, so `tests/unit/test_chain_invariants.py` fails
# if a condition here does not carry it, or if anything else the integrity
# layer files does — `chain_latch_disagrees` and `chain_shipper_overwrote`
# carry it too, but on `mirror_landing`, whose alert group this never touches.
PREFIX = "chain_"


def all_conditions() -> FrozenSet[str]:
    return frozenset(CONDITIONS)


# ─── Which chains ─────────────────────────────────────────────────────────────


def watched_chains() -> Dict[str, Optional[str]]:
    """`{chain: latched_at or None}` for every chain whose writes go here.

    `chain_modes()["mode"]` is the one answer the writers, the sync keys, the
    shipper and the daily comparison all read — latched first, then the flag,
    and None for a flag nobody could parse. Deriving it again from the
    environment is how a latched chain with a flipped-back variable would end
    up unwatched by exactly the check that exists for it.

    Never raises: `chain_modes` carries a flag error out in the mapping rather
    than throwing, and a chain whose flag is not understood stands down with
    the Postgres writers — so it is watched here too. `write_chain_flag_invalid`
    is what says the value is wrong; this still watches its tables.
    """
    from core.write_chains import chain_modes

    return {name: (str(state["latched_at"]) if state["latched_at"] else None)
            for name, state in chain_modes().items()
            if state["mode"] != "duckdb"}


def _reader_groups() -> Dict[str, str]:
    """`{chain: Facts field}` — the chains this module knows the invariants of.

    `watched_chains` follows `WRITE_CHAINS` wherever it goes, and this does
    not: the invariants are written per chain, by hand. A chain registered and
    flipped with no entry here used to be listed as watched, read by nothing
    and judged clean — its tables back where they were before this module
    existed, with a record saying otherwise. `read_facts` now files it as
    unwatched instead, and `tests/unit/test_chain_invariants.py` fails for any
    member of `WRITE_CHAINS` missing from this mapping, so the gap shows in CI
    before it shows at 01:00.
    """
    from core import (
        pg_buyers_write, pg_expense_types_write, pg_expenses_write,
        pg_goals_write, pg_inventory_write,
    )
    from core import pg_catalogue_write
    from core import (
        pg_dq_journal_write, pg_traffic_ledger_write, pg_watchdog_write,
        pg_weekly_ledger_write,
    )

    return {pg_expenses_write.CHAIN: "expenses",
            pg_inventory_write.CHAIN: "inventory",
            pg_goals_write.CHAIN: "goals",
            pg_expense_types_write.CHAIN: "expense_types",
            pg_buyers_write.CHAIN: "buyers",
            pg_catalogue_write.CHAIN: "catalogue",
            # The shadow chains (OD-02 (c)).
            pg_dq_journal_write.CHAIN: "journal",
            pg_watchdog_write.CHAIN: "watchdogs",
            pg_weekly_ledger_write.CHAIN: "weekly_ledger",
            pg_traffic_ledger_write.CHAIN: "traffic_ledger"}


# ─── Reading ──────────────────────────────────────────────────────────────────

_ALLOCATOR_SQL = """
SELECT (SELECT last_value FROM {sequence})            AS last_value,
       (SELECT is_called  FROM {sequence})            AS is_called,
       (SELECT MAX(id) FROM {table})                  AS max_id
"""

# `starts_with` and not LIKE: the prefix carries two underscores, which LIKE
# reads as "any character".
_EXPENSE_TYPES_SQL = """
SELECT count(*) AS rows,
       count(*) FILTER (WHERE starts_with(name, $1)) AS unresolved,
       COALESCE((array_agg(id ORDER BY id)
                 FILTER (WHERE starts_with(name, $1)))[1:10],
                '{}'::int[]) AS sample
FROM bronze.expense_types
"""

_EXPENSE_NULLS_SQL = """
SELECT count(*) FILTER (WHERE created_at IS NULL) AS created_at
FROM app.manual_expenses
"""

# Over the whole table, for `_MOVEMENT_NULLS_SQL`'s reason below: three rows
# at most, every one of them a number somebody typed. NOT measured on
# production, unlike the movements: DuckDB defaults both columns and its
# `set_goal` passes both, so a NULL the replication carried across would have
# had to be inserted by hand. If one exists, this reports it the moment the
# flag goes on — the chain is watched from the flag, before its first write —
# which is the right time to learn it, and why the flip checks this layer
# first.
_GOAL_NULLS_SQL = """
SELECT count(*) FILTER (WHERE updated_at IS NULL) AS updated_at,
       count(*) FILTER (WHERE is_custom IS NULL)  AS is_custom
FROM app.revenue_goals
"""

# Over the whole table and not only the rows since the handover, because a row
# whose `recorded_at` is NULL is exactly the row nothing can date. That would
# make any NULL the replication carried across fire from the moment chain 1
# latches, as a defect of the writer it predates — so it was measured before
# any flip instead: on production, 2026-09-18, 0 of 56 277 `stock_movements`
# rows had a NULL `recorded_at` or `source`, and `manual_expenses` held no rows
# at all. Every NULL this finds is therefore the new writer's.
_MOVEMENT_NULLS_SQL = """
SELECT count(*) FILTER (WHERE recorded_at IS NULL) AS recorded_at,
       count(*) FILTER (WHERE source IS NULL)      AS source
FROM app.stock_movements
"""

# `initial` movements recorded since the handover and in the last 24 h.
#
# **No exemption for the first sync after the handover.** There was one, keyed
# on the earliest batch since the latch, and it guarded a case the handover
# does not produce: `bronze.offer_stocks` is in `replicate_operational`'s
# hourly full replace right up to the latch, and `upsert_stocks` reads it as
# the base inside its own transaction, so the first sync that writes here
# labels `initial` only the offers that arrived since the last shipment — a
# handful. A first sync that labels the whole catalogue is a handover whose
# base never arrived, which is this finding's failure exactly. And "earliest
# since the latch" has no upper bound: when the syncs after a handover changed
# nothing and wrote no movement, the exemption fell on whatever batch came
# first, however late — including the one that had lost its base.
#
# Bounded below by the latch so that a burst DuckDB recorded before the
# handover, and the replication carried across, is not reported as this
# writer's.
_INITIAL_SQL = """
SELECT count(*) AS in_window
FROM app.stock_movements
WHERE movement_type = $2
  AND recorded_at >= GREATEST($1::timestamptz, now() - interval '24 hours')
"""

# An offer first seen AFTER a day it was already photographed.
#
# The snapshot is INSERT..SELECT out of `app.sku_inventory_status`, so a row in
# `app.inventory_sku_history` on day D is the offer standing in the status
# table on D, with a `first_seen_at` no later than D. Nothing deletes an offer
# from `bronze.offer_stocks` — both writers upsert — so it stays in the status
# table and the carry-forward keeps that date for good. A date later than a
# day the offer was already photographed can only be the carry-forward having
# lost it.
#
# Why not a share of recent dates. That was the first form — rows dated on or
# after the handover against the table — and it only ever grew: a new offer is
# dated the day it arrives, so ordinary catalogue growth crossed the threshold
# some months after the handover and paged CRITICAL for ever with nothing
# wrong. Bounding it to "stamped in the last day" would stop that and still
# rest on a threshold: a reset hands every offer that has sold its first
# order date and dates only the never-sold ones today, so the share a real
# reset produces is the never-sold share of the catalogue — a number nobody
# has measured. This needs no threshold, because growth cannot produce it: an
# offer is photographed only from the day it is first seen.
#
# Only dates on or after the handover are judged. Earlier ones are DuckDB's,
# and a date DuckDB moved in March looks exactly like one moved yesterday;
# judging them would fire from the latch about a store this chain replaced.
# The price is named: a reset hands an offer that has sold its first order
# date, usually before the handover, and nothing in Postgres remembers the
# date it replaced. What gives a reset away is the never-sold offers — it
# moves every one of them to the day it happened, and every one of them was
# photographed before that.
#
# It reads the whole history table, four times a day. Measured at production's
# size on a throwaway 17.2 — 162 870 rows over 890 offers, against production's
# 162 883 on 2026-09-18 — it costs 0.1 ms while no status row is dated on or
# after the handover (the aggregate is never executed) and 58 ms with every row
# reset, the worst case there is, against a 15 s statement timeout.
_FIRST_SEEN_SQL = """
WITH first_photo AS (
    SELECT offer_id, MIN(date) AS day
    FROM app.inventory_sku_history
    GROUP BY offer_id
)
SELECT count(*) AS moved,
       COALESCE((array_agg(s.offer_id ORDER BY s.offer_id))[1:10],
                '{}'::int[]) AS sample
FROM app.sku_inventory_status s
JOIN first_photo f ON f.offer_id = s.offer_id
WHERE s.first_seen_at >= $1
  AND f.day < s.first_seen_at
"""

# `full_name` only: the one column of the three tables DuckDB declares NOT
# NULL and Postgres does not. Every other NOT NULL of DuckDB's is NOT NULL here
# too, so the server refuses it before this could see it.
_BUYER_NULLS_SQL = """
SELECT count(*) AS buyers,
       count(*) FILTER (WHERE full_name IS NULL) AS full_name
FROM bronze.buyers
"""

# A buyer's `phone`/`email` column and its contact rows come from one KeyCRM
# list (`landing_rows.buyer_row` / `contact_rows`), so a column value with no
# contact row is a contact list deleted and not written again. 20 657 buyers
# and 34 379 contacts on 2026-09-30, both anti-joins on the contacts' key.
#
# An EMPTY column is not a value: `Buyer.from_api` keeps a list that starts
# with '' as it is and takes `phone = phones[0]`, the parse stores that '' in
# the column, and `contact_rows` skips falsy values — so '' with no contact
# row is the shared parse working, and a rewrite would reproduce it.
_CONTACT_MISSING_SQL = """
SELECT count(*) AS missing,
       COALESCE((array_agg(b.id ORDER BY b.id))[1:10], '{}'::int[]) AS sample
FROM bronze.buyers b
WHERE (NULLIF(b.phone, '') IS NOT NULL AND NOT EXISTS (
           SELECT 1 FROM bronze.buyer_contacts c
           WHERE c.buyer_id = b.id AND c.contact_type = 'phone'
             AND c.value = b.phone))
   OR (NULLIF(b.email, '') IS NOT NULL AND NOT EXISTS (
           SELECT 1 FROM bronze.buyer_contacts c
           WHERE c.buyer_id = b.id AND c.contact_type = 'email'
             AND c.value = b.email))
"""

_BUYER_ORPHANS_SQL = """
WITH orphans AS (
    SELECT c.buyer_id, 'contact' AS kind
    FROM bronze.buyer_contacts c
    WHERE NOT EXISTS (SELECT 1 FROM bronze.buyers b WHERE b.id = c.buyer_id)
    UNION ALL
    SELECT g.buyer_id, 'verdict'
    FROM app.buyer_gender g
    WHERE NOT EXISTS (SELECT 1 FROM bronze.buyers b WHERE b.id = g.buyer_id)
)
SELECT count(*) FILTER (WHERE kind = 'contact') AS contacts,
       count(*) FILTER (WHERE kind = 'verdict') AS verdicts,
       COALESCE((array_agg(DISTINCT buyer_id ORDER BY buyer_id))[1:10],
                '{}'::int[]) AS sample
FROM orphans
"""

# One catalogue table split by `mirrored_at` against the last full write's
# instant ($1) and, once the chain has written, against its record: $2 the
# latch (the owner row's `updated_at`, Postgres's clock), $3 the recorded
# stamps, $4 the full write before the last one. With $2 NULL the record is
# not consulted. The table name is the
# chain's own constant, never input; `{stamp}` is `STAMP_SQL`.
_CATALOGUE_STATE_SQL = (
    "SELECT last_ok_at, last_rows FROM meta.mirror_state WHERE table_name = $1")
_CATALOGUE_RECORD_SQL = (
    "SELECT (SELECT updated_at FROM meta.chain_watermarks WHERE key = $1) AS latched, "
    "(SELECT value FROM meta.chain_watermarks WHERE key = $2) AS record")
_CATALOGUE_SPLIT_SQL = """
WITH r AS (
    SELECT id, mirrored_at, {stamp} AS stamp,
           COALESCE(mirrored_at > $1
                    OR ($2::timestamptz IS NOT NULL AND mirrored_at >= $2
                        AND NOT {stamp} = ANY($3::bigint[])), false) AS around
    FROM {table}
)
SELECT count(*)                                                    AS rows,
       count(*) FILTER (WHERE mirrored_at = $1 AND NOT around)     AS current,
       count(*) FILTER (WHERE mirrored_at < $1 AND NOT around)     AS retired,
       count(*) FILTER (WHERE around)                              AS around,
       (array_agg(id ORDER BY id) FILTER (WHERE mirrored_at < $1 AND NOT around))[1:10]
                                                                   AS retired_sample,
       (array_agg(id ORDER BY id) FILTER (WHERE around))[1:10]     AS around_sample,
       count(*) FILTER (WHERE stamp = $4 AND NOT around)           AS dropped,
       (array_agg(id ORDER BY id) FILTER (WHERE stamp = $4 AND NOT around))[1:10]
                                                                   AS dropped_sample
FROM r
"""

_KYIV_DAY_SQL = "SELECT ($1::timestamptz AT TIME ZONE 'Europe/Kyiv')::date"

_ROLLUP_SQL = """
SELECT d::date AS day
FROM generate_series($1::date, $2::date, interval '1 day') AS d
LEFT JOIN app.inventory_history h ON h.date = d::date
WHERE h.date IS NULL
ORDER BY 1
"""

_SNAPSHOT_SQL = """
SELECT date, count(*) AS rows
FROM app.inventory_sku_history
WHERE date BETWEEN $1 AND $2
GROUP BY date ORDER BY date
"""


_JOURNAL_ORPHANS_SQL = """
    WITH orphans AS (
        SELECT 'issues' AS child, i.run_id FROM app.data_quality_issues i
        WHERE NOT EXISTS (SELECT 1 FROM app.data_quality_runs r
                          WHERE r.run_id = i.run_id)
        UNION ALL
        SELECT 'diffs', d.run_id FROM app.data_quality_diffs d
        WHERE NOT EXISTS (SELECT 1 FROM app.data_quality_runs r
                          WHERE r.run_id = d.run_id)
    )
    SELECT count(*) FILTER (WHERE child = 'issues') AS issues,
           count(*) FILTER (WHERE child = 'diffs') AS diffs,
           (SELECT array_agg(run_id ORDER BY run_id)
              FROM (SELECT DISTINCT run_id FROM orphans
                    ORDER BY run_id LIMIT 10) s) AS sample
    FROM orphans
"""


async def _read_journal(conn) -> Journal:
    row = await conn.fetchrow(_JOURNAL_ORPHANS_SQL)
    return Journal(orphan_issues=int(row["issues"] or 0),
                   orphan_diffs=int(row["diffs"] or 0),
                   orphan_sample=tuple(int(x) for x in (row["sample"] or ())))


def ledger_week_due(today: date, floor: Optional[date]) -> Tuple[date, bool]:
    """The last complete Monday–Sunday week as of `today` (Kyiv), and whether
    its report should have gone out: from Wednesday on — Monday's tick and
    Tuesday's retry have both had their turn — and not before `floor`."""
    from datetime import timedelta

    week_start = today - timedelta(days=today.weekday() + 7)
    due = today.weekday() >= 2 and (floor is None or week_start >= floor)
    return week_start, due


async def _read_ledger(conn, chain, today: date,
                       latched_at: Optional[datetime] = None) -> Ledger:
    table = chain.TABLE
    week_start, due = ledger_week_due(today, chain.first_week())
    row = await conn.fetchrow(
        f"SELECT count(*) FILTER (WHERE sent_at IS NULL) AS sent_at, "
        f"bool_or(week_start = $1 AND sales_type = $2) AS has_week FROM {table}",
        week_start, chain.report_sales_type())
    return Ledger(nulls=Nulls(table=table, counts={"sent_at": int(row["sent_at"])}),
                  latched_at=latched_at, week_start=week_start, due=due,
                  has_week=bool(row["has_week"]))


async def _read_watchdogs(conn) -> Watchdogs:
    from core.pg_watchdog_write import CHAIN_TABLES

    spans = []
    for table in CHAIN_TABLES:
        row = await conn.fetchrow(
            f"SELECT max(sampled_at) AS newest, min(sampled_at) AS oldest FROM {table}")
        spans.append((table, row["newest"], row["oldest"]))
    return Watchdogs(spans=tuple(spans))


async def _read_allocator(conn, table: str, sequence: str) -> Allocator:
    row = await conn.fetchrow(
        _ALLOCATOR_SQL.format(sequence=sequence, table=table))
    last, called = row["last_value"], row["is_called"]
    # A sequence that has never been called hands out `last_value` itself; one
    # that has hands out the next. `setval(seq, n, false)` — what revisions
    # 0030 and 0031 both do — is the first case, so reading `last_value` alone
    # would report every fresh database as one short. Either misreading is
    # silent on a healthy sequence, so both directions are set on a real one
    # at MAX(id) in `tests/integration/test_chain_invariants.py`.
    next_id = None if last is None else int(last) + (1 if called else 0)
    return Allocator(table=table, sequence=sequence, next_id=next_id,
                     max_id=None if row["max_id"] is None else int(row["max_id"]))


async def _read_expenses(conn, latched_at: Optional[datetime] = None) -> Expenses:
    allocator = await _read_allocator(
        conn, "app.manual_expenses", "app.manual_expenses_id_seq")
    row = await conn.fetchrow(_EXPENSE_NULLS_SQL)
    return Expenses(
        allocator=allocator,
        nulls=Nulls(table="app.manual_expenses",
                    counts={"created_at": int(row["created_at"])}),
        latched_at=latched_at)


async def _read_goals(conn, latched_at: Optional[datetime] = None) -> Goals:
    row = await conn.fetchrow(_GOAL_NULLS_SQL)
    return Goals(nulls=Nulls(
        table="app.revenue_goals",
        counts={"updated_at": int(row["updated_at"]),
                "is_custom": int(row["is_custom"])}),
        latched_at=latched_at)


async def _read_expense_types(conn) -> ExpenseTypes:
    # The prefix the shared parse resolves, read from it rather than typed
    # again: a second spelling would stop matching the day the parse changed.
    from core.landing_rows import _LOCALISATION_PREFIX

    row = await conn.fetchrow(_EXPENSE_TYPES_SQL, _LOCALISATION_PREFIX)
    return ExpenseTypes(
        rows=int(row["rows"]), unresolved=int(row["unresolved"]),
        unresolved_sample=tuple(int(i) for i in row["sample"]))


async def _read_buyers(conn, latched_at: Optional[datetime] = None) -> Buyers:
    nulls_row = await conn.fetchrow(_BUYER_NULLS_SQL)
    missing = await conn.fetchrow(_CONTACT_MISSING_SQL)
    nulls = Nulls(table="bronze.buyers",
                  counts={"full_name": int(nulls_row["full_name"])})
    common = dict(nulls=nulls, latched_at=latched_at,
                  buyers=int(nulls_row["buyers"]),
                  contact_missing=int(missing["missing"]),
                  contact_missing_sample=tuple(int(i) for i in missing["sample"]))
    if latched_at is None:
        return Buyers(**common)
    orphans = await conn.fetchrow(_BUYER_ORPHANS_SQL)
    return Buyers(**common,
                  orphan_contacts=int(orphans["contacts"]),
                  orphan_verdicts=int(orphans["verdicts"]),
                  orphan_sample=tuple(int(i) for i in orphans["sample"]))


async def _read_catalogue(conn, latched_at: Optional[datetime] = None) -> Catalogue:
    """Both catalogue tables against their last full write. A table with no
    stamp is read for its size alone — the verdict says why it is unjudged.

    The chain's record of its own writes is consulted from the latch on — the
    owner row of the table, whose `updated_at` is the latching transaction's
    `now()`. No record yet (the chain has latched through the other table) is
    an empty one: nothing at or after the latch is then the chain's. A record
    nobody can parse raises, and the group reads as unwatched."""
    from core import chain_latch
    from core.pg_catalogue_write import (
        CHAIN_TABLES, STAMP_SQL, WriteRecord, parse_record, record_key,
    )

    tables: List[CatalogueTable] = []
    for table in CHAIN_TABLES:
        state = await conn.fetchrow(_CATALOGUE_STATE_SQL, table)
        last_ok_at = state["last_ok_at"] if state else None
        last_rows = state["last_rows"] if state else None
        if last_ok_at is None:
            rows = await conn.fetchval(f"SELECT count(*) FROM {table}")
            tables.append(CatalogueTable(table=table, rows=int(rows),
                                         last_rows=last_rows))
            continue
        mark = await conn.fetchrow(_CATALOGUE_RECORD_SQL,
                                   chain_latch.owner_key(table), record_key(table))
        latched = mark["latched"]
        record = WriteRecord()
        if latched is not None and mark["record"] is not None:
            record = parse_record(mark["record"])
        row = await conn.fetchrow(
            _CATALOGUE_SPLIT_SQL.format(table=table, stamp=STAMP_SQL),
            last_ok_at, latched, sorted(record.stamps), record.previous)
        tables.append(CatalogueTable(
            table=table, rows=int(row["rows"]), last_ok_at=last_ok_at,
            last_rows=None if last_rows is None else int(last_rows),
            current=int(row["current"]), retired=int(row["retired"]),
            around=int(row["around"]),
            retired_sample=tuple(int(i) for i in (row["retired_sample"] or ())),
            around_sample=tuple(int(i) for i in (row["around_sample"] or ())),
            recorded=latched is not None, dropped=int(row["dropped"]),
            dropped_sample=tuple(int(i) for i in (row["dropped_sample"] or ()))))
    return Catalogue(tables=tuple(tables), latched_at=latched_at)


async def _read_inventory(conn, latched_at: Optional[datetime],
                          today: date) -> Inventory:
    from core.landing_rows import MOVEMENT_INITIAL

    allocator = await _read_allocator(
        conn, "app.stock_movements", "app.stock_movements_id_seq")
    nulls_row = await conn.fetchrow(_MOVEMENT_NULLS_SQL)
    nulls = Nulls(table="app.stock_movements",
                  counts={"recorded_at": int(nulls_row["recorded_at"]),
                          "source": int(nulls_row["source"])})
    if latched_at is None:
        return Inventory(allocator=allocator, nulls=nulls)

    offers = int(await conn.fetchval("SELECT count(*) FROM bronze.offers"))
    status_rows = int(await conn.fetchval(
        "SELECT count(*) FROM app.sku_inventory_status"))
    initial = await conn.fetchval(_INITIAL_SQL, latched_at, MOVEMENT_INITIAL)
    # The handover as the warehouse dates it, and not `latched_at.date()`.
    # Every date judged below — `first_seen_at`, a snapshot's `date` — is a
    # Kyiv date the writers take from Postgres' own clock, while the latch
    # stamp is UTC; from 21:00 or 22:00 UTC its `.date()` is the Kyiv day
    # before, which DuckDB wrote, and a bound on the wrong day judges it.
    latch_day = await conn.fetchval(_KYIV_DAY_SQL, latched_at)
    seen = await conn.fetchrow(_FIRST_SEEN_SQL, latch_day)
    since_handover = dict(
        allocator=allocator, nulls=nulls, latched_at=latched_at,
        latch_day=latch_day, offers=offers, status_rows=status_rows,
        initial_24h=int(initial),
        first_seen_moved=int(seen["moved"]),
        first_seen_sample=tuple(int(i) for i in seen["sample"]))

    # A day cannot be missing from before the handover, and a gap older than
    # the window is permanent — `_inventory_snapshot_continuity_check`'s two
    # bounds, for its reasons. Yesterday is the last judged day: today's
    # snapshot is taken by the hourly sync and may simply not have happened yet.
    start = max(latch_day, today - timedelta(days=_window_days()))
    end = today - timedelta(days=1)
    if start > end:
        return Inventory(**since_handover)

    missing = [r["day"] for r in await conn.fetch(_ROLLUP_SQL, start, end)]
    days = [(r["date"], int(r["rows"]))
            for r in await conn.fetch(_SNAPSHOT_SQL, start, end)]
    return Inventory(
        **since_handover,
        rollup_missing=tuple(missing), snapshot_days=tuple(days),
        window_start=start, window_end=end)


def watermark_limit_min(chain) -> Optional[int]:
    """How stale a chain's `last_sync_*` may be before it is called stalled —
    `WATERMARK_MAX_AGE_MIN` unless the chain declares its own, and None for a
    chain whose keys this module does not judge at all."""
    return getattr(chain, "CHAIN_WATERMARK_MAX_AGE_MIN", WATERMARK_MAX_AGE_MIN)


async def _read_watermarks(conn, watched: Mapping[str, Optional[str]],
                           now: datetime) -> Tuple[WatermarkAge, ...]:
    """The `last_sync_*` keys of every watched chain, aged against one `now`.

    Read from `meta.chain_watermarks` because that is where they went with the
    chain (revision 0032). The stored value is what the sync wrote — an ISO
    stamp in the store's timezone — and not `updated_at`, so this and
    `_freshness_check` are judging the same number.
    """
    from core.write_chains import WRITE_CHAINS, chain_name

    wanted: List[Tuple[str, str]] = []
    limits: Dict[str, int] = {}
    for chain in WRITE_CHAINS:
        name = chain_name(chain)
        # A chain whose sync is not hourly says so: None leaves its keys to
        # `_freshness_check` alone (chain 6a's weekly dictionary). Absent means
        # this module's limit, which is what every chain before 6a relied on.
        limit = watermark_limit_min(chain)
        if name in watched and limit is not None:
            limits[name] = limit
            wanted += [(name, key) for key in getattr(chain, "CHAIN_SYNC_KEYS", ())]
    if not wanted:
        return ()
    rows = await conn.fetch(
        "SELECT key, value FROM meta.chain_watermarks WHERE key = ANY($1::text[])",
        [key for _, key in wanted])
    stored = {r["key"]: r["value"] for r in rows}
    out: List[WatermarkAge] = []
    for name, key in wanted:
        raw = stored.get(key)
        age: Optional[float] = None
        if raw:
            try:
                stamp = datetime.fromisoformat(raw)
            except (TypeError, ValueError):
                stamp = None
            if stamp is not None:
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=now.tzinfo)
                age = (now - stamp).total_seconds()
        out.append(WatermarkAge(chain=name, key=key, raw=raw, age_s=age,
                                limit_min=limits[name]))
    return tuple(out)


async def read_facts(*, pool=None) -> Facts:
    """Every fact the invariants are judged on. Never raises.

    **It asks Postgres nothing while no chain writes there.** That early return
    is before the DSN check and before the pool, deliberately: a check that
    opened a connection to discover it had nothing to look at would put a
    Postgres failure on the path of a system that is not using Postgres for
    these tables. It is not production's state — chain 8 is flagged there, so
    this reads its two facts on every run (module docstring).

    "Never raises" includes the pool's own timeout. `pool.acquire(timeout=)`
    raises `TimeoutError`, and letting it out handed the scan `None`, which it
    reports as a job that forgot to pre-read — a code defect named for what was
    an exhausted pool. Only a cancellation goes through.
    """
    watched = watched_chains()
    if not watched:
        return Facts()
    names = tuple(sorted(watched))
    groups_for = _reader_groups()
    unread = tuple(n for n in names if n not in groups_for)

    from core.mirror_reconciliation import configured

    if not configured():
        return Facts.blind(names, "KS_PG_DSN is not set")
    try:
        from core.pg import get_pool, require_revision

        pool = pool or await get_pool()
        await require_revision()
    except Exception as e:  # noqa: BLE001 — becomes the blindness reason
        return Facts.blind(names, f"{type(e).__name__}: {e}")

    from core import (
        pg_buyers_write, pg_expenses_write, pg_goals_write, pg_inventory_write,
    )
    from core import pg_catalogue_write
    from core import pg_traffic_ledger_write, pg_weekly_ledger_write

    groups: Dict[str, Group] = {}
    watermarks_unread: Optional[Unwatched] = None
    try:
        async with pool.acquire(timeout=ACQUIRE_TIMEOUT_S) as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                await conn.execute(
                    f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
                now = await conn.fetchval("SELECT now()")
                today = await conn.fetchval(
                    "SELECT (now() AT TIME ZONE 'Europe/Kyiv')::date")
                since = _stamp(watched.get(pg_inventory_write.CHAIN))
                reader_of = {
                    "expenses": lambda c: _read_expenses(
                        c, _stamp(watched.get(pg_expenses_write.CHAIN))),
                    "inventory": lambda c: _read_inventory(c, since, today),
                    "goals": lambda c: _read_goals(
                        c, _stamp(watched.get(pg_goals_write.CHAIN))),
                    "expense_types": _read_expense_types,
                    "buyers": lambda c: _read_buyers(
                        c, _stamp(watched.get(pg_buyers_write.CHAIN))),
                    "catalogue": lambda c: _read_catalogue(
                        c, _stamp(watched.get(pg_catalogue_write.CHAIN))),
                    "journal": _read_journal,
                    "watchdogs": _read_watchdogs,
                    "weekly_ledger": lambda c: _read_ledger(
                        c, pg_weekly_ledger_write, today,
                        _stamp(watched.get(pg_weekly_ledger_write.CHAIN))),
                    "traffic_ledger": lambda c: _read_ledger(
                        c, pg_traffic_ledger_write, today,
                        _stamp(watched.get(pg_traffic_ledger_write.CHAIN))),
                }
                for group in sorted({groups_for[n] for n in names
                                     if n in groups_for}):
                    try:
                        # A savepoint per group, `pg_warehouse_dq`'s
                        # arrangement: one unreadable table must not take the
                        # transaction and with it the other chain's verdict.
                        async with conn.transaction():
                            groups[group] = await reader_of[group](conn)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:  # noqa: BLE001 — this group only
                        logger.error("chain invariants: %s unreadable: %s", group, e)
                        groups[group] = Unwatched(f"{type(e).__name__}: {e}")
                try:
                    async with conn.transaction():
                        watermarks = await _read_watermarks(conn, watched, now)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001 — reported, not dropped
                    # An empty tuple here used to read exactly like "no chain
                    # declares a sync key" — the one blindness in this module
                    # that came back as silence.
                    logger.error("chain invariants: watermarks unreadable: %s", e)
                    watermarks = ()
                    watermarks_unread = Unwatched(f"{type(e).__name__}: {e}")
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001 — TimeoutError included, see above
        return Facts.blind(names, f"{type(e).__name__}: {e}")

    return Facts(watched=names, whole=None, now=now,
                 expenses=groups.get("expenses"),
                 inventory=groups.get("inventory"),
                 goals=groups.get("goals"),
                 expense_types=groups.get("expense_types"),
                 buyers=groups.get("buyers"),
                 catalogue=groups.get("catalogue"),
                 journal=groups.get("journal"),
                 watchdogs=groups.get("watchdogs"),
                 weekly_ledger=groups.get("weekly_ledger"),
                 traffic_ledger=groups.get("traffic_ledger"),
                 watermarks=watermarks, watermarks_unread=watermarks_unread,
                 unread=unread)


def _stamp(value: Optional[str]) -> Optional[datetime]:
    """The latch stamp as a datetime, or None if it cannot be read.

    An unparseable marker is treated as no handover rather than as now: the
    since-the-handover invariants would otherwise all measure from the moment
    of the read, and every one of them would report its whole table.
    """
    if not value:
        return None
    try:
        stamp = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        logger.error("chain invariants: unparseable latch stamp %r", value)
        return None
    if stamp.tzinfo is None:
        from datetime import timezone

        # `chain_latch` writes UTC and says so; a naive one is an older marker
        # or a hand-written file, and reading it as local time would move the
        # window by the container's offset.
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp


# ─── Judging ──────────────────────────────────────────────────────────────────


def _issue(**kw):
    from core.data_quality import IntegrityIssue

    return IntegrityIssue(**kw)


# What a blindness of everything is filed under — the whole read, a verdict
# that raised, a forgotten pre-read. Each of those is the run's only finding.
WHOLE_PART = "(write chains)"
# The watermarks' blindness, beside any group's in the same run.
WATERMARKS_PART = "meta.chain_watermarks"


def unwatched_issue(reason: str, watched: Tuple[str, ...],
                    part: Optional[str] = None):
    """`part` names what could not be read, and it is the finding's
    `table_name`: one chain's group is `(<chain>)`, the watermarks are
    `meta.chain_watermarks`, everything at once is `(write chains)`.

    It has to differ per part. `app.data_quality_issues` is keyed on
    `(run_id, check_name, table_name)`, and one constant here gave a run with
    two blind groups two findings under one key: Postgres refused the whole
    run under `KS_WRITE_DQ_JOURNAL=postgres`, and under `duckdb` DuckDB took it
    and the hourly copy of the journal — never pruned — failed every hour from
    then on. Left out, it is derived from `watched`, so a group that names its
    one chain is distinct by construction (`tests/unit/test_chain_invariants_unwatched.py`).
    """
    from core.data_quality import Severity

    if part is None:
        part = f"({watched[0]})" if len(watched) == 1 else WHOLE_PART
    return _issue(
        check_name=UNWATCHED, table_name=part,
        severity=Severity.WARN, count=len(watched) or 1,
        description=(
            f"The invariants of {', '.join(watched) or 'the write chains'} "
            f"could not be checked this run: {reason}. Nothing else watches "
            "these tables — the hourly replication and the daily comparison "
            "both stand down for them — so "
            + ", ".join(CONDITIONS)
            + " are held as they were, not cleared."))


def _allocator_issues(allocator: Allocator, chain: str) -> List:
    from core.data_quality import Severity

    if allocator.max_id is None or allocator.next_id is None:
        return []
    if allocator.next_id > allocator.max_id:
        return []
    return [_issue(
        check_name=SEQUENCE_BEHIND, table_name=allocator.table,
        severity=Severity.CRITICAL, count=allocator.max_id - allocator.next_id + 1,
        sample_ids=(allocator.max_id,),
        description=(
            f"{allocator.sequence} would hand out {allocator.next_id} next, and "
            f"{allocator.table} already holds id {allocator.max_id}. {chain} "
            "writes this table, so the next insert collides or gives one row a "
            "second name — the rule revision 0030 restated. The floor is raised "
            "inside every writing transaction, so a sequence below MAX(id) means "
            "that step is not running."))]


def _null_issues(nulls: Nulls, chain: str,
                 latched_at: Optional[datetime]) -> List:
    """One finding per table, worded for what can have put the NULL there.

    Before the chain's first write (`latched_at` None) the chain is watched
    from its flag alone and has written nothing, so a NULL was carried across
    by the replication before the handover and naming the writer would send
    the reader after code that never ran. And the sentence about windowed
    reads is `stock_movements`' alone: its `recorded_at` is what the
    since-the-handover checks window on, while the goals and expenses checks
    read their whole table and have no such column.
    """
    from core.data_quality import Severity

    offenders = {c: n for c, n in sorted(nulls.counts.items()) if n}
    if not offenders:
        return []
    found = (f"{nulls.table} has NULLs the writer must not leave: "
             + ", ".join(f"{c} in {n} row(s)" for c, n in offenders.items())
             + ". These columns have no database default — they were built to "
             "receive the value from the writer, and DuckDB's defaults do not "
             "travel")
    if latched_at is None and nulls.table == "bronze.buyers":
        # Nothing before the handover can have put it there: DuckDB's
        # full_name is NOT NULL, and the mirror ships the shared parse, which
        # writes 'Unknown' for a blank name.
        cause = (f" — and {chain} has not written this table yet, while "
                 "neither DuckDB (NOT NULL there) nor the buyers mirror (the "
                 "shared parse writes 'Unknown') can produce one: it is a hand "
                 "edit or a writer that went round the parse. Correct the row "
                 "before the chain's first write.")
    elif latched_at is None:
        cause = (f" — and {chain} has not written this table yet: it is "
                 "watched from its flag, before its first write, so the NULL "
                 "has been present since before the handover, carried across "
                 "by the replication out of DuckDB. Correct the row before "
                 "the chain's first write.")
    else:
        cause = f" — so {chain} is not supplying one."
    if nulls.table == "app.stock_movements" and "recorded_at" in offenders:
        cause += (" A NULL recorded_at is also invisible to every "
                  "time-windowed read of this table, this check's own "
                  "included.")
    if nulls.table == "bronze.buyers":
        cause += (" DuckDB declares full_name NOT NULL, so "
                  "scripts/chain_copy_back.py refuses to carry such a buyer "
                  "back (handover_rows_unwritable) and the chain cannot be "
                  "rolled back until the row is corrected.")
        if latched_at is not None:
            cause += (" The shared parse writes 'Unknown' for a blank name, "
                      "so a NULL is a write that went round "
                      "core.landing_rows.buyer_row.")
    return [_issue(
        check_name=COLUMN_NULL, table_name=nulls.table,
        severity=Severity.CRITICAL, count=sum(offenders.values()),
        description=found + cause)]


def _initial_burst_issues(inv: Inventory) -> List:
    from core.data_quality import Severity

    counted = inv.initial_24h
    if not inv.offers or not counted:
        return []
    pct = counted / inv.offers * 100.0
    if pct < INITIAL_BURST_PCT:
        return []
    return [_issue(
        check_name=INITIAL_BURST, table_name="app.stock_movements",
        severity=Severity.CRITICAL, count=counted,
        description=(
            f"{counted} 'initial' movement(s) since the handover and in the last "
            f"24 h, against {inv.offers} offer(s) — {pct:.1f}%, over "
            f"{INITIAL_BURST_PCT:.0f}%. An offer is labelled 'initial' when "
            "bronze.offer_stocks does not hold it, so this is the delta base having "
            "been lost, not new stock: every movement in that sync was computed "
            "against zero and the real deltas are not recoverable from anything. "
            "The first sync after a handover is not excused — the hourly "
            "replication delivered the base before the latch, so a whole catalogue "
            "labelled at once there is a base that never arrived."))]


def _first_seen_issues(inv: Inventory) -> List:
    from core.data_quality import Severity

    if not inv.first_seen_moved:
        return []
    shown = ", ".join(str(i) for i in inv.first_seen_sample)
    return [_issue(
        check_name=FIRST_SEEN_RESET, table_name="app.sku_inventory_status",
        severity=Severity.CRITICAL, count=inv.first_seen_moved,
        sample_ids=inv.first_seen_sample,
        description=(
            f"{inv.first_seen_moved} of {inv.status_rows} SKU(s) carry a "
            f"first_seen_at on or after the handover "
            f"({(inv.latch_day or inv.latched_at.date()).isoformat()}) that is "
            "later than a day "
            f"app.inventory_sku_history already photographed them (e.g. offer "
            f"{shown}). An offer is photographed only from the day it is first "
            "seen, so no amount of catalogue growth produces this: the status "
            "rebuild lost the carry-forward and dated these offers again, and the "
            "dates it replaced are not in any other store."))]


def _snapshot_issues(inv: Inventory) -> List:
    from core.data_quality import Severity

    issues: List = []
    if inv.rollup_missing:
        shown = ", ".join(d.isoformat() for d in inv.rollup_missing[:10])
        more = "" if len(inv.rollup_missing) <= 10 else f" (+{len(inv.rollup_missing) - 10} more)"
        issues.append(_issue(
            check_name=ROLLUP_MISSING, table_name="app.inventory_history",
            severity=Severity.CRITICAL, count=len(inv.rollup_missing),
            description=(
                f"{len(inv.rollup_missing)} day(s) since the handover have no "
                f"app.inventory_history row: {shown}{more}. The rolled-up snapshot "
                "is taken from current stock, which KeyCRM serves and never "
                "remembers, so a missed day is missed for good. Nothing else "
                "watches this table: the continuity check reads the per-SKU one.")))

    if inv.status_rows:
        floor = inv.status_rows * SNAPSHOT_MIN_RATIO
        short = [(d, n) for d, n in inv.snapshot_days if n < floor]
        if short:
            shown = ", ".join(f"{d.isoformat()} ({n})" for d, n in short[:10])
            more = "" if len(short) <= 10 else f" (+{len(short) - 10} more)"
            issues.append(_issue(
                check_name=SNAPSHOT_SHORT, table_name="app.inventory_sku_history",
                severity=Severity.WARN, count=len(short),
                description=(
                    f"{len(short)} day(s) since the handover hold far fewer per-SKU "
                    f"rows than the {inv.status_rows} offer(s) tracked today: "
                    f"{shown}{more}. The snapshot is INSERT..SELECT out of "
                    "app.sku_inventory_status, so a short day is a status table that "
                    "was empty or half-rebuilt when the photograph was taken. A day "
                    "with no rows at all is a different finding "
                    "(inventory_snapshot_gaps).")))
    return issues


def _expense_type_issues(et: ExpenseTypes, chain: str) -> List:
    from core.data_quality import Severity

    issues: List = []
    if not et.rows:
        issues.append(_issue(
            check_name=DICTIONARY_EMPTY, table_name="bronze.expense_types",
            severity=Severity.CRITICAL, count=1,
            description=(
                f"bronze.expense_types holds no rows, and {chain} writes it: "
                "/expenses names every order-level cost by joining this table, "
                "so the breakdown is one 'Other' and the type filter is empty. "
                "The writer upserts and never deletes, so something else emptied "
                "it, or the chain was flipped onto a Postgres the hourly copy "
                "never reached. KeyCRM serves this dictionary only to the full "
                "sync, and nothing else will bring it back.")))
    if et.unresolved:
        shown = ", ".join(str(i) for i in et.unresolved_sample)
        issues.append(_issue(
            check_name=NAME_UNRESOLVED, table_name="bronze.expense_types",
            severity=Severity.WARN, count=et.unresolved,
            sample_ids=et.unresolved_sample,
            description=(
                f"{et.unresolved} of {et.rows} expense type(s) are named by a "
                f"KeyCRM localisation key rather than a display name (e.g. id "
                f"{shown}). core.landing_rows.expense_type_rows resolves those, "
                f"so a key standing here is a write that went round the shared "
                f"parse — and with {chain} writing Postgres there is no "
                "comparison left to notice the two stores naming it differently.")))
    return issues


def _buyer_issues(b: Buyers, chain: str) -> List:
    from core.data_quality import Severity

    issues: List = []
    orphans = b.orphan_contacts + b.orphan_verdicts
    if b.latched_at is not None and orphans:
        shown = ", ".join(str(i) for i in b.orphan_sample)
        issues.append(_issue(
            check_name=BUYER_ORPHANS, table_name="bronze.buyers",
            severity=Severity.CRITICAL, count=orphans,
            sample_ids=b.orphan_sample,
            description=(
                f"{b.orphan_contacts} contact row(s) and {b.orphan_verdicts} "
                f"gender verdict(s) name a buyer bronze.buyers does not hold "
                f"(e.g. buyer {shown}). {chain} writes a buyer's row, its "
                "contacts and its verdict in one transaction and nothing "
                "deletes a buyer, so these were written round it — or a buyer "
                "was deleted by hand. The SMS audience joins contacts to "
                "buyers, so an orphaned phone number is in no audience. "
                "scripts/chain_copy_back.py does not refuse them: it reads "
                "contacts through their buyers and leaves an orphaned one "
                "behind, and carries an orphaned verdict into DuckDB — so "
                "--handover will not name them. Correct them before a rollback.")))
    if b.contact_missing:
        shown = ", ".join(str(i) for i in b.contact_missing_sample)
        issues.append(_issue(
            check_name=CONTACT_MISSING, table_name="bronze.buyer_contacts",
            severity=Severity.WARN, count=b.contact_missing,
            sample_ids=b.contact_missing_sample,
            description=(
                f"{b.contact_missing} of {b.buyers} buyer(s) carry a phone or "
                "an email in bronze.buyers that their contact list does not "
                f"hold (e.g. buyer {shown}). Both come from one KeyCRM list in "
                "the shared parse, so this is a contact list deleted and not "
                "written again — and the SMS audience reads phone numbers from "
                "the contact list, not the column. The next write of that "
                "buyer rewrites both; POST /api/duckdb/sync-all-buyers rewrites "
                "every buyer KeyCRM has.")))
    return issues


def _catalogue_issues(cat: Catalogue, chain: str) -> List:
    """OD-15 (a), judged on what Postgres holds. Pure.

    Every rule but "lost" and the short write holds whoever made the last
    full write — the mirror stamps `last_ok_at` in its rows' transaction
    exactly as the chain does — so they are judged from the flag. "Lost" reads
    `last_rows`, which only the chain's own write counts exactly, so it waits
    for that write; the short write reads the record's `previous`, which only
    the chain keeps."""
    from core.data_quality import Severity
    from core.pg_catalogue_write import RETIRED_WARN_MIN_ROWS, RETIRED_WARN_PCT

    issues: List = []
    for t in cat.tables:
        if not t.rows:
            issues.append(_issue(
                check_name=CATALOGUE_EMPTY, table_name=t.table,
                severity=Severity.CRITICAL, count=1,
                description=(
                    f"{t.table} holds no rows, and {chain} writes it: every "
                    "order line Postgres serves joins it for a name, a brand "
                    "and a category, so the dashboard's goods read Unknown. "
                    "The writer upserts and never deletes, so something else "
                    "emptied it — or the chain was flipped onto a Postgres the "
                    "mirror never reached.")))
            continue
        if t.last_ok_at is None:
            issues.append(unwatched_issue(
                f"meta.mirror_state records no full write of {t.table}, so a "
                "retired row cannot be told from a lost one", (chain,)))
            continue
        if t.around:
            shown = ", ".join(str(i) for i in t.around_sample)
            issues.append(_issue(
                check_name=CATALOGUE_WRITTEN_AROUND, table_name=t.table,
                severity=Severity.CRITICAL, count=t.around,
                sample_ids=t.around_sample,
                description=(
                    f"{t.around} row(s) of {t.table} carry a mirrored_at "
                    + ("that is none of the chain's recorded writes "
                       if t.recorded else
                       "later than the last full write of the catalogue ")
                    + f"(e.g. id {shown}). {chain} writes the whole catalogue "
                    "and stamps meta.mirror_state in the same transaction, so "
                    "every row it writes carries that instant exactly"
                    + (", and it records every instant it writes at"
                       if t.recorded else "")
                    + "; any other is a writer going round it, and nothing "
                    "compares this table with DuckDB any more. The finding "
                    "stands until KeyCRM serves the row again or a human "
                    "deletes or restores it.")))
        if (cat.latched_at is not None and t.last_rows is not None
                and t.last_ok_at >= cat.latched_at):
            lost = t.last_rows - t.current - t.around
            if lost > 0:
                issues.append(_issue(
                    check_name=CATALOGUE_ROWS_LOST, table_name=t.table,
                    severity=Severity.CRITICAL, count=lost,
                    description=(
                        f"The last full write of {t.table} carried "
                        f"{t.last_rows} row(s) and {t.current + t.around} of "
                        f"them are still there: at least {lost} were deleted "
                        f"since. {chain} never deletes, so a statement that is "
                        "not the chain's removed them. The next full write "
                        "restores any KeyCRM still serves.")))
        if t.retired:
            shown = ", ".join(str(i) for i in t.retired_sample)
            issues.append(_issue(
                check_name=CATALOGUE_RETIRED, table_name=t.table,
                severity=Severity.INFO, count=t.retired,
                sample_ids=t.retired_sample,
                description=(
                    f"{t.retired} row(s) of {t.table} were not in the last "
                    f"full write (e.g. id {shown}): KeyCRM no longer serves "
                    "them. The writer never deletes, so old orders on them "
                    "keep their names. Not a defect.")))
        # What the LAST write left out of what the one before it carried — not
        # `retired / rows`, which counts every product KeyCRM ever retired:
        # the writer never deletes, so that share only ever rose and, past 5%,
        # warned after every complete write (the chain-6 review's ratchet).
        carried_before = (t.last_rows or 0) + t.dropped
        if t.dropped and carried_before:
            share = t.dropped * 100.0 / carried_before
            if share > RETIRED_WARN_PCT and t.dropped >= RETIRED_WARN_MIN_ROWS:
                issues.append(_issue(
                    check_name=CATALOGUE_SHORT_WRITE, table_name=t.table,
                    severity=Severity.WARN, count=t.dropped,
                    sample_ids=t.dropped_sample,
                    description=(
                        f"The last full write of {t.table} left out {t.dropped} "
                        f"row(s) the write before it carried — {share:.1f}% of "
                        f"{carried_before}, over the {RETIRED_WARN_PCT:g}% KeyCRM "
                        "retires between two writes in the ordinary way. The "
                        "sync's pagination stops on the first short page, so "
                        "this is most likely a truncated catalogue; the next "
                        "complete write carries the rest and clears this.")))
    return issues


def _journal_issues(j: Journal, chain: str) -> List:
    from core.data_quality import Severity

    orphans = j.orphan_issues + j.orphan_diffs
    if not orphans:
        return []
    shown = ", ".join(str(i) for i in j.orphan_sample)
    return [_issue(
        check_name=JOURNAL_ORPHANS, table_name="app.data_quality_runs",
        severity=Severity.CRITICAL, count=orphans, sample_ids=j.orphan_sample,
        description=(
            f"{j.orphan_issues} finding(s) and {j.orphan_diffs} discrepancy "
            f"row(s) name a run app.data_quality_runs does not hold (run "
            f"{shown}). {chain} writes a run and its findings in one "
            "transaction and nothing deletes a run, so these were written "
            "round it, or a run was deleted by hand. The digest and "
            "/api/health/data-quality reach findings through their run, so "
            "these are findings nobody is shown."))]


def _watchdog_issues(w: Watchdogs, chain: str, now: Optional[datetime]) -> List:
    from datetime import timedelta

    from core.data_quality import Severity
    from core.pg_watchdog_write import STALE_AFTER, retention_days

    if now is None:
        return []
    issues: List = []
    keep = retention_days()
    for table, newest, oldest in w.spans:
        limit = STALE_AFTER[table]
        if newest is None or now - newest > limit:
            age = ("holds no sample" if newest is None
                   else f"took its last sample {(now - newest).total_seconds() / 3600:.1f} h ago")
            issues.append(_issue(
                check_name=SAMPLES_STALE, table_name=table, severity=Severity.WARN,
                count=1, description=(
                    f"{table} {age}, past its {limit.total_seconds() / 3600:g} h "
                    f"limit. {chain} writes it in Postgres now, so the watchdog "
                    "differences against nothing newer: a growth or an OOM kill "
                    "behind this gap cannot be seen. The job logs \"sample not "
                    "persisted\" for every tick it could not store.")))
        bound = timedelta(days=keep[table] + 1)
        if oldest is not None and now - oldest > bound:
            issues.append(_issue(
                check_name=RETENTION_UNBOUNDED, table_name=table,
                severity=Severity.WARN, count=1, description=(
                    f"{table}'s oldest sample is {(now - oldest).days} days old, "
                    f"past its {keep[table]}-day retention and a day of slack. "
                    f"The prune runs in the same transaction as {chain}'s "
                    "insert, so it has stopped running or was handed the wrong "
                    "cutoff — and a table that keeps everything is the next "
                    "page about the disk.")))
    return issues


def _ledger_issues(led: Ledger, chain: str) -> List:
    from core.data_quality import Severity

    issues = _null_issues(led.nulls, chain, led.latched_at)
    if led.due and not led.has_week:
        issues.append(_issue(
            check_name=REPORT_WEEK_MISSING, table_name=led.nulls.table,
            severity=Severity.WARN, count=1, description=(
                f"The week starting {led.week_start} has no row in "
                f"{led.nulls.table}, and it is Wednesday or later: either the "
                "report never went out (the job's result in /api/jobs says "
                "why — warehouse_behind, not_delivered, a refusal) or it went "
                "out and the record did not land. A spooled record shows in "
                f"/api/health under write_chains.{chain}.pending; the next "
                "tick lands it.")))
    return issues


def _watermark_issues(marks: Tuple[WatermarkAge, ...]) -> List:
    from core.data_quality import Severity

    issues: List = []
    for mark in marks:
        if mark.raw is None:
            # Absent is what a chain that has not synced since the handover
            # looks like, and the freshness check reports that at its own
            # threshold with its own name. Saying it twice under two names
            # would make one page read as two problems.
            continue
        if mark.age_s is None:
            issues.append(_issue(
                check_name=WATERMARK_STALE, table_name=mark.key,
                severity=Severity.WARN, count=1,
                description=(
                    f"meta.chain_watermarks.{mark.key} holds {mark.raw!r}, which is "
                    f"not a timestamp. {mark.chain} writes it, and nothing can tell "
                    "from it whether that sync is still running.")))
            continue
        if mark.age_s <= mark.limit_min * 60:
            continue
        issues.append(_issue(
            check_name=WATERMARK_STALE, table_name=mark.key,
            severity=Severity.WARN, count=int(mark.age_s // 60),
            description=(
                f"{mark.key} last moved {mark.age_s / 60:.0f} min ago, over the "
                f"{mark.limit_min}-minute limit. That sync falls due an hour "
                "after its last success and is retried on every incremental tick "
                "after that, at most about six minutes apart, so every retry "
                f"{mark.chain} made for over twenty minutes has failed. Until "
                "one succeeds, the stock changes of that time can only be recorded "
                "as one net movement, and what happened in between is gone.")))
    return issues


def check_chain_invariants(facts: Optional[Facts]) -> List:
    """The verdict on everything `read_facts` brought back. Pure.

    `None` is the caller having brought nothing at all — a forgotten pre-read,
    which is the shape `_inventory_snapshot_continuity_check` already guards
    against. It cannot be told apart from a chain that is fine, so it is
    reported rather than assumed harmless.
    """
    from core import (
        pg_buyers_write, pg_expense_types_write, pg_expenses_write,
        pg_goals_write, pg_inventory_write,
    )

    if facts is None:
        watched = tuple(sorted(watched_chains()))
        if not watched:
            return []
        return [unwatched_issue(
            "the integrity job judged these invariants without a pre-read; it "
            "must hand this check what core.pg_chain_invariants.read_facts "
            "returned",
            watched, WHOLE_PART)]
    if facts.whole is not None:
        return [unwatched_issue(facts.whole.reason, facts.watched, WHOLE_PART)]
    if not facts.watched:
        return []

    issues: List = []
    if isinstance(facts.expenses, Unwatched):
        issues.append(unwatched_issue(facts.expenses.reason,
                                      (pg_expenses_write.CHAIN,)))
    elif isinstance(facts.expenses, Expenses):
        issues += _allocator_issues(facts.expenses.allocator, pg_expenses_write.CHAIN)
        issues += _null_issues(facts.expenses.nulls, pg_expenses_write.CHAIN,
                               facts.expenses.latched_at)

    if isinstance(facts.inventory, Unwatched):
        issues.append(unwatched_issue(facts.inventory.reason,
                                      (pg_inventory_write.CHAIN,)))
    elif isinstance(facts.inventory, Inventory):
        inv = facts.inventory
        issues += _allocator_issues(inv.allocator, pg_inventory_write.CHAIN)
        issues += _null_issues(inv.nulls, pg_inventory_write.CHAIN,
                               inv.latched_at)
        if inv.latched_at is not None:
            issues += _initial_burst_issues(inv)
            issues += _first_seen_issues(inv)
            issues += _snapshot_issues(inv)

    if isinstance(facts.goals, Unwatched):
        issues.append(unwatched_issue(facts.goals.reason,
                                      (pg_goals_write.CHAIN,)))
    elif isinstance(facts.goals, Goals):
        issues += _null_issues(facts.goals.nulls, pg_goals_write.CHAIN,
                               facts.goals.latched_at)

    if isinstance(facts.expense_types, Unwatched):
        issues.append(unwatched_issue(facts.expense_types.reason,
                                      (pg_expense_types_write.CHAIN,)))
    elif isinstance(facts.expense_types, ExpenseTypes):
        issues += _expense_type_issues(facts.expense_types,
                                       pg_expense_types_write.CHAIN)

    if isinstance(facts.buyers, Unwatched):
        issues.append(unwatched_issue(facts.buyers.reason,
                                      (pg_buyers_write.CHAIN,)))
    elif isinstance(facts.buyers, Buyers):
        issues += _null_issues(facts.buyers.nulls, pg_buyers_write.CHAIN,
                               facts.buyers.latched_at)
        issues += _buyer_issues(facts.buyers, pg_buyers_write.CHAIN)

    from core import pg_catalogue_write

    if isinstance(facts.catalogue, Unwatched):
        issues.append(unwatched_issue(facts.catalogue.reason,
                                      (pg_catalogue_write.CHAIN,)))
    elif isinstance(facts.catalogue, Catalogue):
        issues += _catalogue_issues(facts.catalogue, pg_catalogue_write.CHAIN)

    # The shadow chains (OD-02 (c)).
    from core import pg_dq_journal_write

    if isinstance(facts.journal, Unwatched):
        issues.append(unwatched_issue(facts.journal.reason,
                                      (pg_dq_journal_write.CHAIN,)))
    elif isinstance(facts.journal, Journal):
        issues += _journal_issues(facts.journal, pg_dq_journal_write.CHAIN)

    from core import pg_watchdog_write

    if isinstance(facts.watchdogs, Unwatched):
        issues.append(unwatched_issue(facts.watchdogs.reason,
                                      (pg_watchdog_write.CHAIN,)))
    elif isinstance(facts.watchdogs, Watchdogs):
        issues += _watchdog_issues(facts.watchdogs, pg_watchdog_write.CHAIN, facts.now)

    from core import pg_traffic_ledger_write, pg_weekly_ledger_write

    for chain_mod, group in ((pg_weekly_ledger_write, facts.weekly_ledger),
                             (pg_traffic_ledger_write, facts.traffic_ledger)):
        if isinstance(group, Unwatched):
            issues.append(unwatched_issue(group.reason, (chain_mod.CHAIN,)))
        elif isinstance(group, Ledger):
            issues += _ledger_issues(group, chain_mod.CHAIN)

    if facts.watermarks_unread is not None:
        issues.append(unwatched_issue(
            f"meta.chain_watermarks unreadable: {facts.watermarks_unread.reason}",
            facts.watched, WATERMARKS_PART))
    issues += _watermark_issues(facts.watermarks)

    for chain in facts.unread:
        issues.append(unwatched_issue(
            "no invariants are defined for this chain — it is registered in "
            "core.write_chains.WRITE_CHAINS and writes Postgres, but "
            "core.pg_chain_invariants._reader_groups has no reader for it, so "
            "nothing here reads its tables", (chain,)))
    return issues


# ─── What the integrity job calls ─────────────────────────────────────────────


def judge(facts: Optional[Facts]) -> List:
    """`check_chain_invariants`, with a raise turned into blindness. Never raises.

    The job calls this outside the DuckDB half, where no `guarded` stands
    around it, and a raise there would escape `_run_dq_integrity` before the
    run is persisted — losing the DuckDB half's findings with this one's. So a
    verdict that cannot be reached is reported the way a read that cannot be
    made already is: `chain_invariants_unwatched`, which holds every condition
    rather than letting the run announce any of them cleared.
    """
    try:
        return check_chain_invariants(facts)
    except Exception as e:  # noqa: BLE001 — becomes the blindness reason
        logger.exception("chain invariants: the verdict raised")
        # Named from the facts alone: asking `watched_chains()` again here
        # could raise the very thing the verdict just did.
        return [unwatched_issue(
            f"judging the facts raised {type(e).__name__}: {e}",
            facts.watched if facts is not None else (), WHOLE_PART)]


def unverified_conditions(issues) -> List[str]:
    """The conditions this run could not re-examine: every one, or none.

    `chain_invariants_unwatched` in the run's findings means some part of the
    facts was not read — one chain's tables, the watermarks, or all of it —
    and a part is enough: nothing else watches these tables, so a run that
    could not read one of them must not announce that any page over them
    cleared. Per-group holding would be finer and is not worth the second map
    it needs; the finding names which part was blind.
    """
    if any(i.check_name == UNWATCHED for i in issues):
        return sorted(CONDITIONS)
    return []
