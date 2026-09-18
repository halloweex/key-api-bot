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


@dataclass(frozen=True)
class Expenses:
    allocator: Allocator
    nulls: Nulls


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


Group = Union[Expenses, Inventory, Unwatched, None]


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
UNWATCHED = "chain_invariants_unwatched"

# What a blind run holds rather than resolves — `data_quality`'s
# GUARDED_CHECK_CONDITIONS shape, and `unverified_conditions` below reads it.
CONDITIONS: Tuple[str, ...] = (
    SEQUENCE_BEHIND, COLUMN_NULL, INITIAL_BURST, FIRST_SEEN_RESET,
    ROLLUP_MISSING, SNAPSHOT_SHORT, WATERMARK_STALE,
)

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
    from core import pg_expenses_write, pg_inventory_write

    return {pg_expenses_write.CHAIN: "expenses",
            pg_inventory_write.CHAIN: "inventory"}


# ─── Reading ──────────────────────────────────────────────────────────────────

_ALLOCATOR_SQL = """
SELECT (SELECT last_value FROM {sequence})            AS last_value,
       (SELECT is_called  FROM {sequence})            AS is_called,
       (SELECT MAX(id) FROM {table})                  AS max_id
"""

_EXPENSE_NULLS_SQL = """
SELECT count(*) FILTER (WHERE created_at IS NULL) AS created_at
FROM app.manual_expenses
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


async def _read_expenses(conn) -> Expenses:
    allocator = await _read_allocator(
        conn, "app.manual_expenses", "app.manual_expenses_id_seq")
    row = await conn.fetchrow(_EXPENSE_NULLS_SQL)
    return Expenses(
        allocator=allocator,
        nulls=Nulls(table="app.manual_expenses",
                    counts={"created_at": int(row["created_at"])}))


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
    for chain in WRITE_CHAINS:
        name = chain_name(chain)
        if name in watched:
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
        out.append(WatermarkAge(chain=name, key=key, raw=raw, age_s=age))
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

    from core import pg_inventory_write

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
                    "expenses": _read_expenses,
                    "inventory": lambda c: _read_inventory(c, since, today),
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


def unwatched_issue(reason: str, watched: Tuple[str, ...]):
    from core.data_quality import Severity

    return _issue(
        check_name=UNWATCHED, table_name="(write chains)",
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


def _null_issues(nulls: Nulls, chain: str) -> List:
    from core.data_quality import Severity

    offenders = {c: n for c, n in sorted(nulls.counts.items()) if n}
    if not offenders:
        return []
    return [_issue(
        check_name=COLUMN_NULL, table_name=nulls.table,
        severity=Severity.CRITICAL, count=sum(offenders.values()),
        description=(
            f"{nulls.table} has NULLs the writer must not leave: "
            + ", ".join(f"{c} in {n} row(s)" for c, n in offenders.items())
            + f". These columns have no database default — they were built to "
            "receive the value from the writer, and DuckDB's defaults do not "
            f"travel — so {chain} is not supplying one. A NULL recorded_at is "
            "also invisible to every time-windowed read of this table, this "
            "check's own included."))]


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


def _watermark_issues(marks: Tuple[WatermarkAge, ...]) -> List:
    from core.data_quality import Severity

    issues: List = []
    limit = WATERMARK_MAX_AGE_MIN * 60
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
        if mark.age_s <= limit:
            continue
        issues.append(_issue(
            check_name=WATERMARK_STALE, table_name=mark.key,
            severity=Severity.WARN, count=int(mark.age_s // 60),
            description=(
                f"{mark.key} last moved {mark.age_s / 60:.0f} min ago, over the "
                f"{WATERMARK_MAX_AGE_MIN}-minute limit. That sync falls due an hour "
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
    from core import pg_expenses_write, pg_inventory_write

    if facts is None:
        watched = tuple(sorted(watched_chains()))
        if not watched:
            return []
        return [unwatched_issue(
            "the integrity job judged these invariants without a pre-read; it "
            "must hand this check what core.pg_chain_invariants.read_facts "
            "returned",
            watched)]
    if facts.whole is not None:
        return [unwatched_issue(facts.whole.reason, facts.watched)]
    if not facts.watched:
        return []

    issues: List = []
    if isinstance(facts.expenses, Unwatched):
        issues.append(unwatched_issue(facts.expenses.reason,
                                      (pg_expenses_write.CHAIN,)))
    elif isinstance(facts.expenses, Expenses):
        issues += _allocator_issues(facts.expenses.allocator, pg_expenses_write.CHAIN)
        issues += _null_issues(facts.expenses.nulls, pg_expenses_write.CHAIN)

    if isinstance(facts.inventory, Unwatched):
        issues.append(unwatched_issue(facts.inventory.reason,
                                      (pg_inventory_write.CHAIN,)))
    elif isinstance(facts.inventory, Inventory):
        inv = facts.inventory
        issues += _allocator_issues(inv.allocator, pg_inventory_write.CHAIN)
        issues += _null_issues(inv.nulls, pg_inventory_write.CHAIN)
        if inv.latched_at is not None:
            issues += _initial_burst_issues(inv)
            issues += _first_seen_issues(inv)
            issues += _snapshot_issues(inv)

    if facts.watermarks_unread is not None:
        issues.append(unwatched_issue(
            f"meta.chain_watermarks unreadable: {facts.watermarks_unread.reason}",
            facts.watched))
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
            facts.watched if facts is not None else ())]


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
