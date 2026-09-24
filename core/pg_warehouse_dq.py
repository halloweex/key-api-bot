"""Postgres twins of the Silver integrity checks — chain 2, step 8a.

The integrity scan checks Silver in DuckDB: the landing→Silver arc, website
attribution coverage, and the two line-item findings. When DuckDB stops deriving
(step 13) every one of those reads a frozen table: the arc pages four times a day
for rows that are fine, and attribution goes vacuously green. The replacements
must be proven equal on live data first, so they run beside the DuckDB checks,
behind `KS_DQ_PG_WAREHOUSE`, and are compared before anything stands alone.

The design was written and attacked before this was built (`.planning`, step 8
spec and two critics). What this module takes from it:

- **Names carry a `pg_` prefix.** The Gate's delivered map, `take_resolved`, the
  series ledger and the digest delta all key on the condition name alone. A
  twin sharing DuckDB's name would let one engine hold or announce the other's
  recovery, and a page could not say which engine it is about.
- **The grace is absolute**, on `bronze.orders.mirrored_at`. Relative to Silver's
  watermark, a derivation that stopped advancing would leave every new order "in
  flight" for ever — the check reporting zero exactly when Silver stopped being
  built.
- **Missing rows page only past the repair window.** Inside it (grace to
  heartbeat + floor + margin, 80 min) a lost mark or an owed rebuild is still on
  its way, so the finding is WARN. Past it nothing is coming, and it is CRITICAL.
- **Not dependent on KS_PG_DERIVE.** The grace is absolute, so the twins are
  sound under piggyback too, where they are the only pager for a lost in-memory
  owed rebuild.
- **Blindness is reported, never `[]`.** A group that could not be read files
  `pg_<group>_unwatched`; two or more, or the whole snapshot, file one
  `pg_warehouse_unwatched`. `data_quality.unverified_conditions` holds their
  conditions firing rather than announcing a recovery nobody verified.
- **One snapshot under PG_LAYER_LOCK, with a hold budget.** Every TRUNCATE of
  silver.orders and silver.order_utm happens under that lock, so a REPEATABLE
  READ snapshot alone could still catch an emptied Silver. The derivation and
  the DuckDB tick wait for this lock while holding the heavy lock the sync needs,
  so the hold is capped at HOLD_BUDGET_S whatever Postgres is doing.
- **Line items compare, not repeat.** While DuckDB's own headline and goods
  checks look, the twins do not file a second standing row each; they file
  `pg_line_items_disagree` when the engines differ by more than the orders still
  in flight. Only when DuckDB did not look does the twin stand in for it.

Step 8b adds the fourth group, **`pg_silver_row_values`**: every Silver row
recomputed from bronze and compared with what is stored. Silver is rebuilt
whole, so the arc cannot see a row that exists and kept its old values — a
status rewrite, a return that took revenue away — and neither can the
derivation's own validation, whose count and checksum net such changes out.
It stands alone: it compares Postgres with Postgres, not with DuckDB, because
it is the check that has to keep working once DuckDB stops deriving.

- **One recompute text for both engines.** `silver_recompute_ctes` renders pass
  1 and a SELECT form of pass 2; DuckDB's `silver_row_values` reads the same
  function, and the tolerances are its `_SILVER_ROW_COLUMNS`.
- **A row is judged by when its inputs last changed**, not by its own stamp
  alone: `bronze.orders.mirrored_at`; the classification's `mirrored_at` when
  `sales_type` is what moved; and, when a pass-2 column moved, the latest
  change to any order the buyer has or had — a first order cancelled today
  makes a months-old second order the buyer's first. Only data dates a row:
  the grace stays absolute across a restart.
- **Covered is CRITICAL, whatever its age.** A change made before the last
  Silver rebuild began is in that rebuild's snapshot, so a row still wrong
  after it is a rebuild that does not produce what bronze holds — provided
  that rebuild ran the rule this process runs, i.e. began at or after this
  process's first committed rebuild (`pg_silver.FIRST_REBUILT_AT`). The
  rebuild is dated by Silver's own watermark as well as the journal, because
  under piggyback the watermark is the only record a rebuild leaves.
- **In a snapshot whose rule is not known, a row is held.** Until this process
  has rebuilt, the last rebuild ran the previous process's code, and a deploy
  that changed the rule looks exactly like a rebuild at fault. Such rows are
  neither a verdict nor a recovery: an INFO says what they are and the
  condition is held, so a page standing for them stays standing. Past the
  repair window after this process loaded the rule with still no rebuild of
  its own, they are CRITICAL.
- **Not in the snapshot is the missing twin's windows**: in flight (not filed)
  inside the settle grace, WARN inside the repair window, CRITICAL past it —
  the absolute bound, so a derivation that stopped leaves rows paging rather
  than in flight for ever.
- **Last in the snapshot.** It reads the whole of Silver and bronze, so a hold
  budget it spends blinds it alone and the cheap groups keep their verdicts.

DN-14 adds what the soak needs to say "the twins agree" out loud, and one
detector the design asked for and nobody built.

- **`pg_signal_missed`**, from the derivation's own journal (a fifth group,
  `derivation_signal`, read before the recompute). Every run records the mark
  it had seen (`requested_seen`) and how far bronze had got
  (`pg_derivation.HIGH_WATER`). Two consecutive error-free runs where bronze
  moved on and the mark did not are an orders write that raised no mark: the
  rows were rebuilt by the later run, so nothing is lost — on a quiet night
  that run is the heartbeat, an hour late, which is exactly what makes a
  broken mark path invisible to every other check. WARN, over the last day of
  runs, because the digest reads one integrity run a day. A run journalled as
  `first_tick` is not judged: before DN-05b a boot sync wrote with no mark by
  design, and that run is what covered it. Postgres against its own journal,
  not against DuckDB.
- **`pg_twin_pairing`**, one INFO finding per run whose description is JSON:
  both engines' arc, attribution and line-item numbers side by side, and what
  `check_pg_warehouse` would file for the same facts standing alone
  (`duckdb_looked` empty). The line-item twins never file while DuckDB looks —
  they compare — so without this the standalone path step 13 relies on would
  have no record at all when it starts. Pure over facts already read: the
  shadow evaluation costs no read.

DN-23 adds the sixth group, **`order_landing`**: Postgres twins of the DuckDB
checks that read landing itself — `orders_without_line_items`, the orphan line
items, `ordered_at` NULL, the status and source domains, and KeyCRM's status
group against the return list. Once chains 3, 4 and 6 move the writes, DuckDB's
`orders` and `order_products` freeze and those checks go green on frozen data;
with them goes the one detector of a status KeyCRM added (status 20 went
unnoticed for a month).

- **Over `bronze.*`, in the same snapshot and budget**, read before the
  recompute so a budget spent there cannot blind them. Gated on the backfill,
  like the line items: before it every count below DuckDB's is history not yet
  carried, not a finding.
- **Compare while DuckDB looks, stand in when it does not** — the line-item
  rule. Five of DuckDB's six run bare in `check_internal_integrity`
  (`DUCKDB_BARE_CHECKS`): a raise there fails the scan, so the scan finishing
  is their looking. A difference beyond the orders in flight is
  `pg_order_landing_disagree`. A CRITICAL twin whose rows still stand while
  it compares holds its `pg_` condition: a page it delivered on a run DuckDB
  could not look is not announced resolved while Postgres still holds them.
- **In flight is the header's absolute grace.** An order whose header was
  mirrored inside the settle grace is not yet an order without line items;
  it is counted apart and never filed. The other five have no grace: a header
  and its line items land in one transaction (`pg_landing.write_orders`), and
  a domain or a NULL is wrong the moment it lands.
- **DuckDB's own thresholds**: `min_total` from its signature, the known ids
  and the return list from where DuckDB reads them, bound as parameters.

Still deferred, with the owner's decision it needs: making blindness news in
every digest (OD-05).
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple, Union

logger = logging.getLogger(__name__)

ENV = "KS_DQ_PG_WAREHOUSE"
_VALID = ("off", "on")

# How long to wait for PG_LAYER_LOCK — a derivation or a ClickHouse ship holds
# it — and how long this may then hold it, pool acquire included.
#
# The row-values recompute (step 8b) shares this budget rather than taking a
# snapshot of its own, because it fits with room to spare. Measured for DN-13
# on a production-shaped snapshot — three line items an order, UTM rows for
# website orders, skewed buyers, the dead tuples of three 05:15-style
# rewrites, Silver freshly rebuilt and not yet analysed — on
# postgres:17.2-alpine with production's settings (shared_buffers 256MB,
# work_mem 8MB, 512 MB) held to one CPU, on a laptop, two runs of nine: the
# whole snapshot took 0.29–0.32 s (max 0.40) over 48,000 orders and 0.75 s
# (max 0.89) over 96,000, the recompute 0.15–0.19 s and 0.40–0.44 s of it.
# Production held 47,756. The spec asked for this on the gate stack, and that
# measurement was not taken — this change had no access to it; every run logs
# the recompute's own time beside this number instead.
#
# DN-23's landing read shares it too, measured the same way (one CPU, nine
# runs, 0.1% of orders with no line items): 0.05–0.07 s over 48,000 orders and
# 144,000 line items, 0.12 s over 96,000 and 288,000 — four statements, each
# one pass or an index probe per row.
LOCK_WAIT_S = 120
HOLD_BUDGET_S = 20
STATEMENT_TIMEOUT = "15s"

# The margin over the floor for "settled", and over heartbeat + floor for
# "nothing is coming". 20 and 80 minutes at the default 600 s floor.
GRACE_MARGIN_MIN = 10
# The orders mirror's age limit, as the canary judges it (bot/canary.py).
LANDING_MAX_AGE_S = 8 * 3600

WEBSITE_SOURCE_ID = 4

# How far back `pg_signal_missed` judges derivation runs. A day, not the six
# hours between integrity runs: the digest reads the latest integrity run once
# a morning, so a miss must stand in every run for a day to be read at all.
SIGNAL_LOOKBACK_H = 24

# The pairing record's shape. Bumped when a key changes meaning or joins, so a
# soak reading a week of records can tell which rows it may compare. 2: the
# order-landing twins (DN-23), and `duckdb.looked` naming DuckDB's bare checks.
PAIRING_VERSION = 2


def flag() -> Tuple[bool, Optional[str]]:
    """`(enabled, error)`. An unknown value is off, with the error to report.

    Not a raise, `KS_PG_DERIVE`'s rule: this decides whether a check runs, and
    taking the integrity scan down over a typo would blind every other check.
    """
    value = os.getenv(ENV, "off").strip().lower() or "off"
    if value in _VALID:
        return value == "on", None
    return False, f"{ENV}={value!r} is not one of {_VALID}; treated as off"


def floor_seconds() -> int:
    return int(os.getenv("KS_PG_SILVER_INTERVAL_S", "600"))


def silver_grace_minutes() -> int:
    return math.ceil(floor_seconds() / 60) + GRACE_MARGIN_MIN


def page_after_minutes() -> int:
    from core.pg_derivation import HEARTBEAT

    return (math.ceil(HEARTBEAT.total_seconds() / 60)
            + math.ceil(floor_seconds() / 60) + GRACE_MARGIN_MIN)


# ─── Facts ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Unwatched:
    reason: str


@dataclass(frozen=True)
class Watermark:
    today: date
    in_flight: int
    backfilled: bool
    landing_age_s: Optional[int]
    landing_failures: int


@dataclass(frozen=True)
class SilverArc:
    missing: int
    past_repair: int
    missing_amount: float
    oldest_age_s: Optional[int]
    missing_sample: Tuple[int, ...]
    orphans: int
    orphan_amount: float
    orphan_sample: Tuple[int, ...]


@dataclass(frozen=True)
class Attribution:
    orders: int
    tagged: int
    base_orders: int
    base_tagged: int


@dataclass(frozen=True)
class LineItems:
    headline: int
    headline_amount: float
    headline_sample: Tuple[int, ...]
    goods: int
    goods_amount: float
    goods_sample: Tuple[int, ...]


@dataclass(frozen=True)
class RowValues:
    """Silver rows whose stored values a recompute from bronze does not produce.

    `reported` is `covered` + `abandoned` + `overdue` + the rows inside the
    repair window; `in_flight` are the ones still inside the settle grace,
    counted apart and never filed. `held` and `overdue` are rows the last
    rebuild's snapshot held, when that rebuild began before this process's
    first (`first_rebuilt_at`, None while there is none) and so may have run
    another rule: `held` while this process has had the rule
    (`rule_loaded_at`) for less than the repair window, `overdue` past it.
    `compared` and `elapsed_ms` are the recompute's size and cost, kept so the
    hold budget can be judged against the real thing.
    """
    reported: int
    covered: int
    abandoned: int
    in_flight: int
    columns: Tuple[Tuple[str, int], ...]
    sample: Tuple[int, ...]
    oldest_age_s: Optional[int]
    rebuilt_at: Optional[datetime]
    compared: int
    elapsed_ms: int
    held: int = 0
    overdue: int = 0
    first_rebuilt_at: Optional[datetime] = None
    rule_loaded_at: Optional[datetime] = None


@dataclass(frozen=True)
class JournalRun:
    """One error-free derivation run, as `meta.derivation_runs` has it.
    `high_water` is None on a run journalled before DN-14, or over an empty
    bronze; such a run is never judged."""
    id: int
    trigger: str
    started_at: datetime
    requested_seen: Optional[int]
    high_water: Optional[datetime]


@dataclass(frozen=True)
class DerivationJournal:
    """The runs `pg_signal_missed` judges: every error-free run that started at
    or after `since`, in journal order, led by the last one before it — the
    first judged run's predecessor, however old."""
    since: datetime
    runs: Tuple[JournalRun, ...]


@dataclass(frozen=True)
class OrderLanding:
    """What `bronze.orders` and `bronze.order_products` say about themselves:
    the facts DN-23's twins of DuckDB's landing checks judge.

    `without_items` are orders billed at least DuckDB's `min_total` with no
    line item, whose header was mirrored before the settle grace;
    `without_items_in_flight` the same inside it, counted apart and never
    filed. `orphan_items` counts line items, `orphan_sample` the orders they
    name. The domains carry the unknown values themselves and a sample of the
    orders holding them. `status_groups` is one `(status_id, status_group_id,
    orders, amount)` per pair where KeyCRM's group and the return list
    disagree, largest amount first.
    """
    without_items: int = 0
    without_items_in_flight: int = 0
    without_items_amount: float = 0.0
    without_items_sample: Tuple[int, ...] = ()
    orphan_items: int = 0
    orphan_sample: Tuple[int, ...] = ()
    null_ordered_at: int = 0
    null_ordered_at_sample: Tuple[int, ...] = ()
    unknown_status: int = 0
    unknown_status_values: Tuple[int, ...] = ()
    unknown_status_sample: Tuple[int, ...] = ()
    unknown_source: int = 0
    unknown_source_values: Tuple[int, ...] = ()
    unknown_source_sample: Tuple[int, ...] = ()
    status_groups: Tuple[Tuple[int, int, int, float], ...] = ()


Group = Union[SilverArc, Attribution, LineItems, RowValues, DerivationJournal,
              OrderLanding, Unwatched, None]


@dataclass(frozen=True)
class Facts:
    whole: Optional[Unwatched]
    watermark: Optional[Watermark] = None
    silver_arc: Group = None
    attribution: Group = None
    line_items: Group = None
    derivation_signal: Group = None
    order_landing: Group = None
    silver_row_values: Group = None

    @classmethod
    def blind(cls, reason: str) -> "Facts":
        return cls(whole=Unwatched(reason))


# In reading order. The recompute is last on purpose: it is the one group heavy
# enough to spend the hold budget, and a budget spent on it should blind it
# alone rather than the cheap groups queued behind it.
GROUPS = ("silver_arc", "attribution", "line_items", "derivation_signal",
          "order_landing", "silver_row_values")

# The order-landing twins (DN-23), one per DuckDB finding they pair with:
# `(DuckDB's guard, DuckDB's finding, the twin's condition)`. The guard decides
# whether the twin compares (DuckDB looked) or stands in (it did not).
# `status_group_agreement` is the name `check_internal_integrity` guards its
# check under; the other five run bare there, so their guard is the finding's
# own name, and the scan finishing is their looking.
LANDING_TWINS: Tuple[Tuple[str, str, str], ...] = (
    ("orders_without_line_items", "orders_without_line_items",
     "pg_orders_without_line_items"),
    ("fk_orphan_order_products_order_id", "fk_orphan_order_products_order_id",
     "pg_fk_orphan_order_products_order_id"),
    ("not_null_orders_ordered_at", "not_null_orders_ordered_at",
     "pg_not_null_orders_ordered_at"),
    ("value_domain_orders_status_id", "value_domain_orders_status_id",
     "pg_value_domain_orders_status_id"),
    ("value_domain_orders_source_id", "value_domain_orders_source_id",
     "pg_value_domain_orders_source_id"),
    ("status_group_agreement", "status_group_vs_return_list",
     "pg_status_group_vs_return_list"),
)

# The DuckDB checks above that `check_internal_integrity` runs bare, outside
# `guarded`: the integrity job adds them to `duckdb_looked` whenever the scan
# finished, since a raise from any of them would have failed it. A test
# derives each from its call site.
DUCKDB_BARE_CHECKS: FrozenSet[str] = frozenset({
    "orders_without_line_items", "fk_orphan_order_products_order_id",
    "not_null_orders_ordered_at", "value_domain_orders_status_id",
    "value_domain_orders_source_id",
})

# Each guard and the conditions its twin emits — `data_quality.GUARDED_CHECK_CONDITIONS`'
# shape, for the same use: what a blind run must hold rather than resolve.
GUARD_CONDITIONS: Dict[str, Tuple[str, ...]] = {
    "silver_arc": ("pg_silver_missing_rows", "pg_silver_orphan_rows"),
    "attribution": ("pg_attribution_coverage_website",),
    "line_items": ("pg_headline_vs_line_items", "pg_goods_shipped_without_sale",
                   "pg_line_items_disagree"),
    "derivation_signal": ("pg_signal_missed",),
    "order_landing": tuple(twin for _g, _d, twin in LANDING_TWINS) + (
        "pg_order_landing_disagree",),
    "silver_row_values": ("pg_silver_row_values",),
}
UNWATCHED_NAMES: Dict[str, str] = {
    "silver_arc": "pg_silver_arc_unwatched",
    "attribution": "pg_attribution_coverage_unwatched",
    "line_items": "pg_line_items_unwatched",
    "derivation_signal": "pg_derivation_signal_unwatched",
    "order_landing": "pg_order_landing_unwatched",
    "silver_row_values": "pg_silver_row_values_unwatched",
}
WHOLE_UNWATCHED = "pg_warehouse_unwatched"
FLAG_INVALID = "pg_warehouse_dq_flag_invalid"


def all_conditions() -> FrozenSet[str]:
    return frozenset(c for cs in GUARD_CONDITIONS.values() for c in cs)


# ─── Reading ──────────────────────────────────────────────────────────────────

_WATERMARK_SQL = """
SELECT (now() AT TIME ZONE 'Europe/Kyiv')::date AS today,
       (SELECT count(*) FROM bronze.orders
         WHERE mirrored_at >= now() - make_interval(mins => $1)) AS in_flight,
       (SELECT backfilled_at IS NOT NULL FROM meta.mirror_state
         WHERE table_name = 'bronze.orders') AS backfilled,
       (SELECT EXTRACT(EPOCH FROM now() - last_ok_at)::int FROM meta.mirror_state
         WHERE table_name = 'bronze.orders') AS landing_age_s,
       (SELECT failures_since_ok FROM meta.mirror_state
         WHERE table_name = 'bronze.orders') AS landing_failures
"""

_MISSING_SQL = """
SELECT count(*) AS n,
       count(*) FILTER (WHERE o.mirrored_at < now() - make_interval(mins => $2)) AS past_repair,
       COALESCE(SUM(o.grand_total), 0) AS amount,
       EXTRACT(EPOCH FROM now() - MIN(o.mirrored_at))::int AS oldest_age_s,
       (array_agg(o.id ORDER BY o.grand_total DESC NULLS LAST, o.id DESC))[1:10] AS sample
FROM bronze.orders o
LEFT JOIN silver.orders s ON s.id = o.id
WHERE s.id IS NULL
  AND o.mirrored_at < now() - make_interval(mins => $1)
"""

_ORPHAN_SQL = """
SELECT count(*) AS n,
       COALESCE(SUM(s.grand_total), 0) AS amount,
       (array_agg(s.id ORDER BY s.grand_total DESC NULLS LAST, s.id DESC))[1:10] AS sample
FROM silver.orders s
LEFT JOIN bronze.orders o ON o.id = s.id
WHERE o.id IS NULL
"""

# The DuckDB check's windows, with today computed in Kyiv explicitly: Postgres'
# CURRENT_DATE is the UTC date, and the 01:00 Kyiv run always falls in the hours
# where the two differ.
_ATTRIBUTION_SQL = """
SELECT
  count(*) FILTER (WHERE s.order_date >= $1::date - $2::int),
  count(*) FILTER (WHERE s.order_date >= $1::date - $2::int AND u.utm_campaign IS NOT NULL),
  count(*) FILTER (WHERE s.order_date <  $1::date - $2::int),
  count(*) FILTER (WHERE s.order_date <  $1::date - $2::int AND u.utm_campaign IS NOT NULL)
FROM silver.orders s
LEFT JOIN silver.order_utm u ON u.order_id = s.id
WHERE s.source_id = $4 AND NOT s.is_return AND s.is_active_source
  AND s.order_date >= $1::date - $3::int AND s.order_date < $1::date
"""

_LINE_ITEMS_SQL = """
WITH li AS (
    SELECT order_id, SUM(price_sold * quantity) AS amount
    FROM bronze.order_products GROUP BY order_id
)
SELECT (s.sales_type = 'internal') AS internal,
       count(*) AS n,
       COALESCE(SUM(li.amount), 0) AS amount,
       (array_agg(s.id ORDER BY li.amount DESC, s.id DESC))[1:10] AS sample
FROM silver.orders s
JOIN li ON li.order_id = s.id
WHERE NOT s.is_return AND s.is_active_source
  AND s.grand_total = 0 AND li.amount > 0
GROUP BY 1
"""

# ── DN-23: landing itself. Each is DuckDB's statement over `orders` and
# `order_products`, read over `bronze.*`, with DuckDB's thresholds bound as
# parameters rather than copied.

# Orders billed at least $1 (`_orders_without_line_items_check`'s `min_total`)
# with no line item, split by whether the header was mirrored inside the settle
# grace ($2, minutes). The sample leads with the largest, as DuckDB's does.
_WITHOUT_ITEMS_SQL = """
WITH bare AS (
    SELECT o.id, o.grand_total,
           o.mirrored_at >= now() - make_interval(mins => $2) AS in_flight
    FROM bronze.orders o
    WHERE o.grand_total >= $1::numeric
      AND NOT EXISTS (SELECT 1 FROM bronze.order_products p WHERE p.order_id = o.id)
)
SELECT count(*) FILTER (WHERE NOT in_flight) AS n,
       count(*) FILTER (WHERE in_flight) AS in_flight,
       COALESCE(SUM(grand_total) FILTER (WHERE NOT in_flight), 0) AS amount,
       (array_agg(id ORDER BY grand_total DESC, id DESC) FILTER (WHERE NOT in_flight))[1:10] AS sample
FROM bare
"""

# Line items naming an order bronze holds no header for. Counted per line item
# and sampled by the order they name, as `_fk_orphan_check` counts and samples.
_ORPHAN_ITEMS_SQL = """
SELECT count(*) AS n,
       (array_agg(DISTINCT p.order_id ORDER BY p.order_id DESC))[1:10] AS sample
FROM bronze.order_products p
WHERE NOT EXISTS (SELECT 1 FROM bronze.orders o WHERE o.id = p.order_id)
"""

# One pass over the headers for the NULL and both domains. $1 the known status
# ids, $2 the known source ids. `source_id` and `status_id` are NOT NULL here
# (revision 0003), so DuckDB's NULL checks on them have no twin to need.
_HEADERS_SQL = """
SELECT count(*) FILTER (WHERE ordered_at IS NULL) AS null_ordered_at,
       (array_agg(id ORDER BY id DESC) FILTER (WHERE ordered_at IS NULL))[1:10] AS null_sample,
       count(*) FILTER (WHERE status_id <> ALL($1::int[])) AS unknown_status,
       array_agg(DISTINCT status_id ORDER BY status_id)
           FILTER (WHERE status_id <> ALL($1::int[])) AS status_values,
       (array_agg(id ORDER BY id DESC) FILTER (WHERE status_id <> ALL($1::int[])))[1:10] AS status_sample,
       count(*) FILTER (WHERE source_id <> ALL($2::int[])) AS unknown_source,
       array_agg(DISTINCT source_id ORDER BY source_id)
           FILTER (WHERE source_id <> ALL($2::int[])) AS source_values,
       (array_agg(id ORDER BY id DESC) FILTER (WHERE source_id <> ALL($2::int[])))[1:10] AS source_sample
FROM bronze.orders
"""

# KeyCRM's own group against the return list, per (status, group) pair that
# disagrees. $1 the lost/cancel group, $2 the listed return statuses. A NULL
# group is a row synced before the column existed and is not compared.
_STATUS_GROUP_SQL = """
SELECT status_id, status_group_id, count(*) AS n,
       COALESCE(SUM(grand_total), 0) AS amount
FROM bronze.orders
WHERE status_group_id IS NOT NULL
  AND (status_group_id = $1) <> (status_id = ANY($2::int[]))
GROUP BY status_id, status_group_id
ORDER BY amount DESC, status_id, status_group_id
"""


# The error-free runs of the lookback, led by the last one before it. $1 the
# layer, $2 the start of the lookback, $3 the high-water key. The cast is on
# the selected rows only: one malformed value from long ago must not blind the
# group for good.
_JOURNAL_SQL = """
SELECT id, trigger, started_at, requested_seen,
       (validation ->> $3::text)::timestamptz AS high_water
FROM meta.derivation_runs
WHERE layer = $1 AND error IS NULL
  AND id >= COALESCE((SELECT max(id) FROM meta.derivation_runs
                       WHERE layer = $1 AND error IS NULL AND started_at < $2), 0)
ORDER BY id
"""


def _row_values_sql() -> str:
    """Silver recomputed from bronze, compared row by row, judged in SQL.

    $1 the settle grace and $2 the repair window, in minutes; $3 the margin a
    change must clear to count as inside a rebuild's snapshot; $4 the
    derivation's layer; $5 Silver's row in `meta.mirror_state`; $6 when this
    process's first Silver rebuild committed (`pg_silver.FIRST_REBUILT_AT`,
    NULL before it); $7 when this process loaded the Silver rule
    (`pg_silver.RULE_LOADED_AT`). Rendered per call rather than at import:
    every import in this module is lazy, and this one would otherwise pull the
    DuckDB store in with it.

    **When Silver was last rebuilt** (`rebuilt_at`) is the later of two
    stamps. `rebuild_silver` writes Silver's watermark with `now()` inside its
    own transaction, so the stamp is that transaction's start: before pass 1
    takes its snapshot, and committed with the rows or not at all. That is the
    one record of a rebuild under **both** modes — `KS_PG_DERIVE=piggyback`,
    production's, rebuilds on DuckDB's tick and writes no journal row, and an
    own-mode run whose Gold or profile failed after Silver committed journals
    an error over a Silver that was rebuilt. The journal's last error-free
    `started_at` is the other stamp, the one the spec named; it precedes its
    own run's watermark, so where both exist the watermark decides.

    **Whether that rebuild ran this process's rule** (`rule_ran`) is
    `rebuilt_at >= $6`, with no margin: $6 is the watermark of this process's
    first rebuild, the same value by the same clock, and the rule cannot
    change inside a process. Before it the answer is no — the last rebuild ran
    the previous process's code, which a deploy may have changed.

    **When a row's inputs last changed** (`changed_at`) is the latest of:

    - its own `bronze.orders.mirrored_at` — every column but three reads only
      the order's own row;
    - the classification's `mirrored_at`, when `sales_type` is what moved: a
      reclassified manager changes the rows of orders nobody touched, and until
      the rebuild it owes, that is in flight rather than stale;
    - the buyer's latest change, when `is_new_customer` or
      `buyer_first_order_date` moved: pass 2 reads every order the buyer has.
      A buyer's clock runs over the orders bronze gives them *and* the orders
      Silver still gives them, so an order moved to another buyer is a recent
      change for the buyer it left — bronze alone no longer says it was theirs.
      (The moved order itself needs no such help: its own stamp is recent.)

    The rule is not among them. Dating rows by it put every differing row in
    flight after each restart — a standing page read as resolved, an abandoned
    row too — and the grace stops being absolute the moment a restart can move
    it.

    **The verdict.** A row whose inputs the last rebuild's snapshot held
    (`in_snapshot`) is *covered* when that rebuild ran this rule — CRITICAL,
    another rebuild would repeat it — and otherwise *held* while this process
    has had the rule for less than the repair window, *overdue* past it. A row
    the snapshot did not hold takes the absolute windows on `changed_at`: in
    flight, then waiting, then *abandoned*.
    """
    from core.data_quality import _SILVER_ROW_COLUMNS, _silver_row_differs
    from core.duckdb_store import silver_recompute_ctes
    from core.pg_replication import CLASSIFICATIONS_TABLE, MANAGERS_TABLE
    from core.sql_dialect import POSTGRES

    columns = [c for c, _ in _SILVER_ROW_COLUMNS]
    flags = ",\n           ".join(
        f"({_silver_row_differs(c, t)}) AS d_{c}" for c, t in _SILVER_ROW_COLUMNS)
    any_differs = " OR ".join(f"c.d_{c}" for c in columns)
    per_column = ",\n       ".join(
        f"count(*) FILTER (WHERE (reported OR held) AND d_{c}) AS n_{c}" for c in columns)
    orders, silver = POSTGRES.orders, POSTGRES.silver_orders
    return f"""
WITH {silver_recompute_ctes(POSTGRES)},
stamps AS (
    SELECT GREATEST((SELECT max(mirrored_at) FROM {MANAGERS_TABLE}),
                    (SELECT max(mirrored_at) FROM {CLASSIFICATIONS_TABLE})) AS classified_at,
           GREATEST((SELECT max(started_at) FROM meta.derivation_runs
                      WHERE layer = $4 AND error IS NULL),
                    (SELECT last_ok_at FROM meta.mirror_state
                      WHERE table_name = $5)) AS rebuilt_at
),
inputs AS (
    SELECT s.*,
           COALESCE(s.rebuilt_at >= $6::timestamptz, FALSE) AS rule_ran,
           COALESCE($7::timestamptz < now() - make_interval(mins => $2), TRUE) AS rule_overdue
    FROM stamps s
),
buyer_clock AS (
    SELECT buyer_id, max(mirrored_at) AS changed_at
    FROM (SELECT o.buyer_id, o.mirrored_at FROM {orders} o
           WHERE o.buyer_id IS NOT NULL
          UNION ALL
          SELECT s.buyer_id, o.mirrored_at FROM {silver} s
            JOIN {orders} o ON o.id = s.id
           WHERE s.buyer_id IS NOT NULL) b
    GROUP BY buyer_id
),
compared AS (
    SELECT r.id, o.mirrored_at, bc.changed_at AS buyer_changed_at,
           {flags}
    FROM recomputed r
    JOIN {silver} s ON s.id = r.id
    JOIN {orders} o ON o.id = r.id
    LEFT JOIN buyer_clock bc ON bc.buyer_id = r.buyer_id
),
stale AS (
    SELECT c.*,
           GREATEST(c.mirrored_at,
                    CASE WHEN c.d_sales_type THEN i.classified_at END,
                    CASE WHEN c.d_is_new_customer OR c.d_buyer_first_order_date
                         THEN c.buyer_changed_at END) AS changed_at,
           i.rebuilt_at, i.rule_ran, i.rule_overdue
    FROM compared c CROSS JOIN inputs i
    WHERE {any_differs}
),
judged AS (
    SELECT st.*,
           (st.rebuilt_at IS NOT NULL
            AND st.changed_at < st.rebuilt_at - $3::interval) AS in_snapshot,
           st.changed_at < now() - make_interval(mins => $2) AS past_repair,
           st.changed_at < now() - make_interval(mins => $1) AS settled
    FROM stale st
),
marked AS (
    SELECT j.*,
           (j.in_snapshot AND j.rule_ran) AS covered,
           (j.in_snapshot AND NOT j.rule_ran AND NOT j.rule_overdue) AS held,
           (j.in_snapshot AND NOT j.rule_ran AND j.rule_overdue) AS overdue,
           (NOT j.in_snapshot AND j.past_repair) AS abandoned,
           (NOT j.in_snapshot AND j.settled) AS waited
    FROM judged j
),
verdicts AS (
    SELECT m.*, (m.covered OR m.overdue OR m.waited) AS reported FROM marked m
)
SELECT (SELECT count(*) FROM compared) AS compared,
       (SELECT rebuilt_at FROM inputs) AS rebuilt_at,
       count(*) FILTER (WHERE reported) AS reported,
       count(*) FILTER (WHERE covered) AS covered,
       count(*) FILTER (WHERE abandoned) AS abandoned,
       count(*) FILTER (WHERE overdue) AS overdue,
       count(*) FILTER (WHERE held) AS held,
       count(*) FILTER (WHERE NOT reported AND NOT held) AS in_flight,
       EXTRACT(EPOCH FROM now() - MIN(changed_at) FILTER (WHERE reported))::int AS oldest_age_s,
       (array_agg(id ORDER BY (covered OR abandoned OR overdue) DESC, reported DESC, id DESC)
            FILTER (WHERE reported OR held))[1:10] AS sample,
       {per_column}
FROM verdicts
"""


def _ids(value) -> Tuple[int, ...]:
    return tuple(int(v) for v in (value or ()))


async def _read_silver_arc(conn, grace: int, page_after: int) -> SilverArc:
    m = await conn.fetchrow(_MISSING_SQL, grace, page_after)
    o = await conn.fetchrow(_ORPHAN_SQL)
    return SilverArc(
        missing=int(m["n"]), past_repair=int(m["past_repair"]),
        missing_amount=float(m["amount"]), oldest_age_s=m["oldest_age_s"],
        missing_sample=_ids(m["sample"]),
        orphans=int(o["n"]), orphan_amount=float(o["amount"]),
        orphan_sample=_ids(o["sample"]),
    )


async def _read_attribution(conn, watermark: Watermark) -> Union[Attribution, Unwatched]:
    from core.data_quality import _attribution_coverage_check
    import inspect

    if not watermark.backfilled:
        return Unwatched("bronze.orders history is not backfilled (meta.mirror_state.backfilled_at)")
    defaults = inspect.signature(_attribution_coverage_check).parameters
    window = defaults["window_days"].default
    span = window + defaults["baseline_days"].default
    row = await conn.fetchrow(_ATTRIBUTION_SQL, watermark.today, window, span,
                              WEBSITE_SOURCE_ID)
    return Attribution(int(row[0]), int(row[1]), int(row[2]), int(row[3]))


async def _read_line_items(conn, watermark: Watermark) -> Union[LineItems, Unwatched]:
    if not watermark.backfilled:
        return Unwatched("bronze.orders history is not backfilled (meta.mirror_state.backfilled_at)")
    by = {bool(r["internal"]): r for r in await conn.fetch(_LINE_ITEMS_SQL)}

    def part(internal):
        r = by.get(internal)
        if r is None:
            return 0, 0.0, ()
        return int(r["n"]), float(r["amount"]), _ids(r["sample"])

    h, g = part(False), part(True)
    return LineItems(h[0], h[1], h[2], g[0], g[1], g[2])


async def _read_journal(conn) -> DerivationJournal:
    """The derivation's journal for `pg_signal_missed`. Not gated on the
    backfill or the derivation mode: under piggyback nothing is journalled, and
    an empty journal judges nothing."""
    from core.pg_derivation import HIGH_WATER, LAYER

    since = await conn.fetchval("SELECT now() - make_interval(hours => $1)",
                                SIGNAL_LOOKBACK_H)
    rows = await conn.fetch(_JOURNAL_SQL, LAYER, since, HIGH_WATER)
    return DerivationJournal(since=since, runs=tuple(
        JournalRun(id=int(r["id"]), trigger=r["trigger"], started_at=r["started_at"],
                   requested_seen=r["requested_seen"], high_water=r["high_water"])
        for r in rows))


async def _read_order_landing(conn, watermark: Watermark, grace: int) -> Union[OrderLanding, Unwatched]:
    """DN-23's facts. Gated on the backfill, like the line items: before it
    bronze holds a delta, and every count set beside DuckDB's would differ by
    history not yet carried rather than by anything wrong.

    Every threshold is DuckDB's own, read from where DuckDB reads it and bound
    as a parameter: `min_total` from `_orders_without_line_items_check`'s
    signature, the known ids from `data_quality`, the return list and the
    lost/cancel group from `core.models`."""
    import inspect
    from decimal import Decimal

    from core.data_quality import (
        KNOWN_SOURCE_IDS, KNOWN_STATUS_IDS, _orders_without_line_items_check,
    )
    from core.models import LOST_STATUS_GROUP_ID, OrderStatus

    if not watermark.backfilled:
        return Unwatched("bronze.orders history is not backfilled (meta.mirror_state.backfilled_at)")
    min_total = inspect.signature(_orders_without_line_items_check).parameters["min_total"].default
    w = await conn.fetchrow(_WITHOUT_ITEMS_SQL, Decimal(str(min_total)), grace)
    o = await conn.fetchrow(_ORPHAN_ITEMS_SQL)
    h = await conn.fetchrow(_HEADERS_SQL, sorted(KNOWN_STATUS_IDS), sorted(KNOWN_SOURCE_IDS))
    groups = await conn.fetch(_STATUS_GROUP_SQL, LOST_STATUS_GROUP_ID,
                              sorted(int(s) for s in OrderStatus.return_statuses()))
    return OrderLanding(
        without_items=int(w["n"]), without_items_in_flight=int(w["in_flight"]),
        without_items_amount=float(w["amount"]), without_items_sample=_ids(w["sample"]),
        orphan_items=int(o["n"]), orphan_sample=_ids(o["sample"]),
        null_ordered_at=int(h["null_ordered_at"]), null_ordered_at_sample=_ids(h["null_sample"]),
        unknown_status=int(h["unknown_status"]), unknown_status_values=_ids(h["status_values"]),
        unknown_status_sample=_ids(h["status_sample"]),
        unknown_source=int(h["unknown_source"]), unknown_source_values=_ids(h["source_values"]),
        unknown_source_sample=_ids(h["source_sample"]),
        status_groups=tuple((int(r["status_id"]), int(r["status_group_id"]), int(r["n"]),
                             float(r["amount"])) for r in groups),
    )


async def _read_row_values(conn, grace: int, page_after: int) -> RowValues:
    """The recompute. Not gated on the backfill, unlike attribution and line
    items: Silver is derived from whatever bronze holds, history or not, so a
    partial bronze still has a Silver that must match it."""
    from core import pg_silver
    from core.data_quality import _SILVER_ROW_COLUMNS
    from core.pg_derivation import DROPPED_MARK_MARGIN, LAYER

    # The margin is DROPPED_MARK_MARGIN, for its two reasons: a writer outside
    # `_heavy_job_lock` commits a round trip after the instant its rows are
    # stamped with, and the journal's `started_at` comes from the web
    # container's clock where `mirrored_at` and the watermark come from
    # Postgres'. It does not apply to the first rebuild's stamp, which is
    # Postgres' own and the watermark's exact value.
    first_rebuilt_at, loaded_at = pg_silver.FIRST_REBUILT_AT, pg_silver.RULE_LOADED_AT
    started = time.monotonic()
    row = await conn.fetchrow(_row_values_sql(), grace, page_after,
                              DROPPED_MARK_MARGIN, LAYER, pg_silver.SILVER_TABLE,
                              first_rebuilt_at, loaded_at)
    elapsed_ms = int((time.monotonic() - started) * 1000)
    columns = tuple(sorted(
        ((c, int(row[f"n_{c}"])) for c, _ in _SILVER_ROW_COLUMNS if row[f"n_{c}"]),
        key=lambda kv: (-kv[1], kv[0])))
    facts = RowValues(
        reported=int(row["reported"]), covered=int(row["covered"]),
        abandoned=int(row["abandoned"]), in_flight=int(row["in_flight"]),
        columns=columns, sample=_ids(row["sample"]),
        oldest_age_s=row["oldest_age_s"], rebuilt_at=row["rebuilt_at"],
        compared=int(row["compared"]), elapsed_ms=elapsed_ms,
        held=int(row["held"]), overdue=int(row["overdue"]),
        first_rebuilt_at=first_rebuilt_at, rule_loaded_at=loaded_at)
    # INFO on every run: the cost is what decides whether this group can keep
    # sharing the snapshot's hold budget, and a number nobody logged is a
    # number nobody can compare with HOLD_BUDGET_S when Silver has grown.
    logger.info("Postgres Silver recomputed for the row-values twin: %d rows "
                "compared in %d ms (hold budget %d s)",
                facts.compared, elapsed_ms, HOLD_BUDGET_S)
    return facts


async def read_facts(*, today: Optional[date] = None, pool=None) -> Facts:
    """Every fact the twins judge, from one snapshot. Never raises: a failure is
    `Facts.blind(reason)` or an Unwatched group."""
    from core.mirror_reconciliation import configured
    from core.pg_silver import PG_LAYER_LOCK

    if not configured():
        return Facts.blind("KS_PG_DSN is not set")
    try:
        from core.pg import get_pool, require_revision

        pool = pool or await get_pool()
        await require_revision()
    except Exception as e:  # noqa: BLE001 — becomes the blindness reason
        return Facts.blind(f"{type(e).__name__}: {e}")

    try:
        grace, page_after = silver_grace_minutes(), page_after_minutes()
    except Exception as e:  # noqa: BLE001 — e.g. a KS_PG_SILVER_INTERVAL_S that is not a number
        return Facts.blind(f"the windows could not be computed: {type(e).__name__}: {e}")
    state: Dict[str, object] = {}
    groups: Dict[str, object] = {}

    # The wait and the hold are two limits, not one: waiting holds nothing, and
    # the hold is what stalls the sync behind the heavy lock.
    try:
        await asyncio.wait_for(PG_LAYER_LOCK.acquire(), timeout=LOCK_WAIT_S)
    except (asyncio.TimeoutError, TimeoutError):
        return Facts.blind(
            f"PG_LAYER_LOCK was not free within {LOCK_WAIT_S} s — a derivation "
            "or a ClickHouse ship held it")
    except Exception as e:  # noqa: BLE001 — never raises; CancelledError still propagates
        return Facts.blind(f"PG_LAYER_LOCK could not be taken: {type(e).__name__}: {e}")
    try:
        async with asyncio.timeout(HOLD_BUDGET_S):
            async with pool.acquire(timeout=HOLD_BUDGET_S) as conn:
                async with conn.transaction(isolation="repeatable_read", readonly=True):
                    await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
                    # No JIT. The row-values recompute is costed past
                    # jit_above_cost, and compiling it never paid for itself
                    # (a fresh backend per run, on the snapshot LOCK_WAIT_S
                    # describes): +0.11–0.12 s on a 0.17–0.20 s query over
                    # 48,000 orders (cost 208k–471k — Silver's estimates move
                    # until it is analysed), and past jit_optimize/
                    # inline_above_cost (500k) +0.87–0.91 s on 0.36–0.46 s
                    # over 96,000 (615k–1.36M). Time spent holding
                    # PG_LAYER_LOCK, to speed up a query that runs four times
                    # a day.
                    await conn.execute("SET LOCAL jit = off")
                    w = await conn.fetchrow(_WATERMARK_SQL, grace)
                    watermark = Watermark(
                        today=today or w["today"], in_flight=int(w["in_flight"]),
                        backfilled=bool(w["backfilled"]),
                        landing_age_s=w["landing_age_s"],
                        landing_failures=int(w["landing_failures"] or 0))
                    state["watermark"] = watermark
                    readers = (
                        ("silver_arc", lambda c: _read_silver_arc(c, grace, page_after)),
                        ("attribution", lambda c: _read_attribution(c, watermark)),
                        ("line_items", lambda c: _read_line_items(c, watermark)),
                        ("derivation_signal", _read_journal),
                        ("order_landing",
                         lambda c: _read_order_landing(c, watermark, grace)),
                        ("silver_row_values",
                         lambda c: _read_row_values(c, grace, page_after)),
                    )
                    for name, reader in readers:
                        try:
                            async with conn.transaction():   # a savepoint per group
                                groups[name] = await reader(conn)
                        except (asyncio.CancelledError, TimeoutError):
                            raise
                        except Exception as e:  # noqa: BLE001 — this group only
                            logger.error("Postgres twin group %s unreadable: %s", name, e)
                            groups[name] = Unwatched(f"{type(e).__name__}: {e}")
    except (asyncio.TimeoutError, TimeoutError):
        if "watermark" not in state:
            return Facts.blind(f"the {HOLD_BUDGET_S} s hold budget was spent before the snapshot opened")
        for name in GROUPS:
            groups.setdefault(name, Unwatched(
                f"the {HOLD_BUDGET_S} s hold budget was spent before this group was read"))
    except Exception as e:  # noqa: BLE001
        if "watermark" not in state:
            return Facts.blind(f"{type(e).__name__}: {e}")
        for name in GROUPS:
            groups.setdefault(name, Unwatched(f"{type(e).__name__}: {e}"))
    finally:
        PG_LAYER_LOCK.release()

    return Facts(whole=None, watermark=state["watermark"], **{
        name: groups.get(name, Unwatched("not read")) for name in GROUPS})


# ─── Judging ──────────────────────────────────────────────────────────────────


def _issue(**kw):
    from core.data_quality import IntegrityIssue

    return IntegrityIssue(**kw)


def _unwatched_issue(group: str, reason: str):
    from core.data_quality import Severity

    held = ", ".join(GUARD_CONDITIONS[group])
    return _issue(
        check_name=UNWATCHED_NAMES[group], table_name="(postgres twins)",
        severity=Severity.WARN, count=1,
        description=(f"The Postgres twin '{group}' could not look this run: {reason}. "
                     f"{held} are held as they were, not cleared."))


def flag_invalid_issue(error: str):
    from core.data_quality import Severity

    return _issue(
        check_name=FLAG_INVALID, table_name="(postgres twins)",
        severity=Severity.WARN, count=1,
        description=f"{error}. The Postgres twins did not run; their conditions are held, not cleared.")


def _silver_arc_check(arc: SilverArc) -> list:
    from core.data_quality import Severity

    issues = []
    grace, page_after = silver_grace_minutes(), page_after_minutes()
    if arc.missing:
        oldest = "" if arc.oldest_age_s is None else f" (oldest {arc.oldest_age_s // 60} min)"
        if arc.past_repair:
            severity = Severity.CRITICAL
            tail = (f" {arc.past_repair} of them are older than {page_after} minutes — past "
                    "the heartbeat and the floor, so no rebuild is coming on its own. Every "
                    "number read from Postgres is short by that much.")
        else:
            severity = Severity.WARN
            tail = (f" Still inside the {page_after}-minute repair window: under own a lost "
                    "mark is rebuilt by the hourly heartbeat, under piggyback by the next "
                    "DuckDB dirty tick.")
        issues.append(_issue(
            check_name="pg_silver_missing_rows", table_name="silver.orders",
            severity=severity, count=arc.missing, sample_ids=arc.missing_sample,
            description=(f"{arc.missing} order(s) worth {arc.missing_amount:,.2f} are in "
                         f"bronze.orders with no silver.orders row {grace}+ minutes after they "
                         f"were mirrored{oldest}." + tail)))
    if arc.orphans:
        issues.append(_issue(
            check_name="pg_silver_orphan_rows", table_name="silver.orders",
            severity=Severity.WARN, count=arc.orphans, sample_ids=arc.orphan_sample,
            description=(f"{arc.orphans} silver.orders row(s) worth {arc.orphan_amount:,.2f} "
                         "have no order behind them in bronze.orders. They inflate every "
                         "number read from Postgres. No grace applies: Silver is only "
                         "written from landing.")))
    return issues


def _row_values_check(rv: RowValues) -> list:
    """One finding: CRITICAL when any row is covered, abandoned or overdue, WARN
    when rows wait inside the repair window, INFO when rows are only held.

    Covered and abandoned are told apart in the words because their levers
    differ: an abandoned row is a derivation that stopped, and a rebuild fixes
    it; a covered row is a rebuild that ran and still did not produce what
    bronze holds, and another rebuild would only repeat it. A held row is
    neither, yet — the rebuild that had it ran before this process, so whether
    it is at fault or a rebuild is owed waits for this process's first — and
    `check_pg_warehouse` holds the condition for it rather than clearing it.
    """
    from core.data_quality import Severity

    total = rv.reported + rv.held
    if not total:
        return []
    grace, page_after = silver_grace_minutes(), page_after_minutes()
    critical = rv.covered + rv.abandoned + rv.overdue
    waiting = rv.reported - critical
    columns = ", ".join(f"{c} ({n})" for c, n in rv.columns[:5]) or "none named"
    oldest = "" if rv.oldest_age_s is None else f" The oldest changed {rv.oldest_age_s // 60} min ago."
    since = "?" if rv.rebuilt_at is None else rv.rebuilt_at.isoformat(timespec="seconds")
    loaded = "?" if rv.rule_loaded_at is None else rv.rule_loaded_at.isoformat(timespec="seconds")
    parts = [f"{total} silver.orders row(s) hold values a recompute from "
             f"bronze.orders does not produce. Columns: {columns}.{oldest}"]
    if rv.covered:
        parts.append(f"{rv.covered} changed before the last Silver rebuild began "
                     f"({since}), so its snapshot held them: the rebuild is not producing "
                     "what bronze holds, and another would repeat it.")
    if rv.abandoned:
        parts.append(f"{rv.abandoned} changed more than {page_after} minutes ago with no "
                     "Silver rebuild begun since — past the heartbeat and the floor, so "
                     "no rebuild is coming on its own.")
    if rv.overdue:
        parts.append(f"{rv.overdue} changed before the last Silver rebuild began ({since}), "
                     "but it ran before this process loaded the Silver rule "
                     f"({loaded}), more than {page_after} minutes ago, and no rebuild has "
                     "run under that rule since — past the heartbeat and the floor, so "
                     "none is coming on its own.")
    if waiting:
        parts.append(f"{waiting} changed {grace}+ minutes ago and are inside the "
                     f"{page_after}-minute repair window: under own the next mark or the "
                     "hourly heartbeat rebuilds them, under piggyback the next DuckDB "
                     "dirty tick.")
    if rv.held:
        parts.append(f"{rv.held} changed before the last Silver rebuild began ({since}), "
                     "but it ran before this process loaded the Silver rule "
                     f"({loaded}) and may have run another one: whether that rebuild is at "
                     "fault or one is owed is not known until this process's first. Held — "
                     "not judged, and a page standing for them stays standing.")
    if rv.in_flight:
        parts.append(f"{rv.in_flight} more changed in the last {grace} minutes and are "
                     "in flight, not counted.")
    severity = (Severity.CRITICAL if critical
                else Severity.WARN if rv.reported else Severity.INFO)
    return [_issue(
        check_name="pg_silver_row_values", table_name="silver.orders",
        severity=severity, count=total, sample_ids=rv.sample,
        description=" ".join(parts))]


def signal_missed_pairs(journal: DerivationJournal) -> List[Tuple[JournalRun, JournalRun]]:
    """Consecutive error-free runs, `(before, run)`, where bronze moved on and
    the mark did not: `run` started inside the lookback, its high-water mark is
    later than `before`'s, and its `requested_seen` is the same.

    Not judged: a run whose trigger is `first_tick`. Before DN-05b a process
    configured its modes after the boot sync, so that sync's writes carried no
    mark by design and this run is what rebuilt them. Since DN-05b a boot sync
    that wrote anything marked it, so a process's first run is `signal` and is
    judged; the exclusion now matters for journal rows of processes started
    before it. Its cost: an unmarked write landing just before a first-tick
    run is never judged — every write after that run is. A `signal` run needs
    no exclusion — `due` says `signal` only when `requested` passed `built`, which
    the previous run left at its own `requested_seen`, so its mark has moved by
    construction. Nor is a pair either of whose runs has no high-water mark or
    no `requested_seen`: a run journalled before DN-14 says nothing either way.
    """
    from core.pg_derivation import FIRST_TICK

    pairs = []
    for before, run in zip(journal.runs, journal.runs[1:]):
        if run.started_at < journal.since or run.trigger == FIRST_TICK:
            continue
        if None in (before.high_water, run.high_water,
                    before.requested_seen, run.requested_seen):
            continue
        if run.high_water > before.high_water and run.requested_seen == before.requested_seen:
            pairs.append((before, run))
    return pairs


def _signal_check(journal: DerivationJournal) -> list:
    """WARN `pg_signal_missed`, one finding for the lookback. Never CRITICAL:
    the run that saw the write rebuilt it, so what failed is the signal, and
    the heartbeat caps what it costs at an hour of lag."""
    from core.data_quality import Severity

    missed = signal_missed_pairs(journal)
    if not missed:
        return []
    before, run = missed[-1]
    return [_issue(
        check_name="pg_signal_missed", table_name="meta.derivation_runs",
        severity=Severity.WARN, count=len(missed),
        sample_ids=tuple(r.id for _, r in reversed(missed))[:10],
        description=(
            f"{len(missed)} Postgres derivation run(s) in the last {SIGNAL_LOOKBACK_H} h found "
            "bronze.orders written since the run before them, with no derivation mark raised "
            f"in between. The latest: run {run.id} ({run.trigger}, started "
            f"{run.started_at.isoformat(timespec='seconds')}) saw the latest mirrored_at move "
            f"from {before.high_water.isoformat(timespec='seconds')} to "
            f"{run.high_water.isoformat(timespec='seconds')} with requested still at "
            f"{run.requested_seen}. That run rebuilt the rows, so nothing is lost; the signal "
            "did not fire for them, and while it does not, Silver waits up to the hourly "
            "heartbeat. A mark dropped on its lock timeout reads the same way: see "
            "meta.mirror_state for meta.derivation_signal. Sample: meta.derivation_runs ids."))]


def _attribution_check(att: Attribution, watermark: Watermark) -> list:
    """The DuckDB check's verdict over Postgres' counts — its thresholds read
    from its own signature, so the two cannot drift apart."""
    import inspect

    from core.data_quality import Severity, _attribution_coverage_check

    d = {k: p.default for k, p in inspect.signature(_attribution_coverage_check).parameters.items()}
    if att.orders < d["min_orders"]:
        landing_bad = (watermark.landing_failures > 0 or watermark.landing_age_s is None
                       or watermark.landing_age_s > LANDING_MAX_AGE_S)
        if landing_bad:
            age = "never" if watermark.landing_age_s is None else f"{watermark.landing_age_s // 3600} h ago"
            return [_unwatched_issue(
                "attribution",
                f"only {att.orders} website order(s) in {d['window_days']} days, and the orders "
                f"mirror last shipped {age} with {watermark.landing_failures} failure(s) since — "
                "a quiet week cannot be told from a mirror that stopped")]
        return []   # a quiet week is not an outage — the DuckDB rule

    pct = att.tagged / att.orders * 100.0
    base_pct = (att.base_tagged / att.base_orders * 100.0
                if att.base_orders >= d["min_orders"] else None)
    below = pct < d["floor_pct"]
    halved = base_pct is not None and base_pct > 0 and pct < base_pct * d["halved_against_baseline"]
    if not (below or halved):
        return []
    trend = "" if base_pct is None else f" Over the previous {d['baseline_days']} days it was {base_pct:.0f}%."
    return [_issue(
        check_name="pg_attribution_coverage_website", table_name="silver.order_utm",
        severity=Severity.WARN, count=att.orders - att.tagged,
        description=(f"In Postgres, {pct:.0f}% of website orders in the last "
                     f"{d['window_days']} days carry a campaign tag ({att.tagged} of "
                     f"{att.orders})." + trend + " Orders are arriving; their UTM parameters "
                     "are not."))]


def _line_items_check(li: LineItems, watermark: Watermark, *,
                      duckdb_issues: Sequence, duckdb_looked: FrozenSet[str]) -> list:
    from core.data_quality import Severity

    counts = {i.check_name: i.count for i in duckdb_issues}
    issues, differ = [], []

    def compare(duck_name: str, n: int) -> bool:
        """True when DuckDB looked, so the twin compares instead of standing in."""
        if duck_name not in duckdb_looked:
            return False
        duck = counts.get(duck_name, 0)
        if abs(n - duck) > watermark.in_flight:
            differ.append(f"{duck_name}: Postgres {n}, DuckDB {duck}")
        return True

    if not compare("headline_vs_line_items", li.headline) and li.headline:
        issues.append(_issue(
            check_name="pg_headline_vs_line_items", table_name="silver.orders",
            severity=Severity.WARN, count=li.headline, sample_ids=li.headline_sample,
            description=(f"In Postgres, {li.headline} order(s) have grand_total = 0 but "
                         f"{li.headline_amount:,.2f} in line items. Revenue figures read "
                         "grand_total and product/brand/category figures read line items, "
                         "so the two disagree by this amount.")))
    if not compare("goods_shipped_without_sale", li.goods) and li.goods:
        issues.append(_issue(
            check_name="pg_goods_shipped_without_sale", table_name="silver.orders",
            severity=Severity.INFO, count=li.goods, sample_ids=li.goods_sample,
            description=(f"In Postgres, {li.goods} internal order(s) shipped "
                         f"{li.goods_amount:,.2f} of goods with no sale attached — seeding "
                         "and gifts. Expected, and counted so it is a number rather than "
                         "a silence.")))
    if differ:
        issues.append(_issue(
            check_name="pg_line_items_disagree", table_name="silver.orders",
            severity=Severity.WARN, count=len(differ),
            description=("The engines count line-item findings differently by more than the "
                         f"{watermark.in_flight} order(s) still in flight: " + "; ".join(differ)
                         + ". One store's Silver or line items differ from the other's.")))
    return issues


def landing_counts(ol: OrderLanding) -> Dict[str, int]:
    """Postgres' count for each DuckDB finding the landing twins pair with,
    keyed by DuckDB's name and counted as DuckDB counts it — every order, in
    flight or not, because DuckDB's check has no grace. What the engines are
    compared on, and what the pairing record carries."""
    return {
        "orders_without_line_items": ol.without_items + ol.without_items_in_flight,
        "fk_orphan_order_products_order_id": ol.orphan_items,
        "not_null_orders_ordered_at": ol.null_ordered_at,
        "value_domain_orders_status_id": ol.unknown_status,
        "value_domain_orders_source_id": ol.unknown_source,
        "status_group_vs_return_list": sum(n for _s, _g, n, _a in ol.status_groups),
    }


def _landing_findings(ol: OrderLanding) -> Dict[str, Any]:
    """What each landing twin files standing in for DuckDB, by condition; a
    twin with nothing to report is absent. DuckDB's severities: a line item
    with no order and an order with no date are CRITICAL there, the rest WARN.
    Only the settled orders without line items are filed — those in flight are
    named in the description and counted nowhere."""
    from core.data_quality import KNOWN_SOURCE_IDS, KNOWN_STATUS_IDS, Severity
    from core.models import OrderStatus

    grace = silver_grace_minutes()
    out: Dict[str, Any] = {}
    if ol.without_items:
        flight = (f" {ol.without_items_in_flight} more were mirrored in the last {grace} "
                  "minutes and are in flight, not counted." if ol.without_items_in_flight else "")
        out["pg_orders_without_line_items"] = _issue(
            check_name="pg_orders_without_line_items", table_name="bronze.order_products",
            severity=Severity.WARN, count=ol.without_items, sample_ids=ol.without_items_sample,
            description=(f"In Postgres, {ol.without_items} order(s) worth "
                         f"{ol.without_items_amount:,.2f} have no line items, their headers "
                         f"mirrored {grace}+ minutes ago. Revenue counts them, product/brand/"
                         "category breakdowns cannot." + flight))
    if ol.orphan_items:
        out["pg_fk_orphan_order_products_order_id"] = _issue(
            check_name="pg_fk_orphan_order_products_order_id",
            table_name="bronze.order_products", severity=Severity.CRITICAL,
            count=ol.orphan_items, sample_ids=ol.orphan_sample,
            description=(f"In Postgres, {ol.orphan_items} bronze.order_products row(s) name an "
                         "order bronze.orders does not hold (the sample is those orders' ids). "
                         "No grace applies: an order's header and line items land in one "
                         "transaction."))
    if ol.null_ordered_at:
        out["pg_not_null_orders_ordered_at"] = _issue(
            check_name="pg_not_null_orders_ordered_at", table_name="bronze.orders",
            severity=Severity.CRITICAL, count=ol.null_ordered_at,
            sample_ids=ol.null_ordered_at_sample,
            description=(f"In Postgres, {ol.null_ordered_at} bronze.orders row(s) have no "
                         "ordered_at. Every figure bound to a date leaves them out."))

    def domain(column: str, known: FrozenSet[int], n: int, values: Tuple[int, ...]) -> str:
        known_list = ", ".join(str(v) for v in sorted(known))
        seen = ", ".join(str(v) for v in values)[:200]
        return (f"In Postgres, {n} row(s) in bronze.orders have {column} not in known set "
                f"{{{known_list}}}. Unknown values seen: {seen}")

    if ol.unknown_status:
        out["pg_value_domain_orders_status_id"] = _issue(
            check_name="pg_value_domain_orders_status_id", table_name="bronze.orders",
            severity=Severity.WARN, count=ol.unknown_status, sample_ids=ol.unknown_status_sample,
            description=domain("status_id", KNOWN_STATUS_IDS, ol.unknown_status,
                               ol.unknown_status_values)
            + ". A status KeyCRM added counts toward revenue by its group, not by this list.")
    if ol.unknown_source:
        out["pg_value_domain_orders_source_id"] = _issue(
            check_name="pg_value_domain_orders_source_id", table_name="bronze.orders",
            severity=Severity.WARN, count=ol.unknown_source, sample_ids=ol.unknown_source_sample,
            description=domain("source_id", KNOWN_SOURCE_IDS, ol.unknown_source,
                               ol.unknown_source_values) + ".")
    if ol.status_groups:
        listed = {int(s) for s in OrderStatus.return_statuses()}
        total = sum(n for _s, _g, n, _a in ol.status_groups)
        amount = sum(a for _s, _g, _n, a in ol.status_groups)
        detail = "; ".join(
            f"status {s} is KeyCRM group {g} but our list says "
            f"{'excluded' if s in listed else 'revenue'} ({n} orders, {a:,.2f})"
            for s, g, n, a in ol.status_groups[:5])
        out["pg_status_group_vs_return_list"] = _issue(
            check_name="pg_status_group_vs_return_list", table_name="bronze.orders",
            severity=Severity.WARN, count=total,
            sample_ids=tuple(s for s, _g, _n, _a in ol.status_groups[:10]),
            description=(f"In Postgres, {total} order(s) worth {amount:,.2f} are classified "
                         "differently by KeyCRM's status group and by our status-id list. "
                         f"{detail}. The group is the source's own answer; the list is our "
                         "copy of it."))
    return out


def _order_landing_check(ol: OrderLanding, watermark: Watermark, *,
                         duckdb_issues: Sequence, duckdb_looked: FrozenSet[str],
                         held_out: Optional[List[str]] = None) -> list:
    """The landing twins: per DuckDB guard, compare where it looked and stand
    in where it did not — the line-item rule.

    Comparing, a twin files nothing of its own, and the engines disagree when
    their counts differ by more than the orders in flight. A twin that would
    page standing alone goes to `held_out`: the rows are still there — DuckDB
    files them under its own name this run, or the engines disagree — and a
    `pg_` page delivered on a run DuckDB could not look must not be announced
    resolved while Postgres still holds them. Only a CRITICAL is held, since
    only a CRITICAL is ever delivered to be resolved.
    """
    from core.data_quality import Severity

    duck: Dict[str, int] = {}
    for issue in duckdb_issues:
        duck[issue.check_name] = duck.get(issue.check_name, 0) + int(issue.count)
    pg, alone = landing_counts(ol), _landing_findings(ol)
    issues, differ = [], []
    for guard, name, twin in LANDING_TWINS:
        if guard not in duckdb_looked:
            if twin in alone:
                issues.append(alone[twin])
            continue
        if abs(pg[name] - duck.get(name, 0)) > watermark.in_flight:
            differ.append(f"{name}: Postgres {pg[name]}, DuckDB {duck.get(name, 0)}")
        if (twin in alone and alone[twin].severity == Severity.CRITICAL
                and held_out is not None):
            held_out.append(twin)
    if differ:
        issues.append(_issue(
            check_name="pg_order_landing_disagree", table_name="bronze.orders",
            severity=Severity.WARN, count=len(differ),
            description=("The engines' landing checks count differently by more than the "
                         f"{watermark.in_flight} order(s) still in flight: " + "; ".join(differ)
                         + ". One store's orders or line items differ from the other's.")))
    return issues


def check_pg_warehouse(
    facts: Optional[Facts], *, duckdb_issues: Sequence = (),
    duckdb_looked: FrozenSet[str] = frozenset(),
    raised_out: Optional[List[str]] = None,
    held_out: Optional[List[str]] = None,
) -> list:
    """The twins over pre-read facts. Pure; never raises.

    `duckdb_looked` are the DuckDB guard names that ran this scan (empty when
    the DuckDB half failed), `DUCKDB_BARE_CHECKS` among them when it finished.
    A twin whose raise is caught files its group's unwatched finding, and its
    guard name goes to `raised_out`. `held_out` receives the conditions of
    every group that could not look; of the row-values group when it looked
    but held rows it could not judge; and of each landing twin that compared
    while Postgres still holds what it would page on — only those: the collapsed
    finding is one line, but a group read clean in the same run still
    announces its recovery.
    """
    from core.data_quality import Severity

    def hold(group: str) -> None:
        if held_out is not None:
            held_out.extend(GUARD_CONDITIONS[group])

    if facts is None or facts.whole is not None:
        reason = "facts were not read" if facts is None else facts.whole.reason
        for group in GROUPS:
            hold(group)
        return [_issue(
            check_name=WHOLE_UNWATCHED, table_name="(postgres twins)",
            severity=Severity.WARN, count=len(GROUPS),
            description=(f"The Postgres twins could not look this run: {reason}. Every pg_* "
                         "condition is held, not cleared."))]

    blind = {g: getattr(facts, g) for g in GROUPS if isinstance(getattr(facts, g), Unwatched)}
    for group in blind:
        hold(group)
    issues = []
    if len(blind) >= 2:
        reasons = "; ".join(f"{g}: {u.reason}" for g, u in blind.items())
        issues.append(_issue(
            check_name=WHOLE_UNWATCHED, table_name="(postgres twins)",
            severity=Severity.WARN, count=len(blind),
            description=(f"The Postgres twins could not look at {', '.join(blind)} this run — "
                         f"{reasons}. Every pg_* condition is held, not cleared.")))
    elif blind:
        (g, u), = blind.items()
        issues.append(_unwatched_issue(g, u.reason))

    landing_held: List[str] = []
    judges = {
        "silver_arc": lambda: _silver_arc_check(facts.silver_arc),
        "attribution": lambda: _attribution_check(facts.attribution, facts.watermark),
        "line_items": lambda: _line_items_check(
            facts.line_items, facts.watermark,
            duckdb_issues=duckdb_issues, duckdb_looked=duckdb_looked),
        "derivation_signal": lambda: _signal_check(facts.derivation_signal),
        "order_landing": lambda: _order_landing_check(
            facts.order_landing, facts.watermark, duckdb_issues=duckdb_issues,
            duckdb_looked=duckdb_looked, held_out=landing_held),
        "silver_row_values": lambda: _row_values_check(facts.silver_row_values),
    }
    for group, judge in judges.items():
        if group in blind:
            continue
        try:
            issues += judge()
        except Exception as exc:  # noqa: BLE001 — named, and its conditions held
            logger.error("Postgres twin %s raised: %s: %s", group, type(exc).__name__, exc)
            if raised_out is not None:
                raised_out.append(group)
            hold(group)
            issues.append(_unwatched_issue(group, f"the check raised {type(exc).__name__}: {exc}"))
            continue
        # Rows a previous process's rebuild had, and no rebuild of this one
        # since: the group looked, but could not judge them, so a page standing
        # for them is held rather than announced resolved after a restart.
        if group == "silver_row_values" and facts.silver_row_values.held:
            hold(group)
        # Landing twins that compared while Postgres still holds their rows:
        # DuckDB's name carries the finding this run, not a recovery of theirs.
        if group == "order_landing" and held_out is not None:
            held_out.extend(landing_held)
    return issues


# ─── The pairing record (DN-14) ───────────────────────────────────────────────
#
# What each engine measured, keyed by DuckDB's finding name so the two sides
# read across: `(DuckDB's guard, DuckDB's finding)`. A DuckDB number is its
# finding's count when the guard looked — nothing filed is 0 — and None when it
# did not look, which is not the same as 0. The landing twins' (DN-23) follow,
# their Postgres numbers counted as DuckDB counts them (`landing_counts`).
_PAIRED_DUCKDB = (
    ("silver_arc", "silver_missing_rows"),
    ("silver_arc", "silver_orphan_rows"),
    ("silver_arc", "silver_row_values"),
    ("headline_vs_line_items", "headline_vs_line_items"),
    ("goods_shipped_without_sale", "goods_shipped_without_sale"),
) + tuple((guard, name) for guard, name, _twin in LANDING_TWINS)
_DUCKDB_ATTRIBUTION_GUARD = "attribution_coverage"


def _coverage(counts: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """Orders, tagged and the share, for the window and the baseline before it.
    A share over no orders is None, never 0 %."""
    if not counts or counts.get("orders") is None:
        return None

    def pct(orders, tagged):
        return None if not orders or tagged is None else round(tagged / orders * 100.0, 2)

    orders, tagged = counts.get("orders"), counts.get("tagged")
    base_orders, base_tagged = counts.get("base_orders"), counts.get("base_tagged")
    return {"orders": orders, "tagged": tagged, "pct": pct(orders, tagged),
            "base_orders": base_orders, "base_tagged": base_tagged,
            "base_pct": pct(base_orders, base_tagged)}


def _pairing(facts: Optional[Facts], duckdb_issues: Sequence,
             duckdb_looked: FrozenSet[str],
             duckdb_attribution: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    for issue in duckdb_issues:
        counts[issue.check_name] = counts.get(issue.check_name, 0) + int(issue.count)
    duckdb: Dict[str, Any] = {"looked": sorted(duckdb_looked)}
    for guard, name in _PAIRED_DUCKDB:
        duckdb[name] = counts.get(name, 0) if guard in duckdb_looked else None
    duckdb["attribution"] = (_coverage(duckdb_attribution)
                             if _DUCKDB_ATTRIBUTION_GUARD in duckdb_looked else None)

    postgres: Dict[str, Any] = {name: None for _, name in _PAIRED_DUCKDB}
    postgres["attribution"] = None
    unwatched: Dict[str, str] = {}
    in_flight = None
    if facts is None or facts.whole is not None:
        unwatched["*"] = "facts were not read" if facts is None else facts.whole.reason
    else:
        in_flight = facts.watermark.in_flight
        for group in GROUPS:
            value = getattr(facts, group)
            if isinstance(value, Unwatched):
                unwatched[group] = value.reason
        arc, li, rv, att = (facts.silver_arc, facts.line_items, facts.silver_row_values,
                            facts.attribution)
        if isinstance(facts.order_landing, OrderLanding):
            postgres.update(landing_counts(facts.order_landing))
        if isinstance(arc, SilverArc):
            postgres["silver_missing_rows"] = arc.missing
            postgres["silver_orphan_rows"] = arc.orphans
        if isinstance(rv, RowValues):
            postgres["silver_row_values"] = rv.reported + rv.held
        if isinstance(li, LineItems):
            postgres["headline_vs_line_items"] = li.headline
            postgres["goods_shipped_without_sale"] = li.goods
        if isinstance(att, Attribution):
            postgres["attribution"] = _coverage(
                {"orders": att.orders, "tagged": att.tagged,
                 "base_orders": att.base_orders, "base_tagged": att.base_tagged})
    postgres["unwatched"] = unwatched

    # The shadow: the same facts judged as if DuckDB had not looked at all,
    # which is how the twins will run once it stops deriving (step 13). Pure,
    # so it reads nothing; its findings are recorded here and filed nowhere.
    standalone = {issue.check_name: issue.count for issue in check_pg_warehouse(
        facts, duckdb_issues=(), duckdb_looked=frozenset())}
    return {"version": PAIRING_VERSION, "in_flight": in_flight,
            "duckdb": duckdb, "postgres": postgres, "standalone": standalone}


def pairing_record(
    facts: Optional[Facts], *, duckdb_issues: Sequence = (),
    duckdb_looked: FrozenSet[str] = frozenset(),
    duckdb_attribution: Optional[Mapping[str, Any]] = None,
):
    """INFO `pg_twin_pairing`: both engines' numbers for one integrity run,
    as JSON in the description, for the soak to parse. Never raises.

    - `duckdb` and `postgres` carry the arc counts, attribution coverage,
      line-item counts and (since version 2) the six order-landing counts each
      engine measured, under DuckDB's names. A DuckDB guard that did not look
      is None, and a Postgres group that could not look is None with its
      reason under `postgres.unwatched`.
    - `standalone` is what `check_pg_warehouse` files for these facts with
      `duckdb_looked` empty — names and counts. While DuckDB looks, the
      line-item and landing twins compare and file nothing of their own, so
      this is the only record that `pg_headline_vs_line_items`,
      `pg_orders_without_line_items` and the rest would find what DuckDB
      finds. Its `pg_orders_without_line_items` leaves the orders in flight
      out, where `postgres.orders_without_line_items` counts them as DuckDB
      does.
    - `in_flight` is the tolerance the soak compares counts within.

    One INFO row a run: it never pages, and a digest shows it as one line.
    `duckdb_attribution` is the counts `_attribution_coverage_check` measured
    (`check_internal_integrity(pairing_out=)`).
    """
    from core.data_quality import Severity

    try:
        body = _pairing(facts, duckdb_issues, duckdb_looked, duckdb_attribution)
    except Exception as exc:  # noqa: BLE001 — a record that could not be built says so
        logger.error("Postgres twin pairing record not built: %s: %s", type(exc).__name__, exc)
        body = {"version": PAIRING_VERSION, "error": f"{type(exc).__name__}: {exc}"}
    return _issue(
        check_name="pg_twin_pairing", table_name="(postgres twins)",
        severity=Severity.INFO, count=1,
        description=json.dumps(body, sort_keys=True, default=str))


def unverified_pg_conditions(*, ran: bool, flag_invalid: bool, held: Sequence[str] = ()) -> List[str]:
    """What the twins' part of the run could not verify.

    - **Ran:** exactly the conditions of the groups that could not look (`held`,
      from `check_pg_warehouse(held_out=)`).
    - **Flag not understood:** everything — the twins were meant to run and did
      not, so nothing about them is known.
    - **Flag off:** nothing. Switched off on purpose is stood down, not blind: a
      page delivered while they were on resolves once rather than standing for
      as long as the flag stays off. Found reviewing #215.
    """
    if flag_invalid:
        return sorted(all_conditions())
    if not ran:
        return []
    return sorted(set(held))
