"""Replicating the seven tables that have no source to be rebuilt from — and
two that do.

`core/pg_landing.py` mirrors a KeyCRM payload: parsed once, written twice,
recoverable from KeyCRM if Postgres is ever lost. `core/pg_replication.py`
copies the manager classification, which a human decided and KeyCRM never knew.
This module is the second kind, seven times over — see revision 0008 for what
each table knows that its source does not; 0017 and 0018 added the two a human
types, `revenue_goals` and `manual_expenses`.

WHY REPLICATED AND NOT MIRRORED, IN ONE SENTENCE EACH

`stock_movements` is a delta against the *previous* contents of `offer_stocks`;
a second computation on this side would need its own previous state and would
diverge the first time either side missed a sync. `sku_inventory_status`
carries `first_seen_at` forward out of its own previous contents.
`inventory_sku_history` and `inventory_history` are snapshots of a moment that
has passed. `order_backfill_misses` records what an API call did *not* return,
which no API call can be made to return. `revenue_goals` and `manual_expenses`
are both numbers a human typed into a form — a target and an ad spend — and
KeyCRM has never heard of either.

TWO SHAPES, CHOSEN BY HOW DUCKDB WRITES EACH ONE

**Full replace** — `offer_stocks` (892 rows), `revenue_goals` (3),
`order_backfill_misses` (43), `inventory_history` (188) and
`sku_inventory_status` (887). Small, and the last is a `DELETE`+`INSERT` on
the DuckDB side too, so replacing it whole is the same operation rather than a
decision. A full replace also removes the "lost or retired" question the
catalogue mirror needed a watermark rule to answer: the writer wrote every row
it holds, so anything missing was lost.

**Append above a watermark** — `inventory_sku_history` (143,274 rows) and
`stock_movements` (50,386). Both are append-only: no `UPDATE` and no `DELETE`
against either exists anywhere in this repository, which was checked rather
than assumed. Rewriting 193,000 rows every hour to gain the ~900 that changed
would be waste with a straight face.

**Neither keeps a cursor.** The watermark is `MAX(date)` and `MAX(id)` read
back out of Postgres at the start of each run — the same choice
`core/pg_backfill.py` made and for the same reason: there is no stored position
to be wrong, and an interrupted run is resumed by recomputing rather than by
trusting a number somebody wrote down.

`inventory_sku_history` asks for `date >= MAX(date)`, not `>`. A day's ~887
rows go in one statement, so a partial day should not be possible — but `>=`
plus an upsert costs one re-shipped day and makes it not matter, where `>`
would leave a hole nothing would ever fill.

`stock_movements` asks for `id > MAX(id)` because its rows go in one
transaction ordered by id, so what Postgres holds is always a complete prefix.

AND THE HOLE A WATERMARK CANNOT SEE

A row lost from Postgres *below* the watermark is invisible to both of those
queries forever — `id > MAX(id)` steps over it and `date >= MAX(date)` never
looks back. That is not hypothetical; it is what a watermark means. The daily
comparison finds such a row (proved by deleting one: `mirror_missing_rows`,
CRITICAL, with the id), and `full=True` is what puts it back: watermarks
ignored, everything shipped, both append tables upserted rather than inserted
so a row that is present and *wrong* is corrected too.

Not automatic, deliberately. Reconciliation A reports and does not repair,
because a check that fixes what it finds destroys the evidence it found
anything — so the repair is a separate act by somebody who has read the
finding, exactly like `POST /api/mirror/backfill/orders`. Full costs ~7 s
against the production catalogue; incremental costs ~120 ms.

THE SIXTH TABLE, WHICH IS NOT LIKE THE OTHER FIVE

`bronze.offer_stocks` is landing data: written straight from KeyCRM's
offers/stocks payload, and re-fetchable if Postgres ever lost it. By the
argument above it belongs in `core/pg_landing.py`, not here.

It rides here because of where it was, and why that stopped working. It shipped
inside `replicate_sms` — sensibly, since `ltv_basis=margin` needs
`purchased_price` and the SMS tab was the only Postgres reader of it. Then
`KS_SMS_STORE=postgres` made DuckDB no longer the writer of the SMS state, and
`replicate_sms` correctly stood down **as a whole**: a full replace out of a
frozen DuckDB would roll back every opt-out recorded since the switch. The five
SMS tables wanted exactly that. `offer_stocks` did not — DuckDB still receives
it from KeyCRM every sync — so it silently stopped moving, at 08:00:53 on
2026-09-06, and `reconcile_sms` stands down on the same flag, so nothing would
ever have reported the drift. The tab kept computing margin from a snapshot
that was hours old and would have been months old, with new SKUs missing
entirely and their COGS therefore zero.

Here it cannot be caught by that class of switch again: this module has no
writer-side flag to stand down on, and `stock_movements` — a delta against the
*previous* contents of `offer_stocks` — is already computed on this tick, so
the two now travel together rather than on schedules that can diverge.

FAILURE POLICY

Never raises. This is called from the inventory sync and from the repair jobs,
and neither may be broken by a Postgres fault — charter rule 8, and the same
policy `pg_landing` and `pg_replication` already have. A run that fails does
not move a watermark, and it writes the failure into the watermark of every
table it was copying, which is what Reconciliation A reads as `mirror_failing`.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
import re
import time
from typing import Any, Dict, List, Mapping, Sequence, Tuple

logger = logging.getLogger(__name__)

OFFER_STOCKS_TABLE = "bronze.offer_stocks"
GOALS_TABLE = "app.revenue_goals"
MANUAL_EXPENSES_TABLE = "app.manual_expenses"
EXPENSE_TYPES_TABLE = "bronze.expense_types"
MISSES_TABLE = "app.order_backfill_misses"
INVENTORY_HISTORY_TABLE = "app.inventory_history"
SKU_STATUS_TABLE = "app.sku_inventory_status"
SKU_HISTORY_TABLE = "app.inventory_sku_history"
MOVEMENTS_TABLE = "app.stock_movements"
BUYER_GENDER_TABLE = "app.buyer_gender"
# The watchdog samples and the two send ledgers (revision 0026). Five tables
# that share no subject and do share a shipping shape: every one is rewritten
# whole or upserted over a fixed key by its DuckDB writer, so `_FULL_REPLACE`
# carries them with no new machinery. See that revision for why they travel
# together and why the three watchdogs may not use a watermark.
DISK_SAMPLES_TABLE = "app.disk_samples"
DATA_DIR_SAMPLES_TABLE = "app.data_dir_samples"
MEMORY_SAMPLES_TABLE = "app.memory_samples"
WEEKLY_SENDS_TABLE = "app.weekly_report_sends"
TRAFFIC_SENDS_TABLE = "app.traffic_report_sends"
# The two forensic logs (revision 0027). Append-only and large — 74 388 rows
# between them — so they ship above a watermark like the two inventory tables,
# not as a full replace.
REFRESHES_TABLE = "app.warehouse_refreshes"
RECONCILIATION_LOG_TABLE = "app.reconciliation_log"
# The data-quality journal (revision 0028): one run and its two children.
DQ_RUNS_TABLE = "app.data_quality_runs"
DQ_ISSUES_TABLE = "app.data_quality_issues"
DQ_DIFFS_TABLE = "app.data_quality_diffs"
# The offer catalogue and the sync watermarks (revision 0029).
OFFERS_TABLE = "bronze.offers"
SYNC_METADATA_TABLE = "app.sync_metadata"
# The forecast group (revision 0025). Four tables, 313 rows, and the reason
# they come first out of the sixteen with no Postgres home: `get_predictions`
# and `generate_smart_goals` are the last two dashboard reads still tied to
# DuckDB, and they are tied to it only because these are not here.
PREDICTIONS_TABLE = "app.revenue_predictions"
SEASONAL_TABLE = "app.seasonal_indices"
WEEKLY_PATTERNS_TABLE = "app.weekly_patterns"
GROWTH_METRICS_TABLE = "app.growth_metrics"

BUYER_GENDER_COLUMNS: Tuple[str, ...] = (
    "buyer_id", "gender", "method", "confidence", "decided_from",
    "rules_version", "override_by_human", "decided_at",
)

# Bookkeeping is excluded by construction: DuckDB stamps `synced_at`, Postgres
# `mirrored_at`, and two correct copies differ on those.
OFFER_STOCK_COLUMNS: Tuple[str, ...] = (
    "id", "sku", "price", "purchased_price", "quantity", "reserve",
)

# Three rows at most, and the one the monthly report draws its target line
# from. Revision 0017 says why a number a human typed is replicated rather
# than derived.
GOAL_COLUMNS: Tuple[str, ...] = (
    "period_type", "goal_amount", "is_custom", "calculated_goal",
    "growth_factor", "updated_at",
)

# DuckDB's column order, `platform` last because a later migration added it
# to the table rather than into the middle of it. The comparison reads the
# same tuple, so the two cannot drift apart.
MANUAL_EXPENSE_COLUMNS: Tuple[str, ...] = (
    "id", "expense_date", "category", "expense_type", "amount",
    "currency", "note", "created_at", "updated_at", "platform",
)

# Imported from the shared parse, not restated: the *display* name is built
# there from a localisation key, and a second list here would be a second
# chance to disagree about what an expense is called.
from core.landing_rows import EXPENSE_TYPE_COLUMNS  # noqa: E402

MISS_COLUMNS: Tuple[str, ...] = ("order_id", "checked_at", "reason")

INVENTORY_HISTORY_COLUMNS: Tuple[str, ...] = (
    "date", "total_quantity", "total_value", "total_reserve", "sku_count",
    "recorded_at",
)

SKU_STATUS_COLUMNS: Tuple[str, ...] = (
    "offer_id", "product_id", "sku", "name", "brand", "category_id",
    "quantity", "reserve", "price", "purchased_price", "last_sale_date",
    "first_seen_at", "updated_at", "last_stock_out_at",
)

SKU_HISTORY_COLUMNS: Tuple[str, ...] = (
    "date", "offer_id", "quantity", "reserve", "price",
)

MOVEMENT_COLUMNS: Tuple[str, ...] = (
    "id", "offer_id", "product_id", "movement_type", "quantity_before",
    "quantity_after", "delta", "reserve_before", "reserve_after",
    "recorded_at", "source",
)

PREDICTION_COLUMNS = (
    "prediction_date", "sales_type", "predicted_revenue",
    "model_mae", "model_mape", "model_wape", "created_at",
)

SEASONAL_COLUMNS = (
    "month", "seasonality_index", "sample_size", "avg_revenue",
    "min_revenue", "max_revenue", "yoy_growth", "confidence", "updated_at",
)

WEEKLY_PATTERN_COLUMNS = (
    "month", "week_of_month", "weight", "sample_size", "updated_at",
)

GROWTH_METRIC_COLUMNS = (
    "metric_type", "value", "period_start", "period_end",
    "sample_size", "updated_at",
)

# The watchdogs. Every measure stays a float on both sides — see revision 0026
# on why none of them is NUMERIC — so none of these appears in a `numeric=`
# tuple, and that absence is the deliberate half of the decision.
DISK_SAMPLE_COLUMNS: Tuple[str, ...] = (
    "sampled_at", "db_size_mb", "disk_pct_used", "disk_free_gb",
)
DATA_DIR_SAMPLE_COLUMNS: Tuple[str, ...] = ("sampled_at", "path_group", "bytes")
MEMORY_SAMPLE_COLUMNS: Tuple[str, ...] = (
    "sampled_at", "working_set_mb", "page_cache_mb", "limit_mb", "oom_kills",
)

# The two ledgers are the same five columns, and they keep two tuples rather
# than sharing one: the tables are separate on purpose, and a shared constant
# would be the first step back towards merging them.
WEEKLY_SEND_COLUMNS: Tuple[str, ...] = (
    "week_start", "sales_type", "revenue", "orders", "sent_at",
)
TRAFFIC_SEND_COLUMNS: Tuple[str, ...] = (
    "week_start", "sales_type", "revenue", "orders", "sent_at",
)

# `silver_mode` is LAST because it is not in the DuckDB CREATE TABLE at all —
# migration 0005 adds it with an ALTER. Anyone rebuilding this tuple from the
# CREATE TABLE alone ships thirteen columns and silently loses the one that
# says whether Silver was rebuilt whole or incrementally.
REFRESH_COLUMNS: Tuple[str, ...] = (
    "id", "refreshed_at", "trigger", "duration_ms", "bronze_orders",
    "silver_rows", "gold_revenue_rows", "gold_products_rows",
    "silver_revenue_checksum", "gold_revenue_checksum", "checksum_match",
    "validation_passed", "error", "silver_mode",
)

RECONCILIATION_LOG_COLUMNS: Tuple[str, ...] = (
    "id", "check_date", "api_count", "db_count", "discrepancy",
    "discrepancy_pct", "status", "checked_at",
)

DQ_RUN_COLUMNS: Tuple[str, ...] = (
    "run_id", "started_at", "ended_at", "as_of", "window_start", "window_end",
    "layer", "status", "integrity_issues_count", "discrepancies_count",
    "critical_count", "warn_count", "api_calls_used", "duration_ms",
    "error_message",
)

# `count` is a reserved-ish word in neither engine and is left alone: renaming
# a column is how two stores stop being copies of each other.
DQ_ISSUE_COLUMNS: Tuple[str, ...] = (
    "run_id", "check_name", "table_name", "severity", "count", "sample_ids",
    "description",
)

DQ_DIFF_COLUMNS: Tuple[str, ...] = (
    "run_id", "month", "source_id", "diff_class", "field", "dk_value",
    "kc_value", "severity", "order_ids",
)

OFFER_COLUMNS: Tuple[str, ...] = ("id", "product_id", "sku", "synced_at")

SYNC_METADATA_COLUMNS: Tuple[str, ...] = ("key", "value", "updated_at")

# The keys that are deliberately not copied, and the only place they are named.
#
# `warehouse_dirty` is a coordination flag, not a fact: set when a sync changes
# something, read every two minutes by the warehouse refresh, and deleted
# CONDITIONALLY on its own `updated_at` — an optimistic concurrency token whose
# meaning is "a rebuild in this process still has to happen". Postgres runs no
# warehouse rebuild, so a copy of it there asserts something that cannot be
# true.
#
# It is also wrong in a specific, recurring way. The flag lives about two
# minutes, this copy runs hourly and the comparison runs at 07:30, so any
# window that catches it set at the copy and cleared at the check reports an
# orphan the copy itself created — with no grace to forgive it, because
# `mirror_orphan_rows` has none.
#
# That paragraph was already written here on 2026-09-14 and it named the wrong
# key. `warehouse_catalog_dirty` had been retired nine hours earlier with the
# products Gold it existed for; the live flag has always been `warehouse_dirty`.
# The prediction was exact and the exclusion missed it, so the mirror shipped
# the flag at 04:22 on 2026-09-15, DuckDB cleared it at 04:22:53, and the 04:30
# comparison reported the orphan this comment describes. A guard that names its
# subject guards only the subject you were thinking of.
#
# So the set is DERIVED, not recited: `_transient_sync_keys()` parses the
# statements DuckDB actually issues against `sync_metadata`, and a key that is
# deleted there is transient by definition. The retired key stays in the set
# because DuckDB was never asked to drop the row — databases that carry it hold
# one stale row, and this projection is why Postgres has never seen it.
#
# Expressed as the source the reads use, rather than as a filter the shipper
# and the comparison each apply: one text, imported by both, so they cannot
# come to disagree about what the Postgres copy is supposed to contain. The
# alias is required — DuckDB refuses an unnamed derived table.
def _transient_sync_keys() -> Tuple[str, ...]:
    """Every `sync_metadata` key DuckDB ever deletes, read out of the source.

    Parsed rather than listed because the listed form has already been wrong
    once, in the direction that is invisible until an alert fires: a key added
    to the store and not to a constant here gets shipped, and one retired from
    the store and left here stops protecting anything. Reading the statements
    means the two cannot drift apart without the parse noticing.

    Falls back to the known pair if the source cannot be read — an installed
    package with no .py beside it must not lose the exclusion and start
    shipping a flag hourly.
    """
    known = ("warehouse_dirty", "warehouse_catalog_dirty")
    try:
        import pathlib

        src = (pathlib.Path(__file__).with_name("duckdb_store.py")).read_text()
    except OSError:
        return known
    found = set(re.findall(
        r"DELETE\s+FROM\s+sync_metadata\s+WHERE\s+key\s*=\s*'([^']+)'",
        src, re.IGNORECASE,
    ))
    return tuple(sorted(found | set(known)))


TRANSIENT_SYNC_KEYS: Tuple[str, ...] = _transient_sync_keys()
SYNC_METADATA_SOURCE = (
    "(SELECT * FROM sync_metadata WHERE key NOT IN ("
    + ", ".join(f"'{k}'" for k in TRANSIENT_SYNC_KEYS)
    + ")) AS sync_metadata"
)

# The DuckDB table each one is read from. Postgres qualifies by schema and
# DuckDB does not, so the pair is spelled out rather than derived by stripping
# a prefix — a rule that guesses a table name is a rule that will guess wrong.
_FULL_REPLACE: Tuple[Tuple[str, str, Tuple[str, ...], str], ...] = (
    (OFFER_STOCKS_TABLE, "offer_stocks", OFFER_STOCK_COLUMNS, "id"),
    (GOALS_TABLE, "revenue_goals", GOAL_COLUMNS, "period_type"),
    # Full replace and not an upsert, because the expenses form genuinely
    # deletes: `DELETE FROM manual_expenses WHERE id = ?` is one of its three
    # statements, and a ghost here is ad spend that was withdrawn and still
    # divides the ROAS.
    (MANUAL_EXPENSES_TABLE, "manual_expenses", MANUAL_EXPENSE_COLUMNS, "id"),
    # Landing, and replicated rather than mirrored — `bronze.offer_stocks`'
    # exact situation and its reason. KeyCRM serves this dictionary, but only
    # to the **weekly** full sync, so a payload-fed mirror leaves Postgres
    # empty for up to a week. That is not a freshness detail: `/expenses`
    # renders the breakdown *by name*, so an empty dictionary collapses every
    # type into "Other" and empties the filter. Measured on production the
    # hour the flag first went on. DuckDB has the 27 rows every minute of that
    # week, so this copies them — until chain 6a moves the write itself to
    # Postgres (`KS_WRITE_EXPENSE_TYPES`, `core/pg_expense_types_write.py`),
    # from which point this entry stands down with the rest of that chain.
    (EXPENSE_TYPES_TABLE, "expense_types", EXPENSE_TYPE_COLUMNS, "id"),
    (MISSES_TABLE, "order_backfill_misses", MISS_COLUMNS, "order_id"),
    (INVENTORY_HISTORY_TABLE, "inventory_history", INVENTORY_HISTORY_COLUMNS, "date"),
    (SKU_STATUS_TABLE, "sku_inventory_status", SKU_STATUS_COLUMNS, "offer_id"),
    # Replicated for `app.manager_classifications`' reason, not `bronze.*`'s:
    # KeyCRM has no gender field, so nothing here can ever be re-fetched from
    # the source. It is decided by `core/gender.py` against the name DuckDB
    # holds, and a second derivation on the Postgres side would need the same
    # tables and would drift the day either copy was edited.
    #
    # Full replace, like `sku_inventory_status` and for the same reason: the
    # DuckDB writer is a DELETE+INSERT too (scripts/backfill_gender.py), so
    # this is the same operation rather than a separate decision. It also means
    # a row that is present and WRONG is corrected, which matters when
    # RULES_VERSION moves and every verdict is re-derived at once.
    #
    # 20 145 rows an hour to ship the ~19 new buyers a day is the cost. Measured
    # against the alternative — a `decided_at` watermark — it is not worth the
    # second mechanism: an upsert keyed on a clock cannot see a row whose
    # verdict was withdrawn to NULL, and withdrawal is exactly what a rules
    # change does.
    (BUYER_GENDER_TABLE, "buyer_gender", BUYER_GENDER_COLUMNS, "buyer_id"),
    # ── the forecast group ──
    #
    # `revenue_predictions` is a full replace because its DuckDB writer is a
    # DELETE followed by an INSERT — the same operation, so nothing is being
    # decided here. The other three are written with ON CONFLICT DO UPDATE
    # over a *fixed* key space — twelve months, sixty month-weeks, one metric
    # type — so replacing them whole is the same set of rows and additionally
    # corrects one that is present and wrong. 313 rows in total; the cost of
    # the decision is nothing and the alternative leaves a stale row standing.
    (PREDICTIONS_TABLE, "revenue_predictions", PREDICTION_COLUMNS, "prediction_date"),
    (SEASONAL_TABLE, "seasonal_indices", SEASONAL_COLUMNS, "month"),
    (WEEKLY_PATTERNS_TABLE, "weekly_patterns", WEEKLY_PATTERN_COLUMNS, "month"),
    (GROWTH_METRICS_TABLE, "growth_metrics", GROWTH_METRIC_COLUMNS, "metric_type"),
    # ── the watchdog samples and the send ledgers (revision 0026) ──
    #
    # These are the first tables here with a *scheduled* retention DELETE —
    # the memory sweep runs every thirty minutes — and that is what settles
    # their shape rather than a preference. A watermark can only ever add
    # rows, so a pruned sample would stay in Postgres for ever and the daily
    # comparison would report an orphan this copy had created. A full replace
    # writes exactly what DuckDB holds, so a prune arrives with everything
    # else and there is nothing left to reconcile.
    #
    # Ordered by their key, like every entry above, so `read_full_replace`
    # returns rows in a stable order and a diff of two runs is readable.
    (DISK_SAMPLES_TABLE, "disk_samples", DISK_SAMPLE_COLUMNS, "sampled_at"),
    (DATA_DIR_SAMPLES_TABLE, "data_dir_samples", DATA_DIR_SAMPLE_COLUMNS,
     "sampled_at, path_group"),
    (MEMORY_SAMPLES_TABLE, "memory_samples", MEMORY_SAMPLE_COLUMNS, "sampled_at"),
    # The smallest two tables here and the only ones whose loss is visible to
    # a person: without them two daily-ticking jobs stop knowing which week
    # they have already reported, and every approved user gets the same
    # message seven times.
    (WEEKLY_SENDS_TABLE, "weekly_report_sends", WEEKLY_SEND_COLUMNS,
     "week_start, sales_type"),
    (TRAFFIC_SENDS_TABLE, "traffic_report_sends", TRAFFIC_SEND_COLUMNS,
     "week_start, sales_type"),
    # ── the data-quality journal (revision 0028) ──
    #
    # Append-only in DuckDB, and full-replaced here anyway. A watermark is the
    # obvious shape for an append table and it was the first draft, with the
    # two children shipping under the PARENT's `MAX(run_id)` — because a run
    # that finds nothing writes no child rows, and a self-derived watermark on
    # `data_quality_diffs` would sit 336 runs behind (it last gained a row at
    # run 260; runs are at 596).
    #
    # That reasoning was sound and the premise was not: 1 057 rows between the
    # three is smaller than `bronze.offer_stocks` and `app.sku_inventory_status`,
    # both rewritten whole every hour already. A watermark buys nothing at this
    # size and costs a three-table coupling that has to stay right for ever.
    # Full replace also absorbs a future retention sweep on the journal without
    # a line of change, where a watermark would accumulate every pruned row.
    #
    # Parent first, so that the transaction never holds children without the
    # run they belong to even for a statement.
    (DQ_RUNS_TABLE, "data_quality_runs", DQ_RUN_COLUMNS, "run_id"),
    (DQ_ISSUES_TABLE, "data_quality_issues", DQ_ISSUE_COLUMNS,
     "run_id, check_name, table_name"),
    (DQ_DIFFS_TABLE, "data_quality_diffs", DQ_DIFF_COLUMNS,
     "run_id, month, source_id, diff_class, field"),
    # ── the offer catalogue and the sync watermarks (revision 0029) ──
    #
    # `offers` rides here rather than in `core/pg_landing.py` for exactly
    # `bronze.offer_stocks`' reason, stated in this module's docstring: the two
    # are halves of one inventory sync, read together by
    # `sku_inventory_status`, and `stock_movements` is a delta against
    # `offer_stocks` computed on this same tick. Splitting them across the
    # mirror and the replicator would give them two clocks and two ways to
    # stand down, which is what silently froze `offer_stocks` on 2026-09-06.
    (OFFERS_TABLE, "offers", OFFER_COLUMNS, "id"),
    # A projection, not the whole table — see `SYNC_METADATA_SOURCE`.
    (SYNC_METADATA_TABLE, SYNC_METADATA_SOURCE, SYNC_METADATA_COLUMNS, "key"),
)

# How many rows one `executemany` carries. 143,274 in a single call is one
# round trip holding one very large parameter list; chunking keeps the
# statement cache and the server's memory both bored.
CHUNK = 5000


def _insert(table: str, columns: Sequence[str]) -> str:
    values = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({values})"


def _upsert(table: str, columns: Sequence[str], keys: Sequence[str]) -> str:
    """Insert, or overwrite the row already there.

    `inventory_sku_history` needs it on every run, because it re-ships the day
    it is resuming from. `stock_movements` needs it only under `full=True`,
    where the point is to correct rows below the watermark that are missing or
    wrong; on the incremental path it takes a plain INSERT so that a conflict
    is an error rather than a silent overwrite.
    """
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c not in keys
    )
    return (
        f"{_insert(table, columns)} "
        f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}"
    )


async def _write_chunked(conn, sql: str, rows: Sequence[tuple]) -> None:
    for start in range(0, len(rows), CHUNK):
        await conn.executemany(sql, rows[start:start + CHUNK])


def read_full_replace(conn, skip: "FrozenSet[str]" = frozenset()) -> Dict[str, List[tuple]]:
    """The seven small tables out of DuckDB, in the column order Postgres wants.

    `skip` are stood-down tables, not read at all: DuckDB is no longer their
    writer, and after stage 5 they will not exist to read."""
    out: Dict[str, List[tuple]] = {}
    for pg_table, dk_table, columns, order_by in _FULL_REPLACE:
        if pg_table in skip:
            continue
        rows = conn.execute(
            f"SELECT {', '.join(columns)} FROM {dk_table} ORDER BY {order_by}"
        ).fetchall()
        out[pg_table] = [tuple(r) for r in rows]
    return out


@dataclass(frozen=True)
class _Append:
    """One table shipped above a watermark, declared rather than hardcoded.

    This was two tables written out longhand in five places — the watermark
    reads, this reader's signature and its two-tuple return, the write branch
    and the two row-count stamps. Adding a third meant editing all five, and a
    fourth meant editing them again; revision 0027 needed two more and the
    quality journal needs three after that, so the shape became a list.

    The two fields that are not obvious:

    `inclusive` picks `>=` over `>`. `inventory_sku_history` asks for
    `date >= MAX(date)` because a day is written as ~887 rows in one statement
    and `>` would step over a day that was half-written, leaving a hole nothing
    would ever fill. Re-shipping one day costs nothing and an upsert makes it
    not matter. Everything keyed on a monotone id asks for `> MAX(id)`: those
    rows go in one transaction ordered by id, so what Postgres holds is always
    a complete prefix and re-shipping the last row would be pure waste.

    `always_upsert` is the same decision seen from the write side.
    `inventory_sku_history` re-ships the day it resumes from and must
    therefore overwrite. The id-keyed tables take a plain INSERT on the
    incremental path, so that a conflict is an error rather than a silent
    overwrite — a row above `MAX(id)` cannot already be there, and if it is,
    the watermark logic is wrong and should say so loudly. Under `full=True`
    every one of them upserts, because the point of that path is to correct a
    row below the watermark that is missing *or* wrong.
    """
    pg_table: str
    dk_table: str
    columns: Tuple[str, ...]
    watermark: str                  # the column MAX() is taken over
    order_by: str
    keys: Tuple[str, ...]           # ON CONFLICT target
    inclusive: bool = False         # `>=` rather than `>`
    always_upsert: bool = False


_APPEND_ABOVE: Tuple[_Append, ...] = (
    _Append(
        pg_table=SKU_HISTORY_TABLE, dk_table="inventory_sku_history",
        columns=SKU_HISTORY_COLUMNS, watermark="date",
        order_by="date, offer_id", keys=("date", "offer_id"),
        inclusive=True, always_upsert=True,
    ),
    _Append(
        pg_table=MOVEMENTS_TABLE, dk_table="stock_movements",
        columns=MOVEMENT_COLUMNS, watermark="id",
        order_by="id", keys=("id",),
    ),
    # ── the two forensic logs (revision 0027) ──
    #
    # `warehouse_refreshes` is 74 388 rows, more than every other stage-3 table
    # together, and gains ~400-700 a day. `reconciliation_log` gains ~14. Both
    # are INSERT-only with no retention anywhere — established by searching the
    # repository, which is also what disqualified the three watchdogs from this
    # shape: a watermark can only ever add rows, so a table that sweeps by age
    # would grow here without bound.
    _Append(
        pg_table=REFRESHES_TABLE, dk_table="warehouse_refreshes",
        columns=REFRESH_COLUMNS, watermark="id", order_by="id", keys=("id",),
    ),
    _Append(
        pg_table=RECONCILIATION_LOG_TABLE, dk_table="reconciliation_log",
        columns=RECONCILIATION_LOG_COLUMNS, watermark="id",
        order_by="id", keys=("id",),
    ),
)


def read_appends(
    conn, since: Mapping[str, Any], skip: "FrozenSet[str]" = frozenset(),
) -> Dict[str, List[tuple]]:
    """Each append-only table, from where Postgres left off.

    A watermark of None means "Postgres holds nothing" and asks for the whole
    table, which is what the first run of each does. `skip` as in
    `read_full_replace`.
    """
    out: Dict[str, List[tuple]] = {}
    for spec in _APPEND_ABOVE:
        if spec.pg_table in skip:
            continue
        cols = ", ".join(spec.columns)
        mark = since.get(spec.pg_table)
        if mark is None:
            rows = conn.execute(
                f"SELECT {cols} FROM {spec.dk_table} ORDER BY {spec.order_by}"
            ).fetchall()
        else:
            op = ">=" if spec.inclusive else ">"
            rows = conn.execute(
                f"SELECT {cols} FROM {spec.dk_table} "
                f"WHERE {spec.watermark} {op} ? ORDER BY {spec.order_by}",
                [mark],
            ).fetchall()
        out[spec.pg_table] = [tuple(r) for r in rows]
    return out


# One copy at a time. `replicate_operational` has two callers — the hourly job
# and `replicate_after_manual_expense` on every expense form submit — and
# APScheduler's `max_instances=1` covers only the first. Two runs that overlap
# read the same `MAX(id)` watermarks and both INSERT the rows above them; the
# one that commits second fails on the primary key, and then stamps that
# failure on every table it was copying *after* the winner's OK. A copy that is
# fully up to date then reads as failing until the next hour — a `mirror_failing`
# on every one of them at 07:30, from a run nothing was wrong with. Serialised,
# the second run reads the watermarks the first one left and ships nothing twice.
#
# `core.pg_silver.PG_LAYER_LOCK`'s shape and its rule: a coordination lock over
# network-bound work, never the DuckDB store's lock, which is not held across
# the network. The form submit waits out a copy already in flight — ~120 ms
# incremental, measured — rather than racing it.
OPERATIONAL_REPLICATION_LOCK = asyncio.Lock()

# How long the expense form waits for a copy already in flight before it leaves
# the row to the hourly job. Three seconds is longer than the whole incremental
# copy (116 ms measured against the production catalogue) and shorter than
# anyone's patience with a form that has already saved.
FORM_COPY_WAIT_S = 3.0


def _tables_to_ship(stood_down) -> Tuple[str, ...]:
    """Every table `replicate_operational` copies, minus the stood-down ones."""
    return tuple(
        table for table in (
            *(t for t, _d, _c, _o in _FULL_REPLACE),
            *(spec.pg_table for spec in _APPEND_ABOVE),
        )
        if table not in stood_down
    )


async def replicate_operational(
    store, *, full: bool = False, wait_s: Optional[float] = None,
) -> Dict[str, Any]:
    """Copy all nine tables from DuckDB to Postgres. Never raises.

    `full=True` ignores both watermarks and re-ships everything, upserting the
    two append-only tables so a row that is missing *or* wrong below the
    watermark is put right. It is the repair path for a finding the daily
    comparison raised, and it is never taken on a schedule.

    The DuckDB reads and the Postgres round trips are in separate blocks: the
    store's lock is not reentrant and every other reader waits behind it, so
    awaiting the network while holding it would stall the dashboard for the
    length of the copy.

    The two watermark reads happen **before** the DuckDB read, so a row written
    between them is re-shipped rather than skipped. The other order loses rows;
    this one costs a duplicate that the upsert and the `id >` filter both
    absorb.
    """
    from core.mirror_reconciliation import configured

    if not configured():
        return {"skipped": "KS_PG_DSN is not set"}

    # Serialised: see OPERATIONAL_REPLICATION_LOCK. The hourly job waits for
    # however long a copy in flight takes, because it has nowhere else to be.
    # A form submit passes `wait_s` and gives up instead: the copy it wanted is
    # the one already running, and the hourly job ships its row within the hour
    # in any case. Waiting out a slow Postgres there — a migration's ACCESS
    # EXCLUSIVE lock during a deploy, a saturated pool — would hold the expense
    # form open for the length of somebody else's copy.
    if wait_s is None:
        await OPERATIONAL_REPLICATION_LOCK.acquire()
    else:
        try:
            await asyncio.wait_for(OPERATIONAL_REPLICATION_LOCK.acquire(), wait_s)
        except asyncio.TimeoutError:
            return {"skipped": f"another copy was still in flight after {wait_s:g}s"}
    try:
        started = time.monotonic()
        # The tables this run is copying, known before the first round trip so
        # that a failure anywhere after it can be written against each of them.
        shipping: Tuple[str, ...] = ()
        try:
            from core.pg import get_pool, require_revision
            from core.pg_landing import _WATERMARK_OK

            # Tables this run must not touch, because DuckDB is no longer their
            # writer. A full replace out of a frozen DuckDB would roll back every
            # row written since the switch, once an hour, looking healthy in
            # between — `replicate_sms`' recorded failure, and the reason it stands
            # down as a whole rather than per row.
            #
            # The set comes from `core.write_chains.stood_down_tables()` rather than
            # being spelled here — every write chain's `CHAIN_TABLES`, for the chains
            # whose flag is on — so no writer and the shipper can come to disagree
            # about which tables have changed hands.
            from core.write_chains import (
                WRITE_CHAINS, chain_name, mismatched_chains, stood_down_tables_checked,
            )

            # A chain whose KS_WRITE_* is not understood stands down with the chains
            # that write Postgres — shipping could overwrite rows Postgres alone
            # holds — and its tables are stamped failing, so the watermark says so
            # rather than aging quietly. Every other table still ships (DN-01).
            stood_down, chain_errors = stood_down_tables_checked()
            # Before the owner rows are read, the local markers and flags are all
            # this run has, so a failure this early may stamp a table an owner row
            # would have held down. It is never written either way.
            shipping = _tables_to_ship(stood_down)

            # A chain that owns its tables in Postgres while its variable says
            # otherwise (DN-06). It stands down through `stood_down` like any
            # other Postgres-writing chain — the latch decides, not the flag —
            # and the disagreement is stamped on its tables so an operator who
            # flipped the variable back finds out from the store rather than
            # from a number that stopped moving.
            mismatches = mismatched_chains()

            pool = await get_pool()
            await require_revision()

            # The second copy of the latch, asked because this function is
            # already holding the connection that carries it.
            # `stood_down_tables_checked()` reads the local markers, and the
            # markers live on a bind mount: an older `./data` snapshot, a wrong
            # mount or a rebuilt data directory loses them while Postgres keeps
            # the owner rows and the rows they were taken for. This copy is what
            # stops the full replace below from putting `app.manual_expenses`
            # back the way a frozen DuckDB remembers it — a destruction the daily
            # comparison would report 24 h later, of rows only Postgres held.
            from core import chain_latch

            owners = await chain_latch.read_owners(pool)
            marker_lost = {name: at for name, at in chain_latch.claimed_chains(owners).items()
                           if not chain_latch.latched(name)}
            # An owner row counts as itself as well as through its chain
            # (`owned_tables`): on an image older than the chain no chain here
            # declares the table, and the replace below would put DuckDB's
            # frozen copy over what the chain wrote — `app.buyer_gender`'s
            # human overrides, reproduced in the DN-22b review. Not stamped on
            # the watermark: the chain, not this job, is what writes these
            # tables, and a count only a later shipment could clear would
            # outlive the rollback. The daily comparison pages it instead
            # (`chain_owner_unregistered`).
            unregistered = {
                table: at
                for table, at in chain_latch.unregistered_owned_tables(owners).items()
                if table in _tables_to_ship(frozenset())
            }
            stood_down = stood_down | chain_latch.owned_tables(owners)
            # Narrowed again, so that a failure from here on is stamped only on
            # tables this run was really going to write — never on one Postgres
            # owns, whose watermark describes a copy that is standing down.
            shipping = _tables_to_ship(stood_down)

            # A watermark of None asks for the whole table, which is what `full`
            # means and what a first run finds anyway.
            since: Dict[str, Any] = {spec.pg_table: None for spec in _APPEND_ABOVE}
            if not full:
                async with pool.acquire() as conn:
                    for spec in _APPEND_ABOVE:
                        if spec.pg_table in stood_down:
                            continue
                        since[spec.pg_table] = await conn.fetchval(
                            f"SELECT MAX({spec.watermark}) FROM {spec.pg_table}"
                        )

            async with store.connection() as conn:
                replaced = read_full_replace(conn, skip=stood_down)
                appended = read_appends(conn, since, skip=stood_down)
                totals = {
                    spec.pg_table: conn.execute(
                        f"SELECT COUNT(*) FROM {spec.dk_table}"
                    ).fetchone()[0]
                    for spec in _APPEND_ABOVE if spec.pg_table not in stood_down
                }

            async with pool.acquire() as conn:
                async with conn.transaction():
                    for pg_table, _dk, columns, _order in _FULL_REPLACE:
                        if pg_table in stood_down:
                            continue
                        rows = replaced[pg_table]
                        await conn.execute(f"DELETE FROM {pg_table}")
                        if rows:
                            await _write_chunked(
                                conn, _insert(pg_table, columns), rows,
                            )
                        await conn.execute(_WATERMARK_OK, pg_table, len(rows))

                    for spec in _APPEND_ABOVE:
                        if spec.pg_table in stood_down:
                            continue
                        rows = appended[spec.pg_table]
                        if rows:
                            # A plain INSERT on the incremental path on purpose:
                            # rows above the watermark cannot already be there, so
                            # a conflict would mean the watermark logic is wrong
                            # and should say so loudly rather than overwrite and
                            # look fine. `inventory_sku_history` is the exception —
                            # it deliberately re-ships the day it resumed from.
                            upsert = spec.always_upsert or full
                            await _write_chunked(
                                conn,
                                _upsert(spec.pg_table, spec.columns, spec.keys)
                                if upsert else
                                _insert(spec.pg_table, spec.columns),
                                rows,
                            )
                        # `last_rows` is the whole table, not this run's delta. The
                        # comparison reads it as "how much should be here", and an
                        # append that shipped nothing would otherwise record zero
                        # and read as an empty table.
                        await conn.execute(
                            _WATERMARK_OK, spec.pg_table, int(totals[spec.pg_table]),
                        )

            # Only what was written. A stood-down table is read out of DuckDB with
            # the rest but never written, and reporting its read count under
            # "replaced" made the first log line after KS_WRITE_EXPENSES was
            # switched on (2026-09-17) read as the hourly copy wiping the table it
            # had in fact left alone.
            result = {
                "full": full,
                "replaced": {t: len(replaced[t]) for t, _d, _c, _o in _FULL_REPLACE
                             if t not in stood_down},
                "appended": {
                    spec.pg_table: len(appended[spec.pg_table])
                    for spec in _APPEND_ABOVE if spec.pg_table not in stood_down
                },
                # The two original spellings, kept beside the map they are now
                # read out of. They are what a year of production log lines say
                # and what anyone grepping back through them will look for; a
                # rename would make the history harder to read for no gain.
                "sku_history_appended": (len(appended[SKU_HISTORY_TABLE])
                                         if SKU_HISTORY_TABLE in appended else None),
                "movements_appended": (len(appended[MOVEMENTS_TABLE])
                                       if MOVEMENTS_TABLE in appended else None),
                "duration_ms": int((time.monotonic() - started) * 1000),
            }
            if stood_down:
                result["stood_down"] = sorted(stood_down)
            if chain_errors:
                result["chain_flag_errors"] = chain_errors
            if mismatches:
                result["chain_flag_mismatch"] = mismatches
            if marker_lost:
                result["chain_marker_lost"] = marker_lost
            if unregistered:
                result["chain_owner_unregistered"] = unregistered
                logger.warning(
                    "Operational history: owner rows name %s, which no chain in "
                    "this build declares; not copied out of DuckDB",
                    ", ".join(sorted(unregistered)))
            # One stamp per table per reason, so a chain that is both latched
            # and misspelt says both things rather than the first one found. The
            # flag error is written LAST and so is the one `last_error` keeps:
            # it is the actionable half — correcting the variable to `postgres`
            # also ends the mismatch, while releasing the latch would not fix a
            # typo.
            stamps = [(name, f"owned by Postgres since {at}; "
                             "run scripts/chain_copy_back.py")
                      for name, at in mismatches.items()]
            # A chain held down by its owner rows alone is stamped too, because
            # a stand-down that says nothing is a watermark ageing quietly — and
            # this one also means the routing copy is gone, so the writers are
            # following KS_WRITE_* again while this job declines to ship. The
            # daily comparison files `chain_latch_disagrees` for the same state.
            stamps += [(name, f"owner rows in Postgres since {at} and no local "
                              "marker: restore data/write-chain-owners or run "
                              "scripts/chain_copy_back.py")
                       for name, at in marker_lost.items()]
            stamps += [(name, f"not shipped: {error}")
                       for name, error in chain_errors.items()]
            if stamps:
                from core.pg_landing import _record_failure

                # Only on tables this job ships itself. A chain may also own
                # tables another shipper carries — chain 4's bronze buyers are
                # the buyers mirror's — and a failure stamped on one of those
                # is a count this job never clears: only that shipper's next
                # non-empty success resets `failures_since_ok`, so after a
                # rollback the daily comparison would page `mirror_failing` on
                # two stores that agree. Those tables carry the disagreement
                # through `chain_latch_disagrees`, `write_chain_flag_mismatch`
                # and `write_chain_flag_invalid` instead. Every table of chains
                # 1, 8, 7a and 6a is shipped here, so for them nothing moves.
                ships = set(_tables_to_ship(frozenset()))
                for name, note in stamps:
                    chain = next(c for c in WRITE_CHAINS if chain_name(c) == name)
                    for table in chain.CHAIN_TABLES:
                        if table in ships:
                            await _record_failure(table, note)
            logger.info("Operational history replicated: %s", result)
            return result
        except Exception as e:
            # ERROR, not DEBUG. A copy that fails quietly is the 2026-08-09 shape.
            logger.error(
                "Operational history replication failed: %s", e, exc_info=True,
            )
            detail = f"{type(e).__name__}: {e}"
            # And into the watermark of every table the run was copying, because
            # a watermark that merely stops moving says too little, too late.
            # Every table here is written in one transaction, so one bad row fails
            # all of them: a duplicate id in DuckDB's `reconciliation_log`, which
            # has no key there and a primary key here, does it every hour until
            # somebody removes the row. Without this stamp that froze the stock
            # movements, the refresh log and the quality journal with an ERROR
            # line as the only trace — `last_ok_at` kept the last success and
            # `failures_since_ok` stayed at zero, so the 07:30 comparison read a
            # healthy copy and could at most report the rows it lacked, as a
            # symptom.
            # Now it reports `mirror_failing` with the error, and the next run that
            # succeeds clears the count through `_WATERMARK_OK`, as for every
            # other copy.
            from core.pg_landing import _record_failure

            for table in shipping:
                if not await _record_failure(table, detail):
                    # The watermark table is behind the same server. When one
                    # stamp cannot be written the rest will not be either, and
                    # each attempt at an unreachable server can cost a connect
                    # timeout.
                    break
            return {"error": detail}
    finally:
        OPERATIONAL_REPLICATION_LOCK.release()


async def replicate_after_manual_expense(store) -> Dict[str, Any]:
    """Carry a manual expense to Postgres now, rather than within the hour.

    WHY THIS EXISTS AT ALL, GIVEN THE ONE-CALL-SITE RULE ABOVE

    `replicate_operational` is hourly and has exactly one scheduled caller,
    because five code paths write these tables and hooking each is how the
    sixth gets forgotten — which is precisely what `update_manager_stats` did
    until `cf34e8b`. That rule is about *writers*, and this is not a second
    writer: it is the same function, called sooner.

    What changed is who reads. While `manual_expenses` was written in DuckDB
    and read in DuckDB, an hour of lag on the copy cost nothing —
    `core/pg_landing.py`'s revision 0018 note says so in as many words, and
    adds that the one-line fix is available if it ever stops being true.
    `KS_READ_EXPENSES=postgres` is when it stops: the expenses form writes one
    store and the page it returns to reads the other, so without this a human
    types an amount and watches it not appear.

    Measured: the incremental replication is 116 ms against the production
    catalogue, which is nothing on a form submit. The full first run is 9.3 s,
    and only ever happens once. A submit that lands during the hourly copy
    waits for it (`OPERATIONAL_REPLICATION_LOCK`) instead of racing it to the
    same rows — but only for `FORM_COPY_WAIT_S`. Past that, the copy in flight
    is the one this call wanted and the hourly job carries the row within the
    hour, so the endpoint returns rather than holding the form open for the
    length of somebody else's copy.

    **Never raises.** The DuckDB write has already committed and the endpoint
    must report it; a Postgres fault costs freshness until the hourly job,
    which is exactly where this table was before this function existed.
    """
    from core.mirror_reconciliation import configured

    if not configured():
        return {"skipped": "KS_PG_DSN is not set"}
    try:
        return await replicate_operational(store, wait_s=FORM_COPY_WAIT_S)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Manual expense not replicated — Postgres keeps the previous "
            "copy until the hourly job: %s", exc, exc_info=True,
        )
        return {"error": f"{type(exc).__name__}: {exc}"}
