"""What became of every DuckDB table and view — a frozen literal.

Stage 5 deleted DuckDB from the code, and its preconditions opened with "a
table-fate manifest covering every DuckDB table, and a test walks
`_init_schema` and a live file against it". This was that manifest: every
name the code could put into `analytics.duckdb`, and every name the
production file still held after its DDL went, with one entry here.

**Since step 14's S14-18 it is a literal and nothing more** (`FATES`, its
types and the vocabulary its entries are written in). The schema it was
walked against (`DuckDBStore._init_schema`), the compaction whose lists it
was checked against (`scripts/compact_duckdb.py`) and the reader of the
switches that said which tables DuckDB still wrote (`duckdb_written`,
`STORE_SWITCHES`) are deleted: nothing in any process writes the file. What
is left reads a *file*, never the code — `deploy/duckdb_table_fates_check.py`,
run by hand against the newest nightly backup inside the pinned image
(`rollback-step14`), which mounts this module beside it and so may import
nothing outside the standard library; and the final stage, which deletes the
file and reads here what each table became. The test
(`tests/unit/test_duckdb_table_fates.py`) holds the literal to the check on a
fixture file.

NOTHING READS THIS IN PRODUCTION.

WHAT AN ENTRY SAYS

- `kind` — MOVED (a writer moved and the rows were carried to `successors`),
  DERIVED (recomputed from Postgres, or a view Postgres renders itself),
  ARCHIVE_ONLY (history, frozen where it stands — the Postgres copy and the
  off-site archive — and nothing writes it again), RETIRED (no successor; the
  `reason` says why).
- `switch` — the variable that moved the writes: a write chain's `KS_WRITE_*`,
  `KS_SMS_STORE`, `KS_USER_STORE` or `KS_WRITE_WAREHOUSE`. Since step 14 no
  code reads any of them; they stay in `.env` while an image from before step
  14 is a rollback target, and here as what moved each table.
- `origin` — what could rebuild the rows if every copy were lost: KeyCRM, a
  computation over other tables, or nothing (IRREPLACEABLE).
- `ddl` — SCHEMA (a fresh file had it), SCRATCH (a migration made and dropped
  it inside one step; a file holds one only if that step died), NONE (no DDL
  left; the production file still holds it).
- `compaction` — what the weekly compaction did with it, while there was one:
  EXPORTED (to Parquet, which is also the off-site archive), SKIPPED (its
  `DERIVED_TABLES`), or NOT_A_TABLE (views; the export read base tables only).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

# ── kind ──
MOVED = "moved"
DERIVED = "derived"
ARCHIVE_ONLY = "archive-only"
RETIRED = "retired-no-successor"
KINDS = (MOVED, DERIVED, ARCHIVE_ONLY, RETIRED)

# ── origin ──
KEYCRM = "keycrm"
COMPUTED = "computed"
IRREPLACEABLE = "irreplaceable"
NOTHING = "nothing"
ORIGINS = (KEYCRM, COMPUTED, IRREPLACEABLE, NOTHING)

# ── ddl ──
SCHEMA = "schema"
SCRATCH = "scratch"
NONE = "none"
DDLS = (SCHEMA, SCRATCH, NONE)

# ── compaction ──
EXPORTED = "exported"
SKIPPED = "skipped"
NOT_A_TABLE = "not-a-table"
COMPACTIONS = (EXPORTED, SKIPPED, NOT_A_TABLE)

# ── object ──
TABLE = "BASE TABLE"
VIEW = "VIEW"

# The switches that were not a write chain's, as `FATES` names them. Nothing
# reads them since step 14 (`STORE_SWITCHES` and their readers went in S14-18).
SMS_STORE = "KS_SMS_STORE"
USER_STORE = "KS_USER_STORE"
WAREHOUSE = "KS_WRITE_WAREHOUSE"

# OD-11 (owner, 01.10.2026): no DROP of any kind before the owner's week after
# stage 4 completes — removing a table from `_init_schema`, adding a name to
# the compaction's `DERIVED_TABLES` and deleting the file all count. The test
# pins both sets as literals beside this constant, so the change that breaks
# the rule has to say so in the diff.
DROPS_ALLOWED = False


@dataclass(frozen=True)
class TableFate:
    kind: str
    origin: str
    reason: str
    successors: Tuple[str, ...] = ()
    chain: Optional[str] = None
    switch: Optional[str] = None
    ddl: str = SCHEMA
    compaction: str = EXPORTED
    object: str = TABLE

    @property
    def offsite(self) -> bool:
        """In the off-site Parquet archive. The nightly and the weekly archive
        are both `phase1_export`'s output, which carries every base table the
        compaction does not skip — empty ones are named in its manifest with
        no file."""
        return self.compaction == EXPORTED


def _view(successor: str, reason: str) -> TableFate:
    return TableFate(kind=DERIVED, origin=COMPUTED, reason=reason,
                     successors=(successor,), compaction=NOT_A_TABLE,
                     object=VIEW)


_INVENTORY_VIEW = (
    "Rendered from the one body in core.sql_dialect.inventory_view_selects; "
    "Postgres has held its own rendering since revision 0014, so nothing is "
    "carried and nothing writes a view.")

FATES: Dict[str, TableFate] = {
    # ── chain 1: inventory (KS_WRITE_INVENTORY, registered) ─────────────────
    "offers": TableFate(
        MOVED, KEYCRM, "The offer catalogue; chain 1 writes it with the stocks "
        "it stamps product ids from.",
        successors=("bronze.offers",), chain="1", switch="KS_WRITE_INVENTORY"),
    "offer_stocks": TableFate(
        MOVED, KEYCRM, "Current stock per offer — the base every movement is "
        "a delta against, so it moves in the same chain as the movements.",
        successors=("bronze.offer_stocks",), chain="1",
        switch="KS_WRITE_INVENTORY"),
    "stock_movements": TableFate(
        MOVED, IRREPLACEABLE, "The only record that a quantity ever changed; "
        "a movement not written when it happened is gone.",
        successors=("app.stock_movements",), chain="1",
        switch="KS_WRITE_INVENTORY"),
    "sku_inventory_status": TableFate(
        MOVED, IRREPLACEABLE, "Rebuilt whole each tick, but first_seen_at is "
        "carried forward out of its own previous contents.",
        successors=("app.sku_inventory_status",), chain="1",
        switch="KS_WRITE_INVENTORY"),
    "inventory_sku_history": TableFate(
        MOVED, IRREPLACEABLE, "Per-SKU daily snapshot since 2026-01-27; a day "
        "not photographed cannot be photographed later.",
        successors=("app.inventory_sku_history",), chain="1",
        switch="KS_WRITE_INVENTORY"),
    "inventory_history": TableFate(
        MOVED, IRREPLACEABLE, "The daily snapshot rolled up; same reason as "
        "the per-SKU one.",
        successors=("app.inventory_history",), chain="1",
        switch="KS_WRITE_INVENTORY"),

    # ── chain 2 / step 13: the warehouse (KS_WRITE_WAREHOUSE) ───────────────
    "silver_orders": TableFate(
        DERIVED, COMPUTED, "Silver; Postgres derives silver.orders from its "
        "own landing and ClickHouse holds a copy of that.",
        successors=("silver.orders", "clickhouse:silver.orders"), chain="2",
        switch=WAREHOUSE, compaction=SKIPPED),
    "gold_daily_revenue": TableFate(
        DERIVED, COMPUTED, "Gold; derived in Postgres from silver.orders and "
        "in ClickHouse from its own silver.",
        successors=("gold.daily_revenue", "clickhouse:gold.daily_revenue"),
        chain="2", switch=WAREHOUSE, compaction=SKIPPED),
    "silver_order_utm": TableFate(
        DERIVED, COMPUTED, "UTM verdicts, re-parsable from "
        "orders.manager_comment; Postgres parses bronze.orders itself under "
        "KS_UTM_PARSE=postgres.",
        successors=("silver.order_utm",), chain="2", switch=WAREHOUSE,
        compaction=SKIPPED),
    "silver_order_lines": _view(
        "silver.order_lines", "The order-line level; one body for both "
        "engines, Postgres rendering frozen by revision 0011."),
    "v_sku_analysis": _view("gold.v_sku_analysis", _INVENTORY_VIEW),
    "v_category_velocity": _view("gold.v_category_velocity", _INVENTORY_VIEW),
    "v_sku_status": _view("gold.v_sku_status", _INVENTORY_VIEW),
    "v_inventory_summary": _view("gold.v_inventory_summary", _INVENTORY_VIEW),
    "v_aging_buckets": _view("gold.v_aging_buckets", _INVENTORY_VIEW),
    "v_sku_sell_through": _view("gold.v_sku_sell_through", _INVENTORY_VIEW),
    "v_abc_classification": _view("gold.v_abc_classification",
                                  _INVENTORY_VIEW),
    "v_abc_summary": _view("gold.v_abc_summary", _INVENTORY_VIEW),
    "v_recommended_actions": _view("gold.v_recommended_actions",
                                   _INVENTORY_VIEW),
    "v_restock_alerts": _view("gold.v_restock_alerts", _INVENTORY_VIEW),
    "v_sku_dead_stock_v2": _view("gold.v_sku_dead_stock_v2", _INVENTORY_VIEW),

    # ── chain 3: orders and order-level expenses (KS_WRITE_ORDERS, registered)
    "orders": TableFate(
        MOVED, KEYCRM, "Order headers; mirrored to bronze.orders from the same "
        "parse, with the version archive captured beside it in Postgres.",
        successors=("bronze.orders",), chain="3", switch="KS_WRITE_ORDERS"),
    "order_products": TableFate(
        MOVED, KEYCRM, "Line items; moves with the headers as one unit.",
        successors=("bronze.order_products",), chain="3",
        switch="KS_WRITE_ORDERS"),
    "expenses": TableFate(
        MOVED, KEYCRM, "Order-level expenses from include=expenses; written "
        "with the orders they belong to, in one transaction.",
        successors=("bronze.expenses",), chain="3", switch="KS_WRITE_ORDERS"),
    "order_backfill_misses": TableFate(
        MOVED, IRREPLACEABLE, "Ids KeyCRM could not supply — the one fact an "
        "API can never be asked for.",
        successors=("app.order_backfill_misses",), chain="3",
        switch="KS_WRITE_ORDERS"),

    # ── chain 4: buyers and gender (KS_WRITE_BUYERS, registered) ────────────
    "buyers": TableFate(
        MOVED, KEYCRM, "Buyers; one parse feeds both stores.",
        successors=("bronze.buyers",), chain="4", switch="KS_WRITE_BUYERS"),
    "buyer_contacts": TableFate(
        MOVED, KEYCRM, "Moves with buyers: one writer, one transaction.",
        successors=("bronze.buyer_contacts",), chain="4",
        switch="KS_WRITE_BUYERS"),
    "buyer_gender": TableFate(
        MOVED, IRREPLACEABLE, "Inferred from names, but override_by_human "
        "rows are a person's decision and no backfill touches them.",
        successors=("app.buyer_gender",), chain="4",
        switch="KS_WRITE_BUYERS"),

    # ── chain 5: manager classification (KS_WRITE_MANAGERS, registered) ─────
    "managers": TableFate(
        MOVED, IRREPLACEABLE, "KeyCRM names the managers, but is_retail is "
        "seeded once and then only a human sets it.",
        successors=("bronze.managers",), chain="5", switch="KS_WRITE_MANAGERS"),
    "manager_classifications": TableFate(
        MOVED, IRREPLACEABLE, "Human-authored retail intervals; sales_type is "
        "decided from them.",
        successors=("app.manager_classifications",), chain="5",
        switch="KS_WRITE_MANAGERS"),

    # ── chain 6: catalogue (6 and 6a registered) ───────────────────────────
    "products": TableFate(
        MOVED, KEYCRM, "The product catalogue; mirrored whole each hour. "
        "Product 1055 is DuckDB-only and is carried over (OD-15).",
        successors=("bronze.products",), chain="6",
        switch="KS_WRITE_CATALOGUE"),
    "categories": TableFate(
        MOVED, KEYCRM, "Categories; shipped by the weekly full sync.",
        successors=("bronze.categories",), chain="6",
        switch="KS_WRITE_CATALOGUE"),
    "expense_types": TableFate(
        MOVED, KEYCRM, "The 27-row dictionary /expenses names its costs by; "
        "KeyCRM serves it only to the weekly full sync.",
        successors=("bronze.expense_types",), chain="6a",
        switch="KS_WRITE_EXPENSE_TYPES"),

    # ── chain 7: goals and the forecast (7a and 7b-3 registered) ────────────
    "revenue_goals": TableFate(
        MOVED, IRREPLACEABLE, "The three goal amounts a human types on /goals.",
        successors=("app.revenue_goals",), chain="7a", switch="KS_WRITE_GOALS"),
    "seasonal_indices": TableFate(
        DERIVED, COMPUTED, "Computed from Silver by the goal calculators "
        "(chain 7b ported them onto Postgres Silver, OD-14); chain 7b-3 moves "
        "their writer, the three goal tables in one transaction.",
        successors=("app.seasonal_indices",), chain="7b-3",
        switch="KS_WRITE_FORECAST"),
    "weekly_patterns": TableFate(
        DERIVED, COMPUTED, "Computed by the goal calculators; written with "
        "the seasonal indices, chain 7b-3.",
        successors=("app.weekly_patterns",), chain="7b-3",
        switch="KS_WRITE_FORECAST"),
    "growth_metrics": TableFate(
        DERIVED, COMPUTED, "Computed by the goal calculators; written with "
        "the seasonal indices, chain 7b-3.",
        successors=("app.growth_metrics",), chain="7b-3",
        switch="KS_WRITE_FORECAST"),
    "revenue_predictions": TableFate(
        DERIVED, COMPUTED, "The model's forecast, written after each "
        "training; chain 7b-3, one owner set with the goal tables.",
        successors=("app.revenue_predictions",), chain="7b-3",
        switch="KS_WRITE_FORECAST"),

    # ── chain 8: manual expenses (KS_WRITE_EXPENSES, registered) ────────────
    "manual_expenses": TableFate(
        MOVED, IRREPLACEABLE, "Money a human typed; nothing re-derives it.",
        successors=("app.manual_expenses",), chain="8",
        switch="KS_WRITE_EXPENSES"),

    # ── chain 9: the quality journal and the forensic trail ─────────────────
    # The journal was a SHADOW chain (OD-02 (c)): under KS_WRITE_DQ_JOURNAL
    # Postgres wrote first and DuckDB was still handed every row, until step
    # 14's S14-10 deleted DuckDB's half.
    "data_quality_runs": TableFate(
        MOVED, IRREPLACEABLE, "The journal every check verdict, layer age and "
        "digest delta is read from.",
        successors=("app.data_quality_runs",), chain="9",
        switch="KS_WRITE_DQ_JOURNAL"),
    "data_quality_issues": TableFate(
        MOVED, IRREPLACEABLE, "Findings of a run; one id space with the runs.",
        successors=("app.data_quality_issues",), chain="9",
        switch="KS_WRITE_DQ_JOURNAL"),
    "data_quality_diffs": TableFate(
        MOVED, IRREPLACEABLE, "Diffs of a run; one id space with the runs.",
        successors=("app.data_quality_diffs",), chain="9",
        switch="KS_WRITE_DQ_JOURNAL"),
    "warehouse_refreshes": TableFate(
        ARCHIVE_ONLY, IRREPLACEABLE, "The forensic trail of DuckDB's own "
        "rebuilds; frozen rather than ported (OD-13), and it stops growing "
        "when DuckDB stops deriving.",
        successors=("app.warehouse_refreshes",), chain="9", switch=WAREHOUSE),
    "reconciliation_log": TableFate(
        ARCHIVE_ONLY, IRREPLACEABLE, "The legacy 06:00 comparator's log; its "
        "writer was retired by OD-10 on 2026-09-30, so it is history.",
        successors=("app.reconciliation_log",), chain="9"),

    # ── chain 10: watchdog samples (KS_WRITE_WATCHDOGS, registered, shadow) ─
    # The reasons below are the manifest's literal (S14-18 freezes it); the
    # shadow they name went with step 14's S14-10.
    "disk_samples": TableFate(
        MOVED, IRREPLACEABLE, "Bounded samples the disk watchdog differences "
        "against; chain 10 moves the writer to app with the reads beside it, "
        "and shadows each insert and prune into DuckDB (OD-02 (c)).",
        successors=("app.disk_samples",), chain="10",
        switch="KS_WRITE_WATCHDOGS"),
    "data_dir_samples": TableFate(
        MOVED, IRREPLACEABLE, "Per-group samples behind the 168 h data-dir "
        "baseline; chain 10, as disk_samples.",
        successors=("app.data_dir_samples",), chain="10",
        switch="KS_WRITE_WATCHDOGS"),
    "memory_samples": TableFate(
        MOVED, IRREPLACEABLE, "Memory samples; the only memory of an OOM kill "
        "across a container recreate. Chain 10, as disk_samples.",
        successors=("app.memory_samples",), chain="10",
        switch="KS_WRITE_WATCHDOGS"),

    # ── chain 11: the two send ledgers (11a and 11b registered, shadow) ─────
    "weekly_report_sends": TableFate(
        MOVED, IRREPLACEABLE, "Which weeks the sales report delivered; a lost "
        "row sends the same week again.",
        successors=("app.weekly_report_sends",), chain="11a",
        switch="KS_WRITE_WEEKLY_LEDGER"),
    "traffic_report_sends": TableFate(
        MOVED, IRREPLACEABLE, "The traffic report's own ledger.",
        successors=("app.traffic_report_sends",), chain="11b",
        switch="KS_WRITE_TRAFFIC_LEDGER"),

    # ── one table, three key families ───────────────────────────────────────
    "sync_metadata": TableFate(
        MOVED, IRREPLACEABLE, "Split by key, not moved as a table: each "
        "last_sync_* key follows the chain that declares it "
        "(write_chains.chain_for_sync_key) into meta.chain_watermarks; "
        "warehouse_dirty retires with step 13; the digest's marker goes with "
        "chain 9 (pg_dq_journal_write.CHAIN_MARKER_KEYS).",
        successors=("app.sync_metadata", "meta.chain_watermarks")),

    # ── moved before stage 4 by a store switch ──────────────────────────────
    "users": TableFate(
        MOVED, IRREPLACEABLE, "The dashboard's user list; KS_USER_STORE moved "
        "its writer on 2026-09-07 and this copy is frozen since.",
        successors=("app.dashboard_users",), switch=USER_STORE),
    "role_permissions": TableFate(
        MOVED, IRREPLACEABLE, "The role matrix; read and written through "
        "_perms_run, which follows the same switch (revision 0022).",
        successors=("app.role_permissions",), switch=USER_STORE),
    "sms_campaigns": TableFate(
        MOVED, IRREPLACEABLE, "Campaigns; KS_SMS_STORE moved /sms on "
        "2026-09-06.",
        successors=("app.sms_campaigns",), switch=SMS_STORE),
    "sms_campaign_members": TableFate(
        MOVED, IRREPLACEABLE, "The frozen roster of each campaign.",
        successors=("app.sms_campaign_members",), switch=SMS_STORE),
    "sms_dlr_events": TableFate(
        MOVED, IRREPLACEABLE, "Delivery reports; nothing re-sends them.",
        successors=("app.sms_dlr_events",), switch=SMS_STORE),
    "marketing_optouts": TableFate(
        MOVED, IRREPLACEABLE, "A customer's refusal; must never be lost.",
        successors=("app.marketing_optouts",), switch=SMS_STORE),
    "sms_audience_presets": TableFate(
        MOVED, IRREPLACEABLE, "Saved audiences a human named.",
        successors=("app.sms_audience_presets",), switch=SMS_STORE),

    # ── chain 12: the file's own bookkeeping ────────────────────────────────
    "schema_migrations": TableFate(
        RETIRED, NOTHING, "DuckDB's migration ledger: it records which steps "
        "ran on this file and means nothing once the file is gone; Postgres "
        "has alembic_version.", chain="12"),

    # ── a migration's scratch: present only if that migration died ──────────
    "order_products_backup": TableFate(
        RETIRED, KEYCRM, "Migration 0007's copy of order_products, dropped by "
        "the same step.", ddl=SCRATCH),
    "expenses_backup": TableFate(
        RETIRED, KEYCRM, "Migration 0008's copy of expenses, dropped by the "
        "same step.", ddl=SCRATCH),
    "order_products_new": TableFate(
        RETIRED, KEYCRM, "Migration 0011's copy of order_products, dropped by "
        "the same step.", ddl=SCRATCH),
    "_offer_stocks_old": TableFate(
        RETIRED, KEYCRM, "Migration 0012 renames offer_stocks to this and "
        "drops it once the new table is filled.", ddl=SCRATCH),

    # ── no DDL left; the production file still holds them ───────────────────
    "gold_product_pairs": TableFate(
        RETIRED, COMPUTED, "A Gold layer whose DDL and readers are gone.",
        ddl=NONE, compaction=SKIPPED),
    "orders_v2": TableFate(
        RETIRED, NOTHING, "An empty table from an abandoned rewrite.",
        ddl=NONE, compaction=SKIPPED),
    "gold_daily_traffic": TableFate(
        RETIRED, COMPUTED, "/traffic reads silver_orders with silver_order_utm "
        "on both engines; this layer was left with no reader.",
        ddl=NONE, compaction=SKIPPED),
    "gold_daily_products": TableFate(
        RETIRED, COMPUTED, "Reproduced to the kopeck by the order-lines level, "
        "which its readers moved to.",
        ddl=NONE, compaction=SKIPPED),
    "bronze_order_events": TableFate(
        RETIRED, NOTHING, "The H3 staging-merge log, retired 2026-09-14; empty "
        "since 2026-05-19.",
        ddl=NONE, compaction=SKIPPED),
}
