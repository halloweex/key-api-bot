"""What becomes of every DuckDB table and view — one declaration, walked.

Stage 5 deletes DuckDB, and its preconditions open with this sentence: "a
table-fate manifest covering every DuckDB table, and a test walks
`_init_schema` and a live file against it". Chain 12 needs the same thing as
a gate: a chain that moves a table must leave the schema, the weekly
compaction's exclusion list and the off-site archive saying the same thing
about it, because the Sunday compaction rebuilds the file from today's code
and ships whatever it exported — a table dropped from the DDL there is a DROP,
and a table added to `DERIVED_TABLES` stops travelling off-site (OD-11).

So every name the code can put into `analytics.duckdb`, and every name the
production file still holds after its DDL went, has one entry here, and
`tests/unit/test_duckdb_table_fates.py` derives the set from the code rather
than trusting this file: a fresh `DuckDBStore.connect()` (the schema, every
migration, every view), an AST walk of every `CREATE TABLE` / `CREATE VIEW` /
`RENAME TO` literal in DuckDB code, and the compaction's own list. It then
checks each entry against the lists that already decide its fate —
`DERIVED_TABLES`, the snapshot validator's tiers, the real `phase1_export`,
the DuckDB→Postgres pairings every shipper and comparison already states, and
the registered write chains.

NOTHING READS THIS IN PRODUCTION. It is data, two pure functions and one
reader of switches that already exist; `deploy/duckdb_table_fates_check.py`
runs it against a copy of the production file by hand. Adding it moved no
table, flag, list or revision.

WHAT AN ENTRY SAYS

- `kind` — MOVED (a writer moves and the rows are carried to `successors`),
  DERIVED (recomputed from Postgres, or a view Postgres renders itself),
  ARCHIVE_ONLY (history, frozen where it stands — the Postgres copy and the
  off-site archive — and nothing writes it again), RETIRED (no successor; the
  `reason` says why).
- `switch` — the variable that moves the writes: a registered chain's
  `KS_WRITE_*`, `KS_SMS_STORE`, `KS_USER_STORE` or `KS_WRITE_WAREHOUSE`. None
  means no switch is built yet — the chain map names the chain, DuckDB still
  writes — or, for a view or a retired table, that nothing writes at all. When
  a chain is registered (`core.write_chains.WRITE_CHAINS`) the test requires
  its tables to carry its variable, so a chain cannot land without saying here
  which tables it took.
- `origin` — what could rebuild the rows if every copy were lost: KeyCRM, a
  computation over other tables, or nothing (IRREPLACEABLE). An irreplaceable
  table must travel off-site; that is chain 12's largest blast radius.
- `ddl` — SCHEMA (a fresh file has it), SCRATCH (a migration makes and drops
  it inside one step; a file holds one only if that step died), NONE (no DDL
  left; the production file still holds it, which is why the compaction names
  it).
- `compaction` — what `scripts/compact_duckdb.py` does: EXPORTED (to Parquet,
  which is also the off-site archive), SKIPPED (`DERIVED_TABLES`), or
  NOT_A_TABLE (views; the export reads base tables only).

"Which tables are still DuckDB-written today?" is `duckdb_written()`.
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

# The switches that are not a registered write chain. Each has exactly one
# reader in the code, and `duckdb_written` asks that reader rather than the
# environment, so a latch, a cached mode or a refusal to read a typo is the
# answer here too.
SMS_STORE = "KS_SMS_STORE"
USER_STORE = "KS_USER_STORE"
WAREHOUSE = "KS_WRITE_WAREHOUSE"
STORE_SWITCHES = (SMS_STORE, USER_STORE, WAREHOUSE)

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

    # ── chain 3: orders and order-level expenses (no switch yet) ────────────
    "orders": TableFate(
        MOVED, KEYCRM, "Order headers; mirrored to bronze.orders from the same "
        "parse, with the version archive captured beside it in Postgres.",
        successors=("bronze.orders",), chain="3"),
    "order_products": TableFate(
        MOVED, KEYCRM, "Line items; moves with the headers as one unit.",
        successors=("bronze.order_products",), chain="3"),
    "expenses": TableFate(
        MOVED, KEYCRM, "Order-level expenses from include=expenses; mirrored "
        "to bronze.expenses.",
        successors=("bronze.expenses",), chain="3"),
    "order_backfill_misses": TableFate(
        MOVED, IRREPLACEABLE, "Ids KeyCRM could not supply — the one fact an "
        "API can never be asked for.",
        successors=("app.order_backfill_misses",), chain="3"),

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

    # ── chain 5: manager classification (no switch yet) ─────────────────────
    "managers": TableFate(
        MOVED, IRREPLACEABLE, "KeyCRM names the managers, but is_retail is "
        "seeded once and then only a human sets it.",
        successors=("bronze.managers",), chain="5"),
    "manager_classifications": TableFate(
        MOVED, IRREPLACEABLE, "Human-authored retail intervals; sales_type is "
        "decided from them.",
        successors=("app.manager_classifications",), chain="5"),

    # ── chain 6: catalogue (6a registered; products/categories no switch) ───
    "products": TableFate(
        MOVED, KEYCRM, "The product catalogue; mirrored whole each hour. "
        "Product 1055 is DuckDB-only and is carried over (OD-15).",
        successors=("bronze.products",), chain="6"),
    "categories": TableFate(
        MOVED, KEYCRM, "Categories; shipped by the weekly full sync.",
        successors=("bronze.categories",), chain="6"),
    "expense_types": TableFate(
        MOVED, KEYCRM, "The 27-row dictionary /expenses names its costs by; "
        "KeyCRM serves it only to the weekly full sync.",
        successors=("bronze.expense_types",), chain="6a",
        switch="KS_WRITE_EXPENSE_TYPES"),

    # ── chain 7: goals and the forecast (7a registered; 7b no switch) ───────
    "revenue_goals": TableFate(
        MOVED, IRREPLACEABLE, "The three goal amounts a human types on /goals.",
        successors=("app.revenue_goals",), chain="7a", switch="KS_WRITE_GOALS"),
    "seasonal_indices": TableFate(
        DERIVED, COMPUTED, "Computed from Silver by the goal calculators; "
        "chain 7b ports them onto Postgres Silver (OD-14).",
        successors=("app.seasonal_indices",), chain="7b"),
    "weekly_patterns": TableFate(
        DERIVED, COMPUTED, "Computed by the goal calculators; chain 7b.",
        successors=("app.weekly_patterns",), chain="7b"),
    "growth_metrics": TableFate(
        DERIVED, COMPUTED, "Computed by the goal calculators; chain 7b.",
        successors=("app.growth_metrics",), chain="7b"),
    "revenue_predictions": TableFate(
        DERIVED, COMPUTED, "The model's forecast, retrained daily; chain 7b.",
        successors=("app.revenue_predictions",), chain="7b"),

    # ── chain 8: manual expenses (KS_WRITE_EXPENSES, registered) ────────────
    "manual_expenses": TableFate(
        MOVED, IRREPLACEABLE, "Money a human typed; nothing re-derives it.",
        successors=("app.manual_expenses",), chain="8",
        switch="KS_WRITE_EXPENSES"),

    # ── chain 9: the quality journal and the forensic trail ─────────────────
    "data_quality_runs": TableFate(
        MOVED, IRREPLACEABLE, "The journal every check verdict, layer age and "
        "digest delta is read from.",
        successors=("app.data_quality_runs",), chain="9"),
    "data_quality_issues": TableFate(
        MOVED, IRREPLACEABLE, "Findings of a run; one id space with the runs.",
        successors=("app.data_quality_issues",), chain="9"),
    "data_quality_diffs": TableFate(
        MOVED, IRREPLACEABLE, "Diffs of a run; one id space with the runs.",
        successors=("app.data_quality_diffs",), chain="9"),
    "warehouse_refreshes": TableFate(
        ARCHIVE_ONLY, IRREPLACEABLE, "The forensic trail of DuckDB's own "
        "rebuilds; frozen rather than ported (OD-13), and it stops growing "
        "when DuckDB stops deriving.",
        successors=("app.warehouse_refreshes",), chain="9", switch=WAREHOUSE),
    "reconciliation_log": TableFate(
        ARCHIVE_ONLY, IRREPLACEABLE, "The legacy 06:00 comparator's log; its "
        "writer was retired by OD-10 on 2026-09-30, so it is history.",
        successors=("app.reconciliation_log",), chain="9"),

    # ── chain 10: watchdog samples (no switch yet) ──────────────────────────
    "disk_samples": TableFate(
        MOVED, IRREPLACEABLE, "Bounded samples the disk watchdog differences "
        "against; chain 10 moves the writer (where to is OD-16's), and the "
        "hourly copy in app holds the history.",
        successors=("app.disk_samples",), chain="10"),
    "data_dir_samples": TableFate(
        MOVED, IRREPLACEABLE, "Per-group samples behind the 168 h data-dir "
        "baseline; chain 10, as disk_samples.",
        successors=("app.data_dir_samples",), chain="10"),
    "memory_samples": TableFate(
        MOVED, IRREPLACEABLE, "Memory samples; the only memory of an OOM kill "
        "across a container recreate. Chain 10, as disk_samples.",
        successors=("app.memory_samples",), chain="10"),

    # ── chain 11: the two send ledgers (no switch yet) ──────────────────────
    "weekly_report_sends": TableFate(
        MOVED, IRREPLACEABLE, "Which weeks the sales report delivered; a lost "
        "row sends the same week again.",
        successors=("app.weekly_report_sends",), chain="11"),
    "traffic_report_sends": TableFate(
        MOVED, IRREPLACEABLE, "The traffic report's own ledger.",
        successors=("app.traffic_report_sends",), chain="11"),

    # ── one table, three key families ───────────────────────────────────────
    "sync_metadata": TableFate(
        MOVED, IRREPLACEABLE, "Split by key, not moved as a table: each "
        "last_sync_* key follows the chain that declares it "
        "(write_chains.chain_for_sync_key) into meta.chain_watermarks; "
        "warehouse_dirty retires with step 13; dq_digest_last_sent goes with "
        "chain 9.",
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


def tables_by_switch(switch: str) -> Tuple[str, ...]:
    return tuple(sorted(n for n, f in FATES.items() if f.switch == switch))


def _store_switch_writes_duckdb(switch: str) -> Optional[bool]:
    """The one reader each store switch has. None when it cannot answer — a
    value the reader refuses, which is the reader's own rule for a typo."""
    try:
        if switch == SMS_STORE:
            from core.pg_sms import sms_store_is_postgres
            return not sms_store_is_postgres()
        if switch == USER_STORE:
            from core.pg_dashboard_users import user_store_is_postgres
            return not user_store_is_postgres()
        if switch == WAREHOUSE:
            from core import warehouse_cutover
            if warehouse_cutover.value() is None:
                # `configure_modes()` has not run in this process, and only
                # web's does: a script, a test, or anything reading this from
                # outside web would otherwise answer "duckdb" whatever the
                # variable says. Not knowing is the answer here.
                return None
            return warehouse_cutover.duckdb_derives()
    except ValueError:
        return None
    raise KeyError(f"{switch} is not a switch this manifest knows how to read")


def duckdb_written() -> Dict[str, Optional[bool]]:
    """`{table: True | False | None}` — does DuckDB still write it, in this
    process, now. None is "cannot tell from here".

    A registered chain answers through `core.write_chains.chain_modes()`, so a
    latch counts exactly as it does for the writers (OD-19 (a)), and a flag
    nobody can read is None: its writers raise rather than pick a store. A
    store switch asks its own reader; `KS_WRITE_WAREHOUSE` is the mode this
    process configured, and None in a process where `configure_modes()` never
    ran — every process but web's. With no switch, DuckDB writes every table
    in its schema except a frozen archive; nothing writes a view, a table
    with no DDL left, or a migration's scratch outside its own step.

    `kind` is the fate at stage 5, not today's state: `schema_migrations` is
    retired with the file, and DuckDB writes it on every connect that applies
    a migration until then. The test holds this answer to the writers the
    code has, not to `kind`.
    """
    from core import write_chains

    modes = write_chains.chain_modes()
    chain_mode = {}
    for chain in write_chains.WRITE_CHAINS:
        mode = modes[write_chains.chain_name(chain)]["mode"]
        chain_mode[chain.WRITE_ENV] = None if mode is None else mode == "duckdb"

    out: Dict[str, Optional[bool]] = {}
    for name, fate in FATES.items():
        if fate.object == VIEW or fate.ddl != SCHEMA:
            out[name] = False
        elif fate.switch is None:
            out[name] = fate.kind != ARCHIVE_ONLY
        elif fate.switch in chain_mode:
            out[name] = chain_mode[fate.switch]
        else:
            out[name] = _store_switch_writes_duckdb(fate.switch)
    return out
