"""The batch-E chains' lock on older images — twenty table comments and nothing else.

Revision ID: 0035_batch_e_chains
Revises: 0034_buyer_chain
Created: 2026-10-09

WHY A REVISION WITH NO SCHEMA IN IT

#280 registered eight write chains at once, all off: 3 (`pg_orders_write`),
5 (`pg_managers_write`), 6 (`pg_catalogue_write`), 7b-3 (`pg_forecast_write`)
and the shadow chains 9, 10, 11a and 11b (`pg_dq_journal_write`,
`pg_watchdog_write`, `pg_weekly_ledger_write`, `pg_traffic_ledger_write`). It
shipped no revision, so a database at 0034 still admits the images built
between chain 4's lock and #280 — 3.0.263 to 3.0.270, image numbers as
`/app/VERSION` and the Docker Hub tag read them, each from `git show
<merge>:VERSION` (the git tag of the same code is usually one higher; #273 and
#274 were both built as 3.0.259) — and none of those knows any of these
chains.

`REQUIRED_REVISION` is the lock, as it was for chain 4 (0034, owner decision
16): every gated path refuses a database at a revision other than its own. One
revision for all eight, because the bump locks out every older image whatever
tables it comments — eight revisions would add seven deploys and seven windows
in which an image-only rollback needs a downgrade, and lock nothing more. It
goes out directly before the first of these flips, not earlier: from the day
it lands, rolling back an image below it for any reason at all takes the
downgrade below.

From the build that carries it, the order is held at runtime: each of the
eight names this revision as its `LOCKOUT_REVISION`, and its flag moves
nothing in a build that requires an earlier one
(`core.chain_latch.lockout_unmet`); its writers pass `require_revision()`
before the latch, so the database is here too. The builds before have no such
rule — 3.0.271 and 3.0.272 carry the eight chains and require 0034, and in
them a flag latches its chain at the first write — so none of the eight flags
may be set until the database says `0035_batch_e_chains`.

WHAT IT CHANGES FOR AN IMAGE OLDER THAN THE CHAINS

In 3.0.263 to 3.0.270 every gated path that writes one of these tables already
stands down on an owner row, read as itself even where no chain in the build
declares the table (DN-22a, DN-22b, both older than 0034): the operational copy
(chain 3's misses ledger, 7b-3's four tables, the journal, the samples and both
ledgers), the classification copy (chain 5), and chain 3's order and expense
backfills, ids-diffs and comment ship. So after a latch the lock adds little on
those paths. What it adds is that such an image cannot run quietly at all — the
outage below — where without it the image would run beside a latched chain
without knowing it: writing DuckDB, shipping through the mirrors that never
gated, and reading each moved table out of DuckDB, where it stopped at the
flip.

What it does NOT hold, said where it is created:

- **The per-tick landing mirrors never gated.** `upsert_orders` →
  `mirror_orders` → `write_orders` (orders and line items) and
  `pg_landing._mirror` (expenses, products, categories) write without asking
  the revision — the alert journal is the third such writer — and the sync
  runs without a session. So a locked-out image run after chain 3's or chain
  6's flip still ships its own copy of what KeyCRM served over the chain's
  rows, writing round it. Chain 6 had said it needed no lock-out for that
  reason; these comments name its writer, and the lock holds none of it. What
  the lock changes there is the evidence: the comparisons that would file
  `owner_row_without_marker` and `order_owner_row_without_marker` are gated,
  so in such an image the 07:30 run fails on the revision instead of naming
  the overwrite.
- **A locked-out image still writes DuckDB.** Its sync, its quality checks,
  its watchdogs, its reports and its goal and forecast jobs write DuckDB before
  — or without — any Postgres call. After a flip those are rows the chain does
  not hold: the shadow chains file `shadow_duckdb_only_rows` for them, and the
  copy-back's handover reports them.
- **It is not "no writes" — it is an outage.** The session read raises under
  `KS_USER_STORE=postgres`, so the dashboard answers 401 to everybody, and the
  bot refuses to start, the canary with it. An image-only rollback below this
  revision therefore needs `alembic downgrade 0034_buyer_chain` first, run with
  the NEW migrate image (an older one does not know 0035), and then all THREE
  images moved back — keycrm-migrate with web and bot. Left new, `up -d`
  re-runs `alembic upgrade head` (web and bot wait on it) and puts 0035 back.
  And only before the first of these chains latches: after one has written
  Postgres, going below 0035 admits builds that do not know it, and the way
  back is `scripts/chain_copy_back.py <chain>`, never a downgrade.
- **It also locks out 3.0.271 and 3.0.272, which carry these chains** — every
  image built between #280 and this revision requires 0034. That is the cost
  of a bump, not a hazard: they would be safe on a 0034 database.

Every statement takes SHARE UPDATE EXCLUSIVE on its table (measured on
PostgreSQL 17.2, `pg_locks` inside the transaction): reads and writes carry on
— every full replace of these tables is a DELETE, not a TRUNCATE — and the
revision waits only behind a manual VACUUM or ANALYZE, an index build or other
DDL on one of the twenty (Postgres cancels an ordinary autovacuum that blocks
it), holding the locks it has taken until it commits.

The comments say who writes each table now. The downgrade puts back exactly
what 0002, 0003, 0005, 0008 and 0020 wrote; the twelve tables that never had a
comment are cleared rather than given an invented past.
"""
from alembic import op

revision = "0035_batch_e_chains"
down_revision = "0034_buyer_chain"
branch_labels = None
depends_on = None

_REPLICATED = ("otherwise replicated from DuckDB, full replace "
               "(core/pg_operational.py).")


def _shadow(what: str, env: str, chain: str, module: str, row: str) -> str:
    return (f"{what} Under {env}=postgres ({chain}) written here first by "
            f"core/{module}.py, and DuckDB is then handed the same {row} as a "
            f"shadow; {_REPLICATED}")


# {table: the text 0035 writes}. Plain text: `_comment` doubles the quote the
# SQL literal needs — several of these carry an apostrophe, and revision 0009 died
# on a real server over exactly one.
COMMENTS = {
    # Chain 3: KS_WRITE_ORDERS.
    "bronze.orders": (
        "Orders as KeyCRM serves them. Under KS_WRITE_ORDERS=postgres "
        "(chain 3) written here by core/pg_orders_write.py and nowhere else; "
        "otherwise the mirror of DuckDB orders (core/pg_landing.py). Both run "
        "pg_landing._write_order_rows, which archives every change in "
        "app.order_versions."
    ),
    "bronze.order_products": (
        "Order line items, replaced per order and never upserted: the ids are "
        "positional. Written with their order by whichever writer holds "
        "bronze.orders: core/pg_orders_write.py under KS_WRITE_ORDERS=postgres "
        "(chain 3), otherwise the mirror of DuckDB (core/pg_landing.py)."
    ),
    "bronze.expenses": (
        "Order-level costs from KeyCRM include=expenses. Under "
        "KS_WRITE_ORDERS=postgres (chain 3) written here by "
        "core/pg_orders_write.py, in the transaction that writes their orders; "
        "otherwise mirrored from the same parsed tuple DuckDB receives "
        "(core/pg_landing.py)."
    ),
    "app.order_backfill_misses": (
        "Ids KeyCRM could not supply, or supplied without line items: the one "
        "fact an API cannot be asked for. Under KS_WRITE_ORDERS=postgres "
        "(chain 3) written here by core/pg_orders_write.py; " + _REPLICATED
    ),
    # Chain 5: KS_WRITE_MANAGERS.
    "bronze.managers": (
        "KeyCRM managers with is_retail, a human decision KeyCRM cannot "
        "supply. Under KS_WRITE_MANAGERS=postgres (chain 5) written here by "
        "core/pg_managers_write.py and nowhere else; otherwise replicated from "
        "DuckDB, full replace (core/pg_replication.py)."
    ),
    "app.manager_classifications": (
        "Effective-dated retail classification. Resolves sales_type as of the "
        "order date. Under KS_WRITE_MANAGERS=postgres (chain 5) written here by "
        "core/pg_managers_write.py under one advisory lock; otherwise "
        "replicated from DuckDB, full replace (core/pg_replication.py)."
    ),
    # Chain 6: KS_WRITE_CATALOGUE.
    "bronze.products": (
        "Products as KeyCRM serves them, parsed once by core.landing_rows. "
        "Under KS_WRITE_CATALOGUE=postgres (chain 6) written here by "
        "core/pg_catalogue_write.py and nowhere else, the whole catalogue each "
        "time; otherwise the mirror of DuckDB products (core/pg_landing.py). "
        "Nothing deletes a row: one the last whole write left out was retired "
        "by KeyCRM."
    ),
    "bronze.categories": (
        "Categories as KeyCRM serves them, parsed once by core.landing_rows. "
        "Under KS_WRITE_CATALOGUE=postgres (chain 6) written here by "
        "core/pg_catalogue_write.py and nowhere else, the whole tree each time; "
        "otherwise the mirror of DuckDB categories (core/pg_landing.py). "
        "Nothing deletes a row."
    ),
    # Chain 7b-3: KS_WRITE_FORECAST.
    "app.seasonal_indices": (
        "Monthly seasonal indices the goal calculators read. Under "
        "KS_WRITE_FORECAST=postgres (chain 7b-3) written here by "
        "core/pg_forecast_write.py, with the YoY updates in the same "
        "transaction; "
        + _REPLICATED
    ),
    "app.growth_metrics": (
        "Growth metrics the goal calculators read. Under "
        "KS_WRITE_FORECAST=postgres (chain 7b-3) written here by "
        "core/pg_forecast_write.py; " + _REPLICATED
    ),
    "app.weekly_patterns": (
        "Each week's share of a month's goal, for the milestone weeks. Under "
        "KS_WRITE_FORECAST=postgres (chain 7b-3) written here by "
        "core/pg_forecast_write.py; " + _REPLICATED
    ),
    "app.revenue_predictions": (
        "The revenue model's daily predictions, stored by every training. "
        "Under KS_WRITE_FORECAST=postgres (chain 7b-3) written here by "
        "core/pg_forecast_write.py; " + _REPLICATED
    ),
    # Chain 9: KS_WRITE_DQ_JOURNAL.
    "app.data_quality_runs": _shadow(
        "The quality journal, one row per check run, a failed run too.",
        "KS_WRITE_DQ_JOURNAL", "chain 9", "pg_dq_journal_write", "rows"),
    "app.data_quality_issues": _shadow(
        "The findings of a quality run.",
        "KS_WRITE_DQ_JOURNAL", "chain 9", "pg_dq_journal_write", "rows"),
    "app.data_quality_diffs": _shadow(
        "The per-cell differences of a quality run.",
        "KS_WRITE_DQ_JOURNAL", "chain 9", "pg_dq_journal_write", "rows"),
    # Chain 10: KS_WRITE_WATCHDOGS.
    "app.disk_samples": _shadow(
        "The disk watchdog's samples.",
        "KS_WRITE_WATCHDOGS", "chain 10", "pg_watchdog_write", "rows"),
    "app.data_dir_samples": _shadow(
        "The data directory watchdog's samples.",
        "KS_WRITE_WATCHDOGS", "chain 10", "pg_watchdog_write", "rows"),
    "app.memory_samples": _shadow(
        "The memory watchdog's samples, per container.",
        "KS_WRITE_WATCHDOGS", "chain 10", "pg_watchdog_write", "rows"),
    # Chains 11a and 11b: KS_WRITE_WEEKLY_LEDGER, KS_WRITE_TRAFFIC_LEDGER.
    "app.weekly_report_sends": _shadow(
        "One row per week the weekly sales report delivered.",
        "KS_WRITE_WEEKLY_LEDGER", "chain 11a", "pg_weekly_ledger_write", "row"),
    "app.traffic_report_sends": _shadow(
        "One row per week the weekly traffic report delivered.",
        "KS_WRITE_TRAFFIC_LEDGER", "chain 11b", "pg_traffic_ledger_write", "row"),
}

# {table: what was there before, verbatim, or None for no comment}.
BEFORE = {
    # 0003's text, verbatim.
    "bronze.orders": (
        "Mirror of DuckDB orders (step 05). DuckDB is the system of record "
        "until the read switches."
    ),
    "bronze.order_products": (
        "Mirror of DuckDB order_products (step 05). Line items are replaced "
        "per order, never upserted: the ids are positional."
    ),
    # 0020's.
    "bronze.expenses": (
        "Order-level costs from KeyCRM include=expenses, ~15k rows. Mirrored "
        "from the same parsed tuple DuckDB receives."
    ),
    # 0008's.
    "app.order_backfill_misses": (
        "Replicated from DuckDB. Ids KeyCRM could not supply — the one fact "
        "an API cannot be asked for. Full replace."
    ),
    # 0005's.
    "bronze.managers": (
        "Replicated from DuckDB, not parsed from KeyCRM: is_retail is a "
        "human decision KeyCRM cannot supply."
    ),
    "app.manager_classifications": (
        "Effective-dated retail classification. Resolves sales_type as of the "
        "order date. Replicated from DuckDB during the parallel period."
    ),
    # 0002's.
    "bronze.products": (
        "Mirror of DuckDB products (step 05). Landing from KeyCRM, "
        "rebuildable; DuckDB is the system of record until the read switches."
    ),
    "bronze.categories": (
        "Mirror of DuckDB categories (step 05). Landing from KeyCRM, "
        "rebuildable; DuckDB is the system of record until the read switches."
    ),
    # Never had one: 0025, 0026 and 0028 created these without a comment.
    "app.seasonal_indices": None,
    "app.growth_metrics": None,
    "app.weekly_patterns": None,
    "app.revenue_predictions": None,
    "app.data_quality_runs": None,
    "app.data_quality_issues": None,
    "app.data_quality_diffs": None,
    "app.disk_samples": None,
    "app.data_dir_samples": None,
    "app.memory_samples": None,
    "app.weekly_report_sends": None,
    "app.traffic_report_sends": None,
}


def _comment(table: str, text) -> str:
    if text is None:
        return f"COMMENT ON TABLE {table} IS NULL"
    return "COMMENT ON TABLE " + table + " IS '" + text.replace("'", "''") + "'"


def upgrade() -> None:
    for table, text in COMMENTS.items():
        op.execute(_comment(table, text))


def downgrade() -> None:
    for table, text in BEFORE.items():
        op.execute(_comment(table, text))
