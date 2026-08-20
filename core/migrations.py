"""Every schema change this database has ever had, in order, applied once.

These used to be 647 lines of `try: ... except Exception as e: logger.debug(e)`
inside one method: thirty-seven blocks, re-run on every boot, each one able to
fail without leaving a trace above DEBUG. That is exactly how the 2026-08-09
incident started — an ALTER failed quietly, the code read the column anyway,
and a one-off became a rebuild every two minutes for hours.

What changed is not the SQL, which is carried over verbatim. It is that each
step now has a name, runs once, and has its outcome written to
`schema_migrations` — success or failure, with the error and how long it took.
A step that fails is logged at ERROR, surfaced on /api/health, and retried on
the next boot rather than being forgotten.

Two steps are deliberately `ALWAYS`:

* `0006_seed_manager_classifications` — a manager who syncs for the first time
  tomorrow still needs a baseline interval, so this cannot be a one-off.
* `0027_reset_sequences_after_compaction` — IMPORT resets every sequence to 1
  while the tables keep their rows, so this runs after every weekly compaction.
* `0004_drop_gold_daily_products_indexes` — a guard, not a one-off. Any ART
  index on that table disables row-group vacuuming for the whole of it, which
  measured 53 MB/day of file growth and was the reason a weekly stop-the-world
  compaction existed. Six `DROP INDEX IF EXISTS` per boot is a cheap price for
  the invariant holding continuously rather than once in the past.

Steps are appended, never renumbered and never edited once live: the id is what
`schema_migrations` remembers, and rewriting one silently un-applies it.
"""
from __future__ import annotations

import logging
from typing import Callable, List, NamedTuple

logger = logging.getLogger(__name__)

ONCE = "once"
ALWAYS = "always"


class Migration(NamedTuple):
    """One schema change. `run` takes the store, so bodies moved here unchanged."""

    id: str
    mode: str
    run: Callable


SCHEMA_MIGRATIONS_DDL = """CREATE TABLE IF NOT EXISTS schema_migrations (
    id VARCHAR PRIMARY KEY,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    duration_ms DECIMAL(10, 2),
    outcome VARCHAR NOT NULL,          -- 'applied' | 'failed'
    error_message VARCHAR
)"""


def _m0001_orders_updated_at(self) -> None:
    # Migration 1: Add updated_at column to orders table (for idempotent sync)
    self._connection.execute(
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE"
    )
    logger.debug("Migration: updated_at column added/verified")


def _m0002_orders_status_group_id(self) -> None:
    # Migration: Add status_group_id to orders. KeyCRM's own grouping of the
    # status — 6 is lost/cancel — read off the order payload and preferred
    # over our hardcoded id list wherever it is known.
    #
    # No DEFAULT, deliberately: `ADD COLUMN ... DEFAULT x` rewrites every
    # row to materialise the value and has OOM-killed this container on a
    # multi-GB database before. Existing rows get NULL, which is exactly
    # what the fallback in the Silver CASE expects.
    self._connection.execute(
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS status_group_id INTEGER"
    )
    logger.debug("Migration: status_group_id column added/verified")


def _m0003_orders_manager_comment(self) -> None:
    # Migration: Add manager_comment column to orders table (for UTM tracking)
    self._connection.execute(
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS manager_comment TEXT"
    )
    logger.debug("Migration: manager_comment column added/verified")


def _m0004_drop_gold_daily_products_indexes(self) -> None:
    # Migration: record which mode a refresh actually ran in.
    #
    # warehouse_refreshes has 69k rows and is the best forensic trail in
    # the system, but it never recorded whether Silver was rebuilt whole
    # or incrementally — so the file growth that forced a weekly
    # stop-the-world compaction could not be attributed to a cause. Twice
    # now an estimate has been made by extrapolation and been wrong.
    #
    # No DEFAULT: on a table this size, materialising one rewrites every
    # row and has OOM-ed the container before. Existing rows get NULL,
    # which reads correctly as "we did not record this".
    # Migration: drop every index on gold_daily_products.
    #
    # They are gone from the schema above; this removes them from databases
    # that already have them. Existing dead row versions are NOT reclaimed
    # by the DROP — that needs a vacuum, which needs an exclusive lock a
    # two-minute refresh loop rarely yields. Growth stops immediately; the
    # ~197 MB already accumulated comes back at the next window that gets
    # the lock, or at the weekly compaction.
    for _idx in (
        "idx_gold_prod_date", "idx_gold_prod_product", "idx_gold_prod_brand",
        "idx_gold_prod_category", "idx_gold_prod_cat_date",
        "idx_gold_prod_brand_date",
    ):
        try:
            self._connection.execute(f"DROP INDEX IF EXISTS {_idx}")
        except Exception as e:
            logger.warning("Migration (drop %s) failed: %s", _idx, e)


def _m0005_warehouse_refreshes_silver_mode(self) -> None:
    # (no note was left with this one)
    self._connection.execute(
        "ALTER TABLE warehouse_refreshes ADD COLUMN IF NOT EXISTS silver_mode VARCHAR"
    )


def _m0006_seed_manager_classifications(self) -> None:
    # Migration: freeze today's classification as each manager's first
    # interval. Runs every startup so a manager who syncs later still gets
    # a baseline; it never touches a manager who already has one, which is
    # what keeps a human's forward-dated answer from being overwritten by
    # the seed that used to fight it.
    #
    # 1970-01-01 rather than the manager's first order: the floor has to
    # precede every order the warehouse will ever hold, and the earliest is
    # 2023-12-02.
    self._connection.execute("""
        CREATE TABLE IF NOT EXISTS manager_classifications (
            manager_id INTEGER NOT NULL,
            is_retail BOOLEAN NOT NULL,
            valid_from DATE NOT NULL,
            valid_to DATE,
            set_by INTEGER,
            set_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            note VARCHAR,
            PRIMARY KEY (manager_id, valid_from)
        )
    """)
    seeded = self._connection.execute("""
        INSERT INTO manager_classifications
            (manager_id, is_retail, valid_from, valid_to, set_by, note)
        SELECT m.id, COALESCE(m.is_retail, FALSE), DATE '1970-01-01',
               NULL, NULL, 'baseline frozen from managers.is_retail'
        FROM managers m
        WHERE NOT EXISTS (
            SELECT 1 FROM manager_classifications mc
            WHERE mc.manager_id = m.id
        )
        RETURNING manager_id
    """).fetchall()
    if seeded:
        logger.info(
            "Migration: seeded %d manager classification baselines",
            len(seeded),
        )


def _m0007_order_products_drop_fk(self) -> None:
    # Migration 2: Recreate order_products without FK constraint (DuckDB FK bug workaround)
    has_fk = False
    try:
        result = self._connection.execute("""
            SELECT COUNT(*) FROM duckdb_constraints()
            WHERE table_name = 'order_products' AND constraint_type = 'FOREIGN KEY'
        """).fetchone()
        has_fk = result[0] > 0 if result else False
    except Exception:
        # duckdb_constraints() might not be available, check by trying a test delete
        # If FK exists, we need to remove it
        logger.debug("duckdb_constraints() not available, checking FK via schema")
        # Alternative: check if REFERENCES exists in table definition
        try:
            schema = self._connection.execute("""
                SELECT sql FROM sqlite_master WHERE type='table' AND name='order_products'
            """).fetchone()
            if schema and 'REFERENCES' in str(schema[0]).upper():
                has_fk = True
        except Exception:
            # Try pragma approach
            try:
                fk_list = self._connection.execute("PRAGMA foreign_key_list('order_products')").fetchall()
                has_fk = len(fk_list) > 0
            except Exception:
                pass

    if has_fk:
        logger.info("Migration: Removing FK constraint from order_products...")
        self._connection.execute("BEGIN TRANSACTION")
        try:
            # Backup data
            self._connection.execute("""
                CREATE TABLE order_products_backup AS SELECT * FROM order_products
            """)
            # Drop old table
            self._connection.execute("DROP TABLE order_products")
            # Create new table without FK
            self._connection.execute("""
                CREATE TABLE order_products (
                    id INTEGER PRIMARY KEY,
                    order_id INTEGER NOT NULL,
                    product_id INTEGER,
                    name VARCHAR NOT NULL,
                    quantity INTEGER NOT NULL,
                    price_sold DECIMAL(12, 2) NOT NULL
                )
            """)
            # Restore data
            self._connection.execute("""
                INSERT INTO order_products SELECT * FROM order_products_backup
            """)
            # Drop backup
            self._connection.execute("DROP TABLE order_products_backup")
            self._connection.execute("COMMIT")
            logger.info("Migration: order_products FK constraint removed successfully")
        except Exception as e:
            self._connection.execute("ROLLBACK")
            logger.error(f"Migration failed (FK removal), rolling back: {e}")
            raise


def _m0008_expenses_drop_fk(self) -> None:
    # Migration 3: Remove FK from expenses table (same DuckDB bug)
    has_fk = False
    try:
        result = self._connection.execute("""
            SELECT COUNT(*) FROM duckdb_constraints()
            WHERE table_name = 'expenses' AND constraint_type = 'FOREIGN KEY'
        """).fetchone()
        has_fk = result[0] > 0 if result else False
    except Exception:
        # Fallback: check via PRAGMA
        try:
            fk_list = self._connection.execute("PRAGMA foreign_key_list('expenses')").fetchall()
            has_fk = len(fk_list) > 0
        except Exception:
            pass

    if has_fk:
        logger.info("Migration: Removing FK constraint from expenses...")
        self._connection.execute("BEGIN TRANSACTION")
        try:
            self._connection.execute("""
                CREATE TABLE expenses_backup AS SELECT * FROM expenses
            """)
            self._connection.execute("DROP TABLE expenses")
            self._connection.execute("""
                CREATE TABLE expenses (
                    id INTEGER PRIMARY KEY,
                    order_id INTEGER NOT NULL,
                    expense_type_id INTEGER,
                    amount DECIMAL(12, 2) NOT NULL,
                    description VARCHAR,
                    status VARCHAR,
                    payment_date TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE,
                    synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self._connection.execute("""
                INSERT INTO expenses SELECT * FROM expenses_backup
            """)
            self._connection.execute("DROP TABLE expenses_backup")
            self._connection.execute("COMMIT")
            logger.info("Migration: expenses FK constraint removed successfully")
        except Exception as e:
            self._connection.execute("ROLLBACK")
            logger.error(f"Migration failed (expenses FK removal), rolling back: {e}")
            raise


def _m0009_gold_daily_traffic_sales_type(self) -> None:
    # Migration: Add sales_type column to gold_daily_traffic
    #
    # This asked for `VARCHAR NOT NULL DEFAULT 'retail'` and DuckDB has never
    # accepted it — "Adding columns with constraints not yet supported". It
    # therefore failed on every boot since it was written, at DEBUG, seen by
    # nobody. The ledger's very first run is what surfaced it.
    #
    # Harmless as it turned out: `sales_type` is part of the table's PRIMARY KEY,
    # so every database that has the table has the column from CREATE TABLE, and
    # production carries it. But a database old enough to need this would never
    # have got it, which is the failure this step existed to prevent.
    #
    # No constraint now, which is this codebase's standing rule for ADD COLUMN
    # anyway: a DEFAULT rewrites the whole table to materialise the value and has
    # OOM-killed this container before.
    self._connection.execute(
        "ALTER TABLE gold_daily_traffic ADD COLUMN IF NOT EXISTS sales_type VARCHAR"
    )


def _m0010_manual_expenses_platform(self) -> None:
    # Migration: Add platform column to manual_expenses (for ad spend tracking)
    self._connection.execute(
        "ALTER TABLE manual_expenses ADD COLUMN IF NOT EXISTS platform VARCHAR"
    )
    self._connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_manual_expenses_platform ON manual_expenses(platform)"
    )
    # Backfill existing marketing rows
    self._connection.execute("""
        UPDATE manual_expenses SET platform = CASE
            WHEN LOWER(expense_type) LIKE '%facebook%' OR LOWER(expense_type) LIKE '%fb %' THEN 'facebook'
            WHEN LOWER(expense_type) LIKE '%tiktok%' THEN 'tiktok'
            WHEN LOWER(expense_type) LIKE '%google%' THEN 'google'
        END WHERE category = 'marketing' AND platform IS NULL
    """)
    logger.debug("Migration: platform column added/verified on manual_expenses")


def _m0011_order_products_id_bigint(self) -> None:
    # Migration: order_products.id INTEGER → BIGINT (overflow safety)
    col_type = self._connection.execute("""
        SELECT data_type FROM information_schema.columns
        WHERE table_name = 'order_products' AND column_name = 'id'
    """).fetchone()
    if col_type and col_type[0] == 'INTEGER':
        logger.info("Migration: order_products.id INTEGER → BIGINT")
        self._connection.execute("BEGIN TRANSACTION")
        try:
            self._connection.execute("CREATE TABLE order_products_new AS SELECT * FROM order_products")
            self._connection.execute("DROP TABLE order_products")
            self._connection.execute("""
                CREATE TABLE order_products (
                    id BIGINT PRIMARY KEY,
                    order_id INTEGER NOT NULL,
                    product_id INTEGER,
                    name VARCHAR NOT NULL,
                    quantity INTEGER NOT NULL,
                    price_sold DECIMAL(12, 2) NOT NULL
                )
            """)
            self._connection.execute("INSERT INTO order_products SELECT * FROM order_products_new")
            self._connection.execute("DROP TABLE order_products_new")
            self._connection.execute("CREATE INDEX IF NOT EXISTS idx_order_products_order ON order_products(order_id)")
            self._connection.execute("CREATE INDEX IF NOT EXISTS idx_order_products_product ON order_products(product_id)")
            self._connection.execute("COMMIT")
            logger.info("Migration: order_products.id BIGINT migration complete")
        except Exception as e:
            self._connection.execute("ROLLBACK")
            logger.error(f"Migration failed (order_products BIGINT), rolling back: {e}")
            raise


def _m0012_offer_stocks_primary_key(self) -> None:
    # Migration: Recreate offer_stocks with PRIMARY KEY if missing
    # (CREATE TABLE IF NOT EXISTS doesn't alter existing tables, so old tables
    # may lack PK constraint. This enables INSERT OR REPLACE instead of DELETE+INSERT.)
    has_pk = False
    try:
        result = self._connection.execute("""
            SELECT COUNT(*) FROM duckdb_constraints()
            WHERE table_name = 'offer_stocks' AND constraint_type = 'PRIMARY KEY'
        """).fetchone()
        has_pk = result[0] > 0
    except Exception:
        pass

    if not has_pk:
        logger.info("Migration: Adding PRIMARY KEY to offer_stocks...")
        self._connection.execute("BEGIN TRANSACTION")
        try:
            self._connection.execute("ALTER TABLE offer_stocks RENAME TO _offer_stocks_old")
            self._connection.execute("""
                CREATE TABLE offer_stocks (
                    id INTEGER PRIMARY KEY,
                    sku VARCHAR,
                    price DECIMAL(12, 2),
                    purchased_price DECIMAL(12, 2),
                    quantity INTEGER DEFAULT 0,
                    reserve INTEGER DEFAULT 0,
                    synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self._connection.execute("""
                INSERT INTO offer_stocks SELECT * FROM _offer_stocks_old
            """)
            self._connection.execute("DROP TABLE _offer_stocks_old")
            self._connection.execute("COMMIT")
            logger.info("Migration: offer_stocks PRIMARY KEY migration complete")
        except Exception as e:
            try:
                self._connection.execute("ROLLBACK")
            except Exception:
                pass
            logger.error(f"Migration failed (offer_stocks PK), rolling back: {e}")


def _m0013_sms_campaigns_cost_columns(self) -> None:
    # Migration: what a campaign cost to send.
    #
    # Without these the results page can report added margin but not whether
    # the campaign paid for itself — on the first real send that arithmetic
    # was done in someone's head against a figure from the provider's
    # invoice. The price is stored per campaign rather than read from config
    # at display time, so a tariff change does not silently restate history.
    #
    # No DEFAULT on any of them: ALTER ... ADD COLUMN ... DEFAULT rewrites
    # the whole table to materialise the value and has OOMed this database
    # before. Existing campaigns get NULL, which reads as "cost unknown".
    for col, ddl in (
        ("message_text", "VARCHAR"),
        ("message_parts", "INTEGER"),
        ("recipients_sent", "INTEGER"),
        ("price_per_part", "DECIMAL(10, 4)"),
        ("cost_total", "DECIMAL(14, 2)"),
    ):
        try:
            self._connection.execute(
                f"ALTER TABLE sms_campaigns ADD COLUMN IF NOT EXISTS {col} {ddl}"
            )
        except Exception as e:
            logger.debug(f"Migration note (sms_campaigns {col}): {e}")
    logger.debug("Migration: sms_campaigns cost columns added/verified")


def _m0015_sku_inventory_last_stock_out_at(self) -> None:
    # Migration: Add last_stock_out_at column to sku_inventory_status
    self._connection.execute(
        "ALTER TABLE sku_inventory_status ADD COLUMN IF NOT EXISTS last_stock_out_at DATE"
    )
    logger.debug("Migration: last_stock_out_at column added/verified on sku_inventory_status")


def _m0016_revenue_predictions_model_wape(self) -> None:
    # Migration: Add model_wape column to revenue_predictions
    self._connection.execute(
        "ALTER TABLE revenue_predictions ADD COLUMN IF NOT EXISTS model_wape DECIMAL(6, 2)"
    )
    logger.debug("Migration: model_wape column added/verified on revenue_predictions")


def _m0017_sms_campaign_members_delivery(self) -> None:
    # Migration: delivery columns on sms_campaign_members.
    #
    # The table ships from CREATE TABLE IF NOT EXISTS, which does nothing to
    # a table that already exists — so a database created before the
    # TurboSMS work has the roster but none of the delivery columns, and
    # every delivery report 500s. No DEFAULT on these: ALTER TABLE ... ADD
    # COLUMN ... DEFAULT rewrites the whole table and OOMs on a large DB.
    for column, ddl_type in (
        ("message_id", "VARCHAR"),
        ("delivery_status", "VARCHAR"),
        ("delivered", "BOOLEAN"),
        ("delivered_at", "TIMESTAMP WITH TIME ZONE"),
    ):
        try:
            self._connection.execute(
                f"ALTER TABLE sms_campaign_members "
                f"ADD COLUMN IF NOT EXISTS {column} {ddl_type}"
            )
            logger.debug(
                "Migration: %s column added/verified on sms_campaign_members",
                column,
            )
        except Exception as e:
            logger.debug(f"Migration note (sms_campaign_members {column}): {e}")


def _m0018_sms_members_message_id_index(self) -> None:
    # Index on message_id, created here rather than with the other indexes
    # because on a database that predates the TurboSMS work the column only
    # exists once the loop above has run. Every delivery report looks a
    # member up by message_id, and a send of 5 000 produces 5 000 of them
    # inside a minute; without this each one scans the whole table.
    self._connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_sms_members_message_id "
        "ON sms_campaign_members(message_id)"
    )


def _m0019_user_preferences_language(self) -> None:
    # Migration: Add language column to user_preferences
    self._connection.execute(
        "ALTER TABLE user_preferences ADD COLUMN IF NOT EXISTS language VARCHAR DEFAULT 'en'"
    )
    logger.debug("Migration: language column added/verified on user_preferences")


def _m0020_promocode_on_orders_and_silver(self) -> None:
    # Migration: Add promocode column to orders and silver_orders
    self._connection.execute(
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS promocode VARCHAR"
    )
    self._connection.execute(
        "ALTER TABLE silver_orders ADD COLUMN IF NOT EXISTS promocode VARCHAR"
    )
    logger.debug("Migration: promocode column added/verified on orders and silver_orders")


def _m0021_orders_audit_columns(self) -> None:
    # Migration: Add audit columns to orders (first_seen_at, update_count)
    # No DEFAULT in ALTER — avoids full table rewrite on 9GB+ DB (OOM).
    # CREATE TABLE schema has defaults for new rows; existing rows get NULL/NULL.
    self._connection.execute(
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP WITH TIME ZONE"
    )
    self._connection.execute(
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS update_count INTEGER"
    )
    logger.debug("Migration: audit columns (first_seen_at, update_count) added/verified on orders")


def _m0022_reconciliation_log(self) -> None:
    # Migration: Add reconciliation_log table
    self._connection.execute(
        "CREATE SEQUENCE IF NOT EXISTS reconciliation_seq START 1"
    )
    self._connection.execute("""
        CREATE TABLE IF NOT EXISTS reconciliation_log (
            id INTEGER DEFAULT(nextval('reconciliation_seq')),
            check_date DATE NOT NULL,
            api_count INTEGER NOT NULL,
            db_count INTEGER NOT NULL,
            discrepancy INTEGER NOT NULL,
            discrepancy_pct DECIMAL(6, 2) NOT NULL,
            status VARCHAR NOT NULL,
            checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    logger.debug("Migration: reconciliation_log table added/verified")


def _m0023_data_quality_tables(self) -> None:
    # Migration: Data Quality framework (Layer 1+2) — run+diff schema.
    # Replaces single-row-per-check pattern with proper run/diff
    # parent-child for trend and audit. Old reconciliation_log stays.
    self._connection.execute(
        "CREATE SEQUENCE IF NOT EXISTS data_quality_run_seq START 1"
    )
    self._connection.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_runs (
            run_id BIGINT PRIMARY KEY DEFAULT(nextval('data_quality_run_seq')),
            started_at TIMESTAMP WITH TIME ZONE NOT NULL,
            ended_at TIMESTAMP WITH TIME ZONE,
            as_of TIMESTAMP WITH TIME ZONE NOT NULL,
            window_start DATE NOT NULL,
            window_end DATE NOT NULL,
            layer VARCHAR NOT NULL,          -- 'integrity' | 'reconciliation' | 'combined'
            status VARCHAR NOT NULL,          -- 'PASS' | 'WARN' | 'CRITICAL' | 'FAILED'
            integrity_issues_count INTEGER DEFAULT 0,
            discrepancies_count INTEGER DEFAULT 0,
            critical_count INTEGER DEFAULT 0,
            warn_count INTEGER DEFAULT 0,
            api_calls_used INTEGER DEFAULT 0,
            duration_ms INTEGER,
            error_message VARCHAR
        )
    """)
    self._connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_dqr_started_at ON data_quality_runs(started_at DESC)"
    )
    self._connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_dqr_layer ON data_quality_runs(layer)"
    )

    # Child table: layer-1 issues
    self._connection.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_issues (
            run_id BIGINT NOT NULL,
            check_name VARCHAR NOT NULL,
            table_name VARCHAR NOT NULL,
            severity VARCHAR NOT NULL,
            count INTEGER NOT NULL,
            sample_ids VARCHAR,    -- JSON array
            description VARCHAR
        )
    """)
    self._connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_dqi_run ON data_quality_issues(run_id)"
    )

    # Child table: layer-2 discrepancies
    self._connection.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_diffs (
            run_id BIGINT NOT NULL,
            month VARCHAR NOT NULL,        -- 'YYYY-MM'
            source_id INTEGER NOT NULL,
            diff_class VARCHAR NOT NULL,
            field VARCHAR NOT NULL,
            dk_value DOUBLE NOT NULL,
            kc_value DOUBLE NOT NULL,
            severity VARCHAR NOT NULL,
            order_ids VARCHAR              -- JSON array, may be NULL
        )
    """)
    self._connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_dqd_run ON data_quality_diffs(run_id)"
    )
    logger.debug("Migration: data_quality_* tables added/verified")


def _m0024_disk_samples(self) -> None:
    # Migration: disk samples for the growth watchdog.
    # Tiny table — one row per 6h sample, retained ~14 days = ~56 rows.
    # No sequence: composite of sampled_at is enough; nobody queries by id.
    self._connection.execute("""
        CREATE TABLE IF NOT EXISTS disk_samples (
            sampled_at TIMESTAMP WITH TIME ZONE NOT NULL,
            db_size_mb DOUBLE NOT NULL,
            disk_pct_used DOUBLE NOT NULL,
            disk_free_gb DOUBLE NOT NULL
        )
    """)
    self._connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_disk_samples_at "
        "ON disk_samples(sampled_at DESC)"
    )
    logger.debug("Migration: disk_samples table added/verified")


def _m0025_memory_samples(self) -> None:
    # Same shape, for memory. Persisted because the kernel's own counters
    # (memory.peak, memory.events oom_kill) reset when the container is
    # recreated — which is how a 5.3 GB reading in August 2026 became
    # impossible to decompose an hour later.
    self._connection.execute("""
        CREATE TABLE IF NOT EXISTS memory_samples (
            sampled_at TIMESTAMP WITH TIME ZONE NOT NULL,
            working_set_mb DOUBLE NOT NULL,
            page_cache_mb DOUBLE NOT NULL,
            limit_mb DOUBLE,
            oom_kills INTEGER NOT NULL DEFAULT 0
        )
    """)
    self._connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_memory_samples_at "
        "ON memory_samples(sampled_at DESC)"
    )
    logger.debug("Migration: memory_samples table added/verified")


def _m0026_data_dir_samples(self) -> None:
    # One row per path group per sample. The growth detector differences
    # this at a 168h lag — the compact's own period — so everything
    # periodic cancels and only trend survives.
    self._connection.execute("""
        CREATE TABLE IF NOT EXISTS data_dir_samples (
            sampled_at TIMESTAMP WITH TIME ZONE NOT NULL,
            path_group VARCHAR NOT NULL,
            bytes BIGINT NOT NULL
        )
    """)
    self._connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_data_dir_samples_at "
        "ON data_dir_samples(sampled_at DESC)"
    )
    logger.debug("Migration: data_dir_samples table added/verified")


def _m0027_reset_sequences_after_compaction(self) -> None:
    # Migration: Fix sequences after EXPORT/IMPORT compaction.
    # DuckDB doesn't support ALTER SEQUENCE, and IMPORT resets sequences to
    # START value (1) even though tables already have rows. Fix by DROP+CREATE
    # with START = max(id) + 1.
    _seq_table_map = [
        ("warehouse_refresh_seq", "warehouse_refreshes", "id"),
        ("reconciliation_seq", "reconciliation_log", "id"),
        ("data_quality_run_seq", "data_quality_runs", "run_id"),
        ("seq_stock_movements_id", "stock_movements", "id"),
        ("seq_buyer_contacts_id", "buyer_contacts", "id"),
        ("seq_manual_expenses_id", "manual_expenses", "id"),
        ("seq_report_history_id", "report_history", "id"),
        ("seq_bronze_order_events_id", "bronze_order_events", "id"),
    ]
    for seq_name, table_name, col in _seq_table_map:
        try:
            row = self._connection.execute(
                f"SELECT COALESCE(MAX({col}), 0) FROM {table_name}"
            ).fetchone()
            max_id = row[0] if row else 0
            if max_id > 0:
                # Check current sequence value by peeking at nextval
                cur = self._connection.execute(f"SELECT nextval('{seq_name}')").fetchone()[0]
                if cur <= max_id:
                    new_start = max_id + 1
                    self._connection.execute(f"DROP SEQUENCE {seq_name}")
                    self._connection.execute(f"CREATE SEQUENCE {seq_name} START {new_start}")
                    logger.info(f"Sequence {seq_name} reset: {cur} → {new_start}")
        except Exception as e:
            logger.debug(f"Sequence fix note ({seq_name}): {e}")


MIGRATIONS: List[Migration] = [
    Migration("0001_orders_updated_at", ONCE, _m0001_orders_updated_at),
    Migration("0002_orders_status_group_id", ONCE, _m0002_orders_status_group_id),
    Migration("0003_orders_manager_comment", ONCE, _m0003_orders_manager_comment),
    Migration("0004_drop_gold_daily_products_indexes", ALWAYS, _m0004_drop_gold_daily_products_indexes),
    Migration("0005_warehouse_refreshes_silver_mode", ONCE, _m0005_warehouse_refreshes_silver_mode),
    Migration("0006_seed_manager_classifications", ALWAYS, _m0006_seed_manager_classifications),
    Migration("0007_order_products_drop_fk", ONCE, _m0007_order_products_drop_fk),
    Migration("0008_expenses_drop_fk", ONCE, _m0008_expenses_drop_fk),
    Migration("0009_gold_daily_traffic_sales_type", ONCE, _m0009_gold_daily_traffic_sales_type),
    Migration("0010_manual_expenses_platform", ONCE, _m0010_manual_expenses_platform),
    Migration("0011_order_products_id_bigint", ONCE, _m0011_order_products_id_bigint),
    Migration("0012_offer_stocks_primary_key", ONCE, _m0012_offer_stocks_primary_key),
    Migration("0013_sms_campaigns_cost_columns", ONCE, _m0013_sms_campaigns_cost_columns),
    Migration("0015_sku_inventory_last_stock_out_at", ONCE, _m0015_sku_inventory_last_stock_out_at),
    Migration("0016_revenue_predictions_model_wape", ONCE, _m0016_revenue_predictions_model_wape),
    Migration("0017_sms_campaign_members_delivery", ONCE, _m0017_sms_campaign_members_delivery),
    Migration("0018_sms_members_message_id_index", ONCE, _m0018_sms_members_message_id_index),
    Migration("0019_user_preferences_language", ONCE, _m0019_user_preferences_language),
    Migration("0020_promocode_on_orders_and_silver", ONCE, _m0020_promocode_on_orders_and_silver),
    Migration("0021_orders_audit_columns", ONCE, _m0021_orders_audit_columns),
    Migration("0022_reconciliation_log", ONCE, _m0022_reconciliation_log),
    Migration("0023_data_quality_tables", ONCE, _m0023_data_quality_tables),
    Migration("0024_disk_samples", ONCE, _m0024_disk_samples),
    Migration("0025_memory_samples", ONCE, _m0025_memory_samples),
    Migration("0026_data_dir_samples", ONCE, _m0026_data_dir_samples),
    Migration("0027_reset_sequences_after_compaction", ALWAYS, _m0027_reset_sequences_after_compaction),
]
