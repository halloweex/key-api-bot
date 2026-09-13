"""The Postgres twins of the two forensic logs, both shipped above a watermark.

Revision ID: 0027_forensics
Revises: 0026_watchdogs_and_ledgers
Created: 2026-09-14

TWO TABLES, 76 589 ROWS, AND 93% OF EVERYTHING STILL HOMELESS

`warehouse_refreshes` is 74 388 rows on its own — more than every other table
in the stage-3 set put together, and the best forensic trail this system has.
One row per warehouse tick since 2026-03-14: what triggered it, how long it
took, how many rows each layer held, the two revenue checksums, whether they
matched, whether validation passed, the error if it threw, and — since
migration 0005 — whether Silver was rebuilt whole or incrementally. It is what
answered the file-growth question, and it is what anyone debugging a bad
afternoon reads first.

`reconciliation_log` is the older, smaller record of the same instinct: 2 201
rows of "KeyCRM said this many orders, we held that many". The job that writes
it is labelled legacy — `dq_reconciliation` is the source of truth for alerts
now — but the history is not, and a legacy writer is a reason to move the table
carefully, not a reason to drop the rows.

APPEND ABOVE A WATERMARK, AND WHY THAT IS NOT A PREFERENCE

Revision 0026's five ship as a full replace because their writers rewrite them.
These two cannot: re-sending 74 388 rows every hour to carry the ~25 that were
added would be waste with a straight face, and the DuckDB writers only ever
INSERT. That was established by search rather than assumed — no UPDATE and no
DELETE against either table exists anywhere in the repository, and there is no
retention sweep, which is the thing that disqualified the watchdogs.

The watermark is `MAX(id)` read back out of Postgres at the start of each run.
**No stored cursor**, `core/pg_backfill.py`'s reason and `app.stock_movements`'
arrangement exactly: there is no position to be wrong, and an interrupted run
resumes by recomputing rather than by trusting a number somebody wrote down.

THE ID IS CARRIED, NEVER GENERATED

Both ids come from a DuckDB sequence and neither writer names the column.
`app.stock_movements` settled this already and the argument is unchanged: a
Postgres `IDENTITY` would give the same refresh two names, and the bucketed
comparison — which joins on the id — would be meaningless before it began. So
the column is a plain INTEGER PRIMARY KEY with no default, and the value that
arrives is the value DuckDB allocated.

INTEGER and not BIGINT, because DuckDB's INTEGER is int32 and the two stores
should agree about the type. At ~400-700 rows a day, int32 is several thousand
years away.

`reconciliation_log` gains a PRIMARY KEY that DuckDB does not declare — it has
only `id INTEGER DEFAULT(nextval(...))`, with no key and no NOT NULL. Measured
before declaring: 2 201 rows, 2 201 distinct non-null ids on the 2026-09-13
production backup.

TWO COLUMNS THAT LOOK LIKE OVERSIGHTS AND ARE NOT

`gold_products_rows` is NULL on every row written since the products Gold was
retired, and `core/duckdb_store.py` says why in the writer: the column holds
74k rows of history and a 0 would read as "built nothing", when the truth is
that there is no such layer any more. NOT NULL or DEFAULT 0 here would destroy
exactly that distinction.

`silver_mode` arrived by `ALTER TABLE` in migration 0005 and is **not in the
CREATE TABLE** — so a twin built by reading the CREATE TABLE alone is one
column short, and the shipper would then silently never carry it. It is NULL on
two whole classes of row and both are real: every row written before 0005 ran,
and every row written by the error path, whose INSERT never named it.

`checksum_match` and `validation_passed` are nullable for the same reason —
the error path names five columns and neither is among them. NULL there means
"we never got far enough to compare", which is a different fact from FALSE.

`error` is TEXT, not VARCHAR(n): it is `str(e)` of an arbitrary exception, and
it is also what the self-heal budget keys on (`error IS NULL` separates "it
threw" from "validation failed"), so a truncating type would change behaviour
rather than just storage.

`trigger` is a legal unqualified column name in PostgreSQL — `pg_get_keywords()`
reports it unreserved — and is left unbounded rather than VARCHAR(32). The five
literals in use are all under fifteen characters, but a narrowing Postgres
enforces and DuckDB does not means a future sixth would be accepted by the
writer and would kill the replication of every operational table for that hour.

MONEY AND DURATIONS ARE NUMERIC

`duration_ms`, and both revenue checksums, are DECIMAL in DuckDB and come back
as `Decimal`. They are compared at a tolerance of zero, so NUMERIC with the
same scale is the only type that round-trips without manufacturing a
difference — `bronze.orders.grand_total`'s rule.
"""
from alembic import op


revision = "0027_forensics"
down_revision = "0026_watchdogs_and_ledgers"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # `id` carries DuckDB's sequence value: no IDENTITY, no DEFAULT. See the
    # module docstring — a second allocator would give one refresh two names.
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.warehouse_refreshes (
            id                      INTEGER      NOT NULL,
            refreshed_at            TIMESTAMPTZ,
            trigger                 VARCHAR      NOT NULL,
            duration_ms             NUMERIC(10, 2),
            bronze_orders           INTEGER,
            silver_rows             INTEGER,
            gold_revenue_rows       INTEGER,
            gold_products_rows      INTEGER,
            silver_revenue_checksum NUMERIC(14, 2),
            gold_revenue_checksum   NUMERIC(14, 2),
            checksum_match          BOOLEAN,
            validation_passed       BOOLEAN,
            error                   TEXT,
            silver_mode             VARCHAR,
            PRIMARY KEY (id)
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.reconciliation_log (
            id              INTEGER       NOT NULL,
            check_date      DATE          NOT NULL,
            api_count       INTEGER       NOT NULL,
            db_count        INTEGER       NOT NULL,
            discrepancy     INTEGER       NOT NULL,
            discrepancy_pct NUMERIC(6, 2) NOT NULL,
            status          VARCHAR       NOT NULL,
            checked_at      TIMESTAMPTZ,
            PRIMARY KEY (id)
        )
    """)

    # Carried from the DuckDB originals. Both tables are read newest-first by
    # the things that consult them — the consecutive-failure count, the
    # self-heal budget, the admin page — and the bucketed comparison reads them
    # by id, which the primary key already serves.
    op.execute("CREATE INDEX IF NOT EXISTS idx_warehouse_refreshes_at "
               "ON app.warehouse_refreshes (refreshed_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_reconciliation_log_at "
               "ON app.reconciliation_log (checked_at DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.warehouse_refreshes")
    op.execute("DROP TABLE IF EXISTS app.reconciliation_log")
