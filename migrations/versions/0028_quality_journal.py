"""The Postgres twins of the data-quality journal: one run and its two children.

Revision ID: 0028_quality_journal
Revises: 0027_forensics
Created: 2026-09-14

WHAT THESE THREE ARE

`data_quality_runs` is one row per check run — layer, window, status, counts,
duration, and `error_message` when the run itself threw. `data_quality_issues`
and `data_quality_diffs` are its findings: the layer-1 integrity issues and the
layer-2 per-month discrepancies against KeyCRM. Together they are what the
09:00 digest reads, what `/api/health/data-quality` publishes, and — through
that endpoint — what the bot's canary judges every fifteen minutes from the
other container.

A failed run writes a row too, with `error_message` set. That is why the age
watchdog keys on `error_message IS NULL` and not on row-existence, and it is
the reason `error_message` must stay nullable and untruncated here.

313 + 333 + 411 = 1 057 rows, measured on the 2026-09-13 production backup.

FULL REPLACE, NOT A WATERMARK — AND THAT IS A REVERSAL

These are append-only in DuckDB, so a watermark looks like the obvious shape,
and the first draft of this used one: the children shipping under the
**parent's** `MAX(run_id)` rather than their own, because a run that finds
nothing writes no child rows at all. That argument is real and the measurement
is worse than it sounds — 144 of 313 runs have no issues, and
`data_quality_diffs` has not gained a row since run 260 while runs are at 596.
A self-derived watermark on the diffs would sit 336 runs behind for ever.

But the premise was wrong. 1 057 rows is *smaller* than two tables already
rewritten whole every hour here — `bronze.offer_stocks` (892) and
`app.sku_inventory_status` (887). A watermark buys nothing at this size, and it
costs a coupling between three tables that would have to be right for ever.
Full replace also means a future retention sweep on the journal needs no change
here at all, where a watermark would silently accumulate every pruned row.

THE CHILDREN HAVE NO CLOCK, AND BORROW THEIR PARENT'S

Neither child table has a timestamp column — seven columns and nine columns,
none of them a time. That matters more than it looks: the daily comparison
forgives a row written between the copy and the check, and it needs a clock to
do it. Without one, every finding written by the 07:00 integrity scan is
CRITICAL at the 07:30 comparison, most mornings.

They borrow the run's `started_at` through a correlated subquery in
`MirroredTable.synced_column`, which is read **only** on the DuckDB side and
only to date the row. The borrowing is sound rather than convenient: all three
tables are written inside one DuckDB transaction by `persist_run`, so a child
is in Postgres if and only if its parent is, and the parent's clock is
therefore exactly this child's clock. There is no third state to get wrong.

KEYS THE SOURCE DOES NOT DECLARE

`data_quality_runs` has a primary key in DuckDB; the two children have only an
index on `run_id`. Their natural keys were measured before being declared, on
the production backup: 0 duplicates of `(run_id, check_name, table_name)`
across 333 rows and 0 of `(run_id, month, source_id, diff_class, field)` across
411 — and 0 orphan children on either table, so the parent relationship holds
in the data as well as in the code.

No FOREIGN KEY is declared, deliberately. DuckDB has none, the three ship
inside one transaction in parent-first order, and an FK would turn a partial
shipment into a failure of the whole hourly replication rather than a finding
the next comparison reports.

`run_id` GAPS ARE NORMAL AND MUST NOT BE READ AS LOSS

313 rows across run_id 1..596. The weekly compaction exports to Parquet and
re-imports, and the sequence does not come back where it left off. Nothing here
may assume the ids are dense.

TYPES

`dk_value` and `kc_value` stay DOUBLE PRECISION. They look like money and are
compared at a tolerance of zero, which is the usual argument for NUMERIC — and
here it inverts: the source column is DOUBLE, so both sides hand back a float,
and declaring NUMERIC would return `Decimal` on one side only and report every
row as differing. `sample_ids` and `order_ids` stay TEXT and are **not** json
or jsonb: a json column re-renders on the round trip, and a zero-tolerance
comparison would then report every row. `order_ids` NULL is meaningful — it is
None precisely when the discrepancy carries no ids — so no DEFAULT '[]'.
"""
from alembic import op


revision = "0028_quality_journal"
down_revision = "0027_forensics"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # `run_id` carries DuckDB's sequence value: no IDENTITY. The children key
    # on it, and a second allocator would make the two stores disagree about
    # which run a finding belongs to.
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.data_quality_runs (
            run_id                 BIGINT      NOT NULL,
            started_at             TIMESTAMPTZ NOT NULL,
            ended_at               TIMESTAMPTZ,
            as_of                  TIMESTAMPTZ NOT NULL,
            window_start           DATE        NOT NULL,
            window_end             DATE        NOT NULL,
            layer                  VARCHAR     NOT NULL,
            status                 VARCHAR     NOT NULL,
            integrity_issues_count INTEGER     DEFAULT 0,
            discrepancies_count    INTEGER     DEFAULT 0,
            critical_count         INTEGER     DEFAULT 0,
            warn_count             INTEGER     DEFAULT 0,
            api_calls_used         INTEGER     DEFAULT 0,
            duration_ms            INTEGER,
            error_message          TEXT,
            PRIMARY KEY (run_id)
        )
    """)
    # No FOREIGN KEY on either child — see the module docstring.
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.data_quality_issues (
            run_id      BIGINT  NOT NULL,
            check_name  VARCHAR NOT NULL,
            table_name  VARCHAR NOT NULL,
            severity    VARCHAR NOT NULL,
            count       INTEGER NOT NULL,
            sample_ids  TEXT,
            description TEXT,
            PRIMARY KEY (run_id, check_name, table_name)
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.data_quality_diffs (
            run_id     BIGINT           NOT NULL,
            month      VARCHAR          NOT NULL,
            source_id  INTEGER          NOT NULL,
            diff_class VARCHAR          NOT NULL,
            field      VARCHAR          NOT NULL,
            dk_value   DOUBLE PRECISION NOT NULL,
            kc_value   DOUBLE PRECISION NOT NULL,
            severity   VARCHAR          NOT NULL,
            order_ids  TEXT,
            PRIMARY KEY (run_id, month, source_id, diff_class, field)
        )
    """)

    # Carried from DuckDB. The digest and /api/health both ask for the latest
    # run per layer, which is what these two serve.
    op.execute("CREATE INDEX IF NOT EXISTS idx_dqr_started_at "
               "ON app.data_quality_runs (started_at DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_dqr_layer "
               "ON app.data_quality_runs (layer)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_dqi_run "
               "ON app.data_quality_issues (run_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_dqd_run "
               "ON app.data_quality_diffs (run_id)")


def downgrade() -> None:
    for table in ("data_quality_diffs", "data_quality_issues",
                  "data_quality_runs"):
        op.execute(f"DROP TABLE IF EXISTS app.{table}")
