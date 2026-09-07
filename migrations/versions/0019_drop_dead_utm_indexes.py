"""Two indexes on `silver.order_utm` that no query can use.

Revision ID: 0019_drop_dead_utm_indexes
Revises: 0018_traffic
Created: 2026-09-07

Revision 0018 gave the table indexes on `platform` and `traffic_type`, copied
across from DuckDB's DDL on the reasoning that they are "the two the reads
filter and group on". They are — but never as themselves.

WHY THEY CANNOT BE USED

Every predicate in this tab wraps the column with the order's own fallback:

    COALESCE(u.platform,
             CASE s.source_id WHEN 1 THEN 'instagram' ... ELSE 'other' END) = ?

That expression is not `u.platform`, and its value depends on a column in the
*other* table, so no index on `silver.order_utm` alone can answer it. Confirmed
two ways rather than assumed: no predicate anywhere in `core/` or `web/` reads
`u.platform` or `u.traffic_type` without the COALESCE, and `EXPLAIN (ANALYZE)`
of the filtered query on production takes `Seq Scan on order_utm` and applies
`Filter: (COALESCE(u.platform, CASE ...) = 'facebook')` after the join.

THE STATISTIC THAT LOOKED LIKE USAGE

`pg_stat_user_indexes` reported 3 scans on `idx_order_utm_platform` against 108
on the primary key, and 0 on `idx_order_utm_traffic_type`. The 3 are not a
predicate: at 248 kB that was the smallest index on the table, so `SELECT
count(*)` took an index-only scan over it. The primary key answers those just
as well.

WHAT THEY COST

`ship_order_utm` replaces the table whole every Silver tick, so both indexes
are rebuilt over 32,924 rows roughly every twelve minutes — write
amplification on a permanent cycle, for a read benefit of zero. Half a
megabyte of it is also half a megabyte in every base backup.

The primary key stays, and it is the one that is load-bearing: the join reads
`u.order_id`, and the 1:1 grain it enforces is what lets this store do without
a traffic Gold at all.
"""
from alembic import op

revision = "0019_drop_dead_utm_indexes"
down_revision = "0018_traffic"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS silver.idx_order_utm_platform")
    op.execute("DROP INDEX IF EXISTS silver.idx_order_utm_traffic_type")


def downgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_order_utm_platform "
        "ON silver.order_utm (platform)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_order_utm_traffic_type "
        "ON silver.order_utm (traffic_type)"
    )
