"""meta.mirror_state — what the dual write has and has not shipped

Revision ID: 0001_mirror_state
Revises:
Created: 2026-08-23

The first table this application owns in Postgres, and the one step 05 cannot
start without.

When landing is mirrored, a failure to write Postgres must not break the sync —
DuckDB stays the system of record for the whole parallel period, and charter
rule 8 says the step leaves the system working. But it must not be silent
either: a failure swallowed at DEBUG is exactly how 2026-08-09 turned one bad
`ALTER` into a rebuild every two minutes for hours.

This is where it stops being silent. Without a watermark the daily
reconciliation cannot tell **"not shipped yet"** from **"shipped wrong"**, and
every lag reads as a discrepancy. With one, the two are different rows.

One row per mirrored table rather than a log: the question asked of it is
always "where is this table now", and a table that answers that in one row
needs no retention policy of its own.
"""
from __future__ import annotations

from alembic import op

revision = "0001_mirror_state"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE meta.mirror_state (
            table_name         TEXT PRIMARY KEY,

            -- When the writer last tried, and when it last succeeded. Both,
            -- not one: equal means healthy, apart means the gap between them
            -- is how long this table has been behind.
            last_attempted_at  TIMESTAMPTZ,
            last_ok_at         TIMESTAMPTZ,

            -- Consecutive failures, reset by a success. A count that only ever
            -- rises tells you a failure happened once; this tells you whether
            -- it is still happening.
            failures_since_ok  BIGINT NOT NULL DEFAULT 0,
            last_error         TEXT,

            -- Rows in the last successful batch. Enough to notice a mirror
            -- that is running, reporting success, and shipping nothing.
            last_rows          BIGINT,

            CONSTRAINT mirror_state_failures_nonneg
                CHECK (failures_since_ok >= 0),
            -- A success cannot predate the attempt that produced it.
            CONSTRAINT mirror_state_ok_not_before_attempt
                CHECK (last_ok_at IS NULL
                       OR last_attempted_at IS NULL
                       OR last_ok_at <= last_attempted_at)
        )
        """
    )
    op.execute(
        "COMMENT ON TABLE meta.mirror_state IS "
        "'Per-table watermark for the DuckDB -> Postgres mirror (step 05). "
        "Lets reconciliation tell not-yet-shipped from shipped-wrong.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS meta.mirror_state")
