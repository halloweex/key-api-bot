"""The revenue goals, so the monthly report can be read from Postgres.

Revision ID: 0017_revenue_goals
Revises: 0016_dashboard_users
Created: 2026-09-07

Three rows at most — `daily`, `weekly`, `monthly` — and the monthly report
reads exactly one of them, to draw the target line. Small enough to look
unnecessary, and it is the last thing standing between `/marketing` and
Postgres, because a query cannot join a table that is not there.

WHY IT IS REPLICATED AND NOT DERIVED

`goal_amount` is a number a human typed, or a suggestion the goals service
computed from history and a growth factor that a human also typed. KeyCRM has
never heard of it, and no rebuild can produce it — the same argument revision
0008 makes for `stock_movements` and 0016 for the user list. So it rides
`replicate_operational` with the rest of that family: one hourly job, one call
site, and the daily comparison for free.

THE STALENESS THIS ACCEPTS

The goals API writes DuckDB, and this copy is up to an hour behind it. A goal
set at 10:00 shows on the monthly report by 11:00. That is a target line on a
report of a calendar month, changed a few times a year — unlike the user list,
where a per-request read *is* revocation and an hour of lag would have been a
security hole rather than a display lag. The writer therefore stays where it
is, and this is a read replica.

`seasonal_indices` — the goals service's other table — is deliberately **not**
here. Nothing on `/marketing` reads it, and copying a table because it sits
next to one that is needed is how a mirror grows tables nobody compares.
"""
from alembic import op

revision = "0017_revenue_goals"
down_revision = "0016_dashboard_users"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE app.revenue_goals (
            -- daily | weekly | monthly. VARCHAR(10) in DuckDB; TEXT here for
            -- 0009's reason — a length limit the other store does not have
            -- turns the comparison into a filter, and a truncated row would
            -- read as a difference the mirror invented.
            period_type      TEXT PRIMARY KEY,

            -- NUMERIC(12,2), matching DuckDB's DECIMAL(12,2) exactly. A wider
            -- or narrower scale makes the comparison report a change the
            -- rounding invented — `app.order_versions`' note, and the reason
            -- money is never `double precision` in this schema.
            goal_amount      NUMERIC(12,2) NOT NULL,
            is_custom        BOOLEAN,
            calculated_goal  NUMERIC(12,2),
            growth_factor    NUMERIC(4,2),
            updated_at       TIMESTAMPTZ,

            -- Not shared with DuckDB and not compared: two correct copies
            -- differ on when each of them wrote the row.
            mirrored_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        "COMMENT ON TABLE app.revenue_goals IS "
        "'Revenue targets a human set. A read replica of DuckDB, up to an hour "
        "behind; the goals API still writes the other store.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.revenue_goals")
