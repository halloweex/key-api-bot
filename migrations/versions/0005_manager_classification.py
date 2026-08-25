"""bronze.managers and app.manager_classifications — the one thing KeyCRM cannot tell us

Revision ID: 0005_manager_classification
Revises: 0004_backfilled_at
Created: 2026-08-25

Everything mirrored so far came from a KeyCRM payload: parse it once, hand the
same tuple to both stores, and if Postgres is ever lost the sync refills it.
These two tables break that rule, and they are the prerequisite for computing
Silver in Postgres at all.

`sales_type` — the column every dashboard endpoint filters on — is decided by
`silver_sales_type_case()`, and four of its six branches read these two tables.
KeyCRM has no idea whether a manager is retail, wholesale, internal, or someone
who ships to bloggers. **The only source for that is a human**, and that is
exactly why this cannot be a payload-fed mirror: `upsert_managers` seeds
`is_retail` from `RETAIL_MANAGER_IDS` for managers it has never seen and
**never touches it again**, because recomputing it from that constant on every
sync is what made the column unfixable — whatever a human set was overwritten
within the minute, and managers 3, 5, 6, 7, 10, 28, 34 and 40 sold ₴3.1M into
the wrong bucket for months.

So a naive mirror that re-seeded from the same constant would give the two
stores **different classifications**, and the Gold reconciliation that follows
would then be measuring the classification instead of the migration. These rows
are therefore *replicated from DuckDB*, not parsed from KeyCRM.

WHY THE CLASSIFICATIONS GO IN `app` AND THE MANAGERS DO NOT

`postgres/initdb/30-app.sql` draws the line at recoverability. Effective-dated
classifications are decisions somebody made on a date and nothing can derive
them again, so they are `app` — beside users and goals.

`bronze.managers` is mostly KeyCRM's own (name, email, status) and carries one
column that is not: `is_retail`. It stays whole rather than being split across
two schemas for a twenty-row table, and it is honest about the cost — **dropping
`bronze` today loses `is_retail`.** Not yet a real risk: during the parallel
period the column is a replica and re-replicating restores it. It becomes one
the day Postgres is the system of record, and the fix then is the same either
way — `manager_classifications` already outranks it in the CASE, so the residual
`is_retail` fallback exists only for a manager nobody has ever classified.

THE EFFECTIVE-DATED KEY

`(manager_id, valid_from)`, matching DuckDB. `valid_to NULL` means "still in
force". Reclassifying somebody used to rewrite their whole history on the next
rebuild; the owner ruled that out on 2026-08-20, and the interval table is how
`sales_type` is resolved **as of the order's own date** instead.
"""
from __future__ import annotations

from alembic import op

revision = "0005_manager_classification"
down_revision = "0004_backfilled_at"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE bronze.managers (
            id               INTEGER PRIMARY KEY,
            name             TEXT    NOT NULL,
            email            TEXT,
            status           TEXT,
            -- Replicated from DuckDB, never seeded from RETAIL_MANAGER_IDS
            -- here. See the module docstring: re-seeding is what made this
            -- column unfixable on the other side.
            is_retail        BOOLEAN NOT NULL DEFAULT FALSE,
            first_order_date DATE,
            last_order_date  DATE,
            order_count      INTEGER NOT NULL DEFAULT 0,

            mirrored_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )

    op.execute(
        """
        CREATE TABLE app.manager_classifications (
            manager_id  INTEGER NOT NULL,
            is_retail   BOOLEAN NOT NULL,
            valid_from  DATE    NOT NULL,
            -- NULL means "still in force". No EXCLUDE constraint on the
            -- interval: DuckDB has none either, the writer is a single
            -- endpoint, and a constraint here that the other store does not
            -- enforce would let Postgres reject a row DuckDB accepted — the
            -- filter-not-a-mirror failure revision 0002 avoids.
            valid_to    DATE,
            set_by      INTEGER,
            set_at      TIMESTAMPTZ,
            note        TEXT,

            mirrored_at TIMESTAMPTZ NOT NULL DEFAULT now(),

            PRIMARY KEY (manager_id, valid_from)
        )
        """
    )
    op.execute(
        "CREATE INDEX idx_manager_classifications_manager "
        "ON app.manager_classifications (manager_id)"
    )

    op.execute(
        "COMMENT ON TABLE bronze.managers IS "
        "'Replicated from DuckDB, not parsed from KeyCRM: is_retail is a "
        "human decision KeyCRM cannot supply.'"
    )
    op.execute(
        "COMMENT ON TABLE app.manager_classifications IS "
        "'Effective-dated retail classification. Resolves sales_type as of the "
        "order date. Replicated from DuckDB during the parallel period.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.manager_classifications")
    op.execute("DROP TABLE IF EXISTS bronze.managers")
