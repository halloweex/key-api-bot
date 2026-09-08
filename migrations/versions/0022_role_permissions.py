"""The last table the access path still read out of DuckDB.

Revision ID: 0022_role_permissions
Revises: 0021_user_allowed_features
Created: 2026-09-08

WHY THIS ONE MOVES NOW

Revision 0016 moved *who* may open the dashboard; the matrix saying *what each
role may do* stayed in DuckDB, and that was defensible while it was read once
per role and cached in-process. Per-user tabs changed the arithmetic: a
permission dependency now sits on roughly 120 of the 140 endpoints instead of
the ~20 that were gated before, so every cache miss — every deploy, every
permission edit — takes DuckDB's **process-wide store lock** on the request
path of an authorization check.

That lock is not a detail here. The warehouse rebuild holds it every two
minutes, and it is the measured cause of the SMS tab going from 124 req/s to
38 (see `core/pg_dashboard_users.py`). An authorization check that can queue
behind a rebuild is the one read that should never be able to.

WHY NOT CLICKHOUSE

Asked, and the answer is no. These rows are read on every request, written by
a human through the admin page, and authoritative — ClickHouse has no
transactional update, and this repository uses it for analytics that can be
rebuilt from source. Access data cannot be rebuilt from anything.

WHY THE SAME SWITCH

`KS_USER_STORE` already decides where the user list lives. Splitting the two
across engines would mean an approval and the permissions behind it could
disagree about which database is authoritative — so the matrix follows the
list, and one variable still answers "where does access live".

NO CHECK CONSTRAINTS, and no foreign key to anything. Roles and features are
code (`core/permissions.py`); a constraint here would turn "a feature was
added" into a failed write rather than a row nobody has seeded yet, which is
0016's reasoning about `role` and `status` applied one table over.
"""
from alembic import op

revision = "0022_role_permissions"
down_revision = "0021_user_allowed_features"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app.role_permissions (
            role       TEXT    NOT NULL,
            feature    TEXT    NOT NULL,
            can_view   BOOLEAN NOT NULL DEFAULT FALSE,
            can_edit   BOOLEAN NOT NULL DEFAULT FALSE,
            can_delete BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_by BIGINT,
            PRIMARY KEY (role, feature)
        )
        """
    )
    op.execute(
        "COMMENT ON TABLE app.role_permissions IS "
        "'What each role may do with each feature. Seeded from "
        "core.permissions.ROLE_PERMISSIONS for any pair the table does not "
        "carry; turning a permission off stores false rather than deleting the "
        "row, so re-seeding cannot resurrect a decision somebody made.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.role_permissions")
