"""The dashboard's own user list, so authentication can leave DuckDB.

Revision ID: 0016_dashboard_users
Revises: 0015_inventory_views_today
Created: 2026-09-07

WHY A SECOND USER TABLE, NEXT TO `app.authorized_users`

Because there genuinely are two lists, and they do not agree. Measured on
production the day this was written:

    DuckDB `users`            24 rows, 16 approved   ← the dashboard's list
    app.authorized_users      24 rows,  4 approved   ← the bot's list

Twelve of the sixteen people who can open the dashboard are `denied` in the
bot's list. That is not drift: `revoke_user` in the bot store is implemented as
`deny`, and revoking somebody's *bot* access has never been a statement about
their dashboard access. One of the twelve is an owner, who keeps working today
only because their id is hardcoded in `ADMIN_USER_IDS` and short-circuits the
lookup entirely.

So pointing the session at `app.authorized_users` would lock twelve of sixteen
people out. Whether the two lists *should* be one is a decision for the owner
and is deliberately not taken here; this revision moves the dashboard's list to
Postgres unchanged, and leaves both lists saying exactly what they said before.

AND THE COLUMN THAT ONLY THIS TABLE HAS

`role`. Neither the SQLite nor the Postgres `authorized_users` has ever carried
one — that is why `web/routes/auth.py` downgrades to `viewer` on its fallback
path rather than trusting the cookie. Of the 24 rows, three are not `viewer`:
two admins, both hardcoded, and one `marketer`, which is the role the SMS
permission rides on. So exactly one person's access depends on this column
being read correctly, and they would silently lose the SMS tab if it were not.

WHY THE WRITER MOVES TOO, RATHER THAN A REPLICA BEING READ

`_resolve_session` re-reads status and role on **every request**, and that read
is what revocation actually is here — sessions are stateless signed tokens that
cannot be withdrawn, so taking somebody's access away means the next request
finds them no longer approved. Reading an hourly replica would give a revoked
account up to an hour more, which trades a lock for a security hole. There is
exactly one writer — the web container, via the admin page — so moving it is
cheaper than making a lagging copy safe.

The replication below therefore exists only to carry history across before the
switch, and stands down after it, the way `replicate_sms` does under
`KS_SMS_STORE=postgres`.
"""
from alembic import op

revision = "0016_dashboard_users"
down_revision = "0015_inventory_views_today"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE app.dashboard_users (
            -- BIGINT for `app.authorized_users`' reason: Telegram ids passed
            -- 2^31 in 2021.
            user_id       BIGINT PRIMARY KEY,
            username      TEXT,
            first_name    TEXT,
            last_name     TEXT,
            photo_url     TEXT,

            -- admin | editor | viewer | marketer. No CHECK, and the reason is
            -- the one 0009 gives for `status`: a constraint the other store
            -- does not have turns the comparison into a filter, and a role
            -- added in code would then fail to copy rather than fail to be
            -- meaningful.
            role          TEXT NOT NULL DEFAULT 'viewer',
            -- pending | approved | denied | frozen
            status        TEXT NOT NULL DEFAULT 'pending',

            -- Defaulted, because DuckDB's table defaults them and
            -- `create_user` inserts neither: a user created on this side would
            -- otherwise carry no request date at all, the admin page would
            -- show a blank, and `COALESCE(reviewed_at, created_at)` — the
            -- clock the daily comparison forgives a row in flight by — would
            -- be NULL. Caught by the differential test, which created a user
            -- on each engine and compared the rows.
            --
            -- Replication passes both columns explicitly, so the default never
            -- overwrites a carried value.
            requested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            reviewed_at   TIMESTAMPTZ,
            reviewed_by   BIGINT,
            last_activity TIMESTAMPTZ,
            denial_count  INTEGER NOT NULL DEFAULT 0,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

            -- Not shared with DuckDB, and not compared. Two correct copies
            -- differ on when each of them wrote the row.
            mirrored_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    # The admin page lists by status and the session looks up by primary key.
    # Twenty-four rows need no index for speed; this one is here because the
    # table is expected to grow with the company and the list query is the only
    # one that is not a point lookup.
    op.execute(
        "CREATE INDEX idx_dashboard_users_status ON app.dashboard_users (status)"
    )
    op.execute(
        "COMMENT ON TABLE app.dashboard_users IS "
        "'Who may open the dashboard, and as what. Distinct from "
        "app.authorized_users, which is the bot''s list and carries no role; "
        "the two disagreed about 12 of 16 people when this was created.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.dashboard_users")
