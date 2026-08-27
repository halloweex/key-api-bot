"""app.* — the bot's own state, which exists in exactly one place on earth

Revision ID: 0009_bot_state
Revises: 0008_operational_history
Created: 2026-08-27

Step 03: the bot's SQLite into Postgres. This is the destination; the copy is
`core/pg_bot_state.py` and the switch of the *writer* is a later step.

WHY THIS IS THE MOST URGENT FIFTY ROWS IN THE SYSTEM

`data/bot.db` is not in any backup. Measured, not assumed:

    data/backups/          analytics-*.duckdb only
    deploy/daily_offsite.sh  reads `ls -t "$BACKUP_DIR"/analytics-*.duckdb`
    the Ark                  analytics.duckdb.frozen, its parquet and schema

One file, one disk, no copy — holding 24 approved users, 25 celebrated
milestones and everyone's language choice. Nothing re-derives any of it: an
approval is a human's decision, a celebrated milestone is a message that was
already sent, and a language is a preference nobody will re-enter. Losing the
disk means re-approving 24 people by hand and congratulating the shop a second
time on revenue it passed in March.

Landing them here puts them inside step 01's WAL archiving and nightly dump —
the backup story they have never had — and that is worth having on its own,
before any question of switching the writer.

WHY `app` AND NOT A SCHEMA OF THEIR OWN

Because `postgres/initdb/30-app.sql` already decided it, in writing, naming
these exact rows: "bot.db today holds 24 authorised users, 25 celebrated
milestones and everyone's language choice — about fifty rows that exist nowhere
else and that no sync can bring back." `app` is defined as what nothing can
decide again, and that is precisely what these are. A `bot` schema would draw a
boundary along the writing *container* rather than along the meaning, and this
repository's bot and web are one codebase, one image build and one deploy —
unlike the shop, whose `tgbot` schema separates a genuinely different
application with a role of its own.

FOUR TABLES, NOT FIVE

`cache` stays behind. It is a TTL cache — `bot/services.py` writes it with a
ten-minute life and `bot/main.py` sweeps it — so it holds no fact that can be
lost, and copying it would report a discrepancy every time an entry expired
between the copy and the comparison. That is the `headline_vs_line_items`
disease: a check that fires daily for a permanent reason teaches its reader to
skip the message it arrives in. Production has 0 rows in it today.

TYPES: A COPY IS A COPY, WITH TWO DELIBERATE CONVERSIONS

`start_date` and `end_date` stay `TEXT` because that is what SQLite holds;
narrowing them to `DATE` here would make this a filter rather than a copy, and
the daily comparison would report a difference the schema created. Same
reasoning as revisions 0002, 0003, 0006 and 0008.

Two conversions are unavoidable and both are pinned by tests:

* **`notifications_enabled` INTEGER → BOOLEAN.** SQLite has no boolean and
  stores 1/0. Keeping it an integer here would carry SQLite's lack of a type
  into a database that has one.
* **Timestamps TEXT → `TIMESTAMPTZ`, read as UTC.** SQLite writes
  `CURRENT_TIMESTAMP` as `'YYYY-MM-DD HH:MM:SS'` with no zone, and that
  function is UTC by definition. Reading it as anything else would move 24
  approvals by three hours.

`revenue` stays `DOUBLE PRECISION` rather than becoming `NUMERIC`. SQLite holds
it as `REAL`, and a conversion on the way in would mean the daily comparison is
measuring the conversion. When the writer moves, that is the moment to decide
whether money should stop being a float — not now, mid-copy.

`celebrated_milestones.id` and `report_history.id` are **carried from SQLite,
not generated here**, for `app.stock_movements`' reason: a sequence would give
the same row two names and the comparison would be meaningless before it began.
"""
from __future__ import annotations

from alembic import op

revision = "0009_bot_state"
down_revision = "0008_operational_history"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── who may use the bot ──────────────────────────────────────────────────
    op.execute(
        """
        CREATE TABLE app.authorized_users (
            -- BIGINT, not INTEGER: Telegram ids passed 2^31 in 2021.
            user_id      BIGINT PRIMARY KEY,
            username     TEXT,
            first_name   TEXT,
            last_name    TEXT,
            -- pending | approved | denied | frozen | revoked. No CHECK: a
            -- constraint the other store lacks makes this a filter.
            status       TEXT,
            requested_at TIMESTAMPTZ,
            reviewed_at  TIMESTAMPTZ,
            reviewed_by  BIGINT,
            last_activity TIMESTAMPTZ,
            denial_count INTEGER
        )
        """
    )

    # ── what each of them chose ──────────────────────────────────────────────
    op.execute(
        """
        CREATE TABLE app.user_preferences (
            user_id               BIGINT PRIMARY KEY,
            default_source        TEXT,
            default_report_type   TEXT,
            timezone              TEXT,
            created_at            TIMESTAMPTZ,
            updated_at            TIMESTAMPTZ,
            default_date_range    TEXT,

            -- INTEGER in SQLite, which has no boolean. The weekly report reads
            -- it to decide who is written to, so it is a decision and not a
            -- number.
            notifications_enabled BOOLEAN,

            -- en | uk | ru. A stored choice overrides the default for good;
            -- see core/bot_prefs.default_language_for.
            language              TEXT
        )
        """
    )

    # ── which milestones have already been announced ─────────────────────────
    op.execute(
        """
        CREATE TABLE app.celebrated_milestones (
            -- From SQLite's AUTOINCREMENT. Not generated here: see the module
            -- docstring.
            id               BIGINT PRIMARY KEY,
            period_type      TEXT   NOT NULL,
            period_key       TEXT   NOT NULL,
            milestone_amount BIGINT NOT NULL,

            -- REAL on the other side. Left a float on purpose — a conversion
            -- here would be the thing the daily comparison measures.
            revenue          DOUBLE PRECISION NOT NULL,

            celebrated_at    TIMESTAMPTZ,

            -- The rule that stops the same milestone being celebrated twice.
            -- SQLite declares it, so it travels: a copy that dropped it would
            -- accept a duplicate the source would have refused.
            CONSTRAINT celebrated_milestones_once
                UNIQUE (period_type, period_key, milestone_amount)
        )
        """
    )

    # ── what was asked for and when ──────────────────────────────────────────
    op.execute(
        """
        CREATE TABLE app.report_history (
            id          BIGINT PRIMARY KEY,
            user_id     BIGINT NOT NULL,
            report_type TEXT   NOT NULL,

            -- TEXT on both sides. SQLite holds them as text and narrowing them
            -- to DATE here would make this a filter rather than a copy.
            start_date  TEXT   NOT NULL,
            end_date    TEXT   NOT NULL,

            source      TEXT,
            created_at  TIMESTAMPTZ
        )
        """
    )
    # No foreign key to app.user_preferences, though SQLite declares one: the
    # standing rule since revision 0002. A row the source accepted must land.

    # `comment.replace` is not decoration: the first draft of this loop put
    # "a human's decision" inside a single-quoted SQL literal and the migration
    # failed at `COMMENT ON` — on a real server, after four tables had already
    # been created. Nothing local caught it, because every test here parses the
    # DDL and none of them executes it.
    for table, comment in (
        ("authorized_users",
         "Replicated from data/bot.db. Who may use the bot — a human's "
         "decision, in one place on earth until this table existed."),
        ("user_preferences",
         "Replicated from data/bot.db. Language and notification choices; "
         "read by the weekly report through core/bot_prefs.py."),
        ("celebrated_milestones",
         "Replicated from data/bot.db. Which milestones have already been "
         "announced — losing it congratulates the shop twice."),
        ("report_history",
         "Replicated from data/bot.db. What was asked for and when."),
    ):
        escaped = comment.replace("'", "''")
        op.execute(f"COMMENT ON TABLE app.{table} IS '{escaped}'")


def downgrade() -> None:
    for table in (
        "report_history", "celebrated_milestones", "user_preferences",
        "authorized_users",
    ):
        op.execute(f"DROP TABLE IF EXISTS app.{table}")
