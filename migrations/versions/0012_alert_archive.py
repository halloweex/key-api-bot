"""Step 03 of the alerts rework: the archive of what the alerting did.

Revision ID: 0012_alert_archive
Revises: 0011_buyers_lines_vitrina
Created: 2026-08-29

Until now the fact "alert X was sent at T and reached N people" lived only in
docker logs with a 250 MB rotation — the one delivery ledger in the system is
`weekly_report_sends`, and its shape (a row written only on delivery, read as
a gate before acting) is the model these two tables follow.

TWO TABLES, ONE PER QUESTION

`app.alert_series` answers "what is the state of this condition?" — one row
per canonical condition key from core/alerting.py's REGISTRY, carrying the
lifecycle (firing → resolved, acknowledged for П4) and the counters. Repeats
are counters here and never event rows: the warehouse validator re-raises a
standing condition every two minutes, and the June storms measured ~545
failing ticks a day for three weeks — an event row per suppressed repeat
would be write amplification in the store that is itself the subject of half
the alerts.

`app.alert_events` answers "what happened, when?" — one row per thing that
actually happened: a delivered fire, an escalation, a resolution, an
acknowledgement, a trigger execution. At the measured alert rate (7 CRITICAL
days in the last 30) this grows by a handful of rows a day.

WHAT THIS MIGRATION DELIBERATELY DOES NOT ADD

No REQUIRED_REVISION gate protects these tables' accessor. The deploy window
between `migrate` and the container swap is exactly when rebuilds and
reconciliations start failing — the moment the alerting machinery is most
needed — and a version gate on the alerting store would turn a deploy-order
mistake into deaf alerting. `core/alert_archive.py` tolerates the tables'
absence (logs once, stands down) instead of demanding their presence.

No foreign keys, no CHECK constraints: the writers are single statements in
one module, pinned by the suite; a constraint here would only fail a deploy
the tests already fail. `condition_key` is deliberately NOT constrained to
the code's REGISTRY — an unregistered key must still be archivable, because
"somebody alerted without declaring the condition" is precisely a fact worth
keeping.
"""
from alembic import op

revision = "0012_alert_archive"
down_revision = "0011_buyers_lines_vitrina"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE app.alert_series (
            -- The canonical condition key (core/alerting.py REGISTRY).
            condition_key    TEXT        PRIMARY KEY,

            -- condition | event, as the emitter declared it. Events never
            -- leave state='event'; only conditions carry the lifecycle.
            kind             TEXT        NOT NULL,

            -- firing | resolved | event
            state            TEXT        NOT NULL,

            first_fired_at   TIMESTAMPTZ NOT NULL,
            last_fired_at    TIMESTAMPTZ NOT NULL,
            fired_count      INTEGER     NOT NULL DEFAULT 1,

            -- Total repeats the Gate swallowed for this series. Flushed when
            -- the next message is delivered (they ride its reminder line), so
            -- pure suppression costs zero Postgres writes.
            suppressed_count INTEGER     NOT NULL DEFAULT 0,

            resolved_at      TIMESTAMPTZ,

            -- П4, when the owner decides who may write these.
            acknowledged_by  TEXT,
            acknowledged_at  TIMESTAMPTZ,
            acknowledged_reason TEXT,

            -- KS_INSTANCE of the process that last fired it.
            instance         TEXT,

            updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )

    op.execute(
        """
        CREATE TABLE app.alert_events (
            id            BIGSERIAL   PRIMARY KEY,
            condition_key TEXT        NOT NULL,

            -- fired | escalated | resolved | acknowledged | trigger_executed
            event_type    TEXT        NOT NULL,

            at            TIMESTAMPTZ NOT NULL DEFAULT now(),
            instance      TEXT        NOT NULL,

            -- Admins the message actually reached. NULL for event types that
            -- deliver nothing (trigger_executed, acknowledged).
            delivered_to  INTEGER,

            -- The body as sent (post-clamp). One condition of a multi-
            -- condition raise carries it; the siblings reference the same
            -- moment by (at, instance).
            message       TEXT,

            -- Room for the diagnose trigger's evidence snapshots (track Б).
            context       JSONB
        )
        """
    )

    op.execute(
        "CREATE INDEX idx_alert_events_key_at ON app.alert_events "
        "(condition_key, at DESC)"
    )
    op.execute("CREATE INDEX idx_alert_events_at ON app.alert_events (at DESC)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.alert_events")
    op.execute("DROP TABLE IF EXISTS app.alert_series")
