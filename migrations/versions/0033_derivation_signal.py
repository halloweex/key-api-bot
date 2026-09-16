"""The Postgres derivation's own signal and its own journal — no callers yet.

Revision ID: 0033_derivation_signal
Revises: 0032_chain_watermarks
Created: 2026-09-16

Chain 2, step 2. Today Postgres recomputes Silver, Gold and the customer
profile only when DuckDB's warehouse tick asks it to: `_rebuild_postgres_layers`
is called from `_run_warehouse_refresh`, after DuckDB's dirty flag has been
peeked and cleared, and a retry it owes is held in process memory. A deploy
between the two loses the rebuild; a DuckDB refresh that errors vetoes it. For
Postgres to derive when DuckDB stops, it needs a reason to derive that belongs
to Postgres.

`meta.derivation_signal` is that reason: a pair of counters per layer.

- A writer that lands rows raises `requested` **inside its own transaction**, so
  there is never a mark without the rows or rows without the mark.
- The derivation reads `requested` inside the snapshot it rebuilds from, and on
  success sets `built` to what it saw — never higher. An increment its snapshot
  could not see stays owed, which is exact without comparing any clock.

Seeded `('warehouse', 1, 0)`: owed from the start, so the first consumer run
rebuilds rather than trusting a table it never built.

`meta.derivation_runs` is the journal: one row per attempt, with row counts,
the validation verdict and the error. `app.warehouse_refreshes` was the
alternative and is refused — it has no id allocator, and the hourly copy appends
to it above `MAX(id)` in the one transaction that carries every irreplaceable
table, so a collision there would abort all of it.

WHY NEITHER LIVES IN `app.sync_metadata`

That table is full-replaced out of DuckDB every hour (revision 0032 is the same
lesson): an unconditional clear by a third party. Both tables are in `meta`,
where no copy and no comparison reaches — pinned by a test.

THE BOUND IS A COUNT, NOT AN AGE

CLAUDE.md: age is never the retention key here. `core/pg_derivation.record_run`
deletes everything more than `RUNS_KEPT` ids below the row it has just written,
in the same transaction — the bound declared where the thing is created.

Nothing calls either table in this revision. Rollback is `alembic downgrade
0032_chain_watermarks` with the web image that expects it; both tables are
empty until a later step writes them.
"""
from alembic import op

revision = "0033_derivation_signal"
down_revision = "0032_chain_watermarks"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS meta.derivation_signal (
            layer        TEXT PRIMARY KEY,
            requested    BIGINT NOT NULL,
            built        BIGINT NOT NULL,
            requested_at TIMESTAMPTZ,
            built_at     TIMESTAMPTZ,
            CHECK (built <= requested)
        )
    """)
    op.execute("""
        INSERT INTO meta.derivation_signal (layer, requested, built, requested_at)
        VALUES ('warehouse', 1, 0, now())
        ON CONFLICT (layer) DO NOTHING
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS meta.derivation_runs (
            id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            layer             TEXT NOT NULL,
            trigger           TEXT NOT NULL,
            started_at        TIMESTAMPTZ NOT NULL,
            ended_at          TIMESTAMPTZ,
            requested_seen    BIGINT,
            silver_rows       INTEGER,
            gold_rows         INTEGER,
            gold_cells        INTEGER,
            profile_rows      INTEGER,
            validation_passed BOOLEAN,
            validation        JSONB,
            error             TEXT
        )
    """)
    op.execute("""
        COMMENT ON TABLE meta.derivation_signal IS
        'Why Postgres owes a rebuild of its derived layers: requested is raised '
        'inside the writers'' transactions, built is set by the derivation to '
        'what its snapshot saw. See revision 0033.'
    """)
    op.execute("""
        COMMENT ON TABLE meta.derivation_runs IS
        'One row per Postgres derivation attempt. Bounded by count in '
        'core/pg_derivation.record_run, never by age.'
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS meta.derivation_runs")
    op.execute("DROP TABLE IF EXISTS meta.derivation_signal")
