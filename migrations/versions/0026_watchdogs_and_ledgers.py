"""The Postgres twins of the three watchdog samples and the two send ledgers.

Revision ID: 0026_watchdogs_and_ledgers
Revises: 0025_forecast_tables
Created: 2026-09-14

FIVE TABLES, ONE SHAPE, AND THAT IS WHY THEY TRAVEL TOGETHER

Twelve DuckDB tables still have no Postgres home. They do not divide usefully
by subject — a disk sample and a send ledger have nothing to say to each other
— but they divide exactly by *shipping mechanism*, and the mechanism is the
part that can be wrong. These five all ship as a full replace: every one is
rewritten whole or upserted over a fixed key by its DuckDB writer, so
`_FULL_REPLACE` describes them without inventing a shape. The tables that need
a watermark (`warehouse_refreshes`, `reconciliation_log`), a parent's watermark
(the three quality-journal tables), a key filter (`sync_metadata`) or a shared
parse (`offers`) each bring a different one, and each gets its own revision so
that a defect in one cannot be attributed to another.

ONE NEW IDEA CAME WITH THEM, AND IT WAS NOT PLANNED

This revision was drafted claiming these five needed "no new machinery at all".
That was wrong, and the three watchdogs are why: they are the first replicated
tables whose DuckDB writer *deletes on a schedule of its own*. A full replace
leaves the two copies identical at the end of every run, so an orphan can only
be a row that left DuckDB after the copy — and here that is exactly what the
retention sweep does. `mirror_orphan_rows` is computed with no grace and no
watermark, so roughly one or two rows would have been WARN every morning, a
finding invented entirely by the copy. `MirroredTable.prunes_by_age` is the
answer and `mirror_pruned_rows` is what it reports: INFO, counted, never a
page. See that field for why the rule is derived from the data rather than
given the retention period as a constant.

1 240 rows in total, measured on the 2026-09-13 backup: 56 + 554 + 630 + 5 + 0.

WHY SCHEMA `app`

None of the five comes from a KeyCRM payload — KeyCRM has no opinion about
this host's free disk, this container's cgroup, or which Monday a report was
delivered — so `bronze` is wrong. None can be recomputed from other Postgres
data either: a sample is a *reading taken at a moment that has passed*, which
is `app.inventory_sku_history`'s argument word for word, and a ledger row
records that a message was actually sent, which nothing but the sending can
establish. `app` is where what cannot be decided again lives.

THE LEDGERS ARE THE LOAD-BEARING TWO, DESPITE BEING THE SMALLEST

`weekly_report_sends` holds five rows and `traffic_report_sends` holds none,
and between them they are what keeps two daily-ticking jobs to one message a
week. Both jobs fire every morning and report the last *complete* week; six
firings out of seven find the week already recorded and return quiet. Lose the
ledger and every approved user gets the same report seven times; lose it in the
other direction — a row invented — and a week is never reported at all, which
is the failure the ledger was built to prevent.

They keep their own separate tables here, exactly as they do in DuckDB, and for
the reason written there: `weekly_report_sends.sales_type` means a sales type,
and "traffic" is not one.

TIMESTAMPTZ, AND MEASURE COLUMNS THAT STAY DOUBLE PRECISION

Every timestamp here is `TIMESTAMP WITH TIME ZONE` in DuckDB and is an instant
— `datetime.now(timezone.utc)` in the watchdogs, `CURRENT_TIMESTAMP` in the
ledgers — so `TIMESTAMPTZ` is the honest type and the only one that survives
the round trip through asyncpg unchanged.

The measures are deliberately **not** NUMERIC. `db_size_mb`, `disk_pct_used`,
`disk_free_gb`, `working_set_mb`, `page_cache_mb` and `limit_mb` are all DOUBLE
in DuckDB and arrive as Python floats; declaring them NUMERIC here would return
`Decimal` from asyncpg, and the daily comparison — which runs at a tolerance of
zero and only coerces the columns named in `MirroredTable.numeric` — would
report all 1 240 rows as differing every morning, forever. The rule is that the
Postgres type follows the DuckDB type unless something is being *gained*; here
nothing would be.

`revenue` on the two ledgers goes the other way and is NUMERIC(14, 2), because
DuckDB stores it as DECIMAL(14, 2) and hands back a `Decimal`. It is listed in
`numeric=` on both specs for the same reason, which is what makes the pair
agree.

`limit_mb` IS NULLABLE, AND NOT BECAUSE IT IS NULL TODAY

Production holds zero NULLs in it right now. It is nullable anyway, because
`core/memory_monitor.py` writes `None` whenever the cgroup reports `memory.max`
as the literal string `max` — an unlimited container. Declaring NOT NULL on the
strength of today's data would make the *first* such reading abort the whole
hourly replication transaction, taking the other twelve tables with it, on a
host that had just stopped enforcing a memory limit.

PRIMARY KEYS THE SOURCE DOES NOT DECLARE

The three watchdog tables carry no PRIMARY KEY in DuckDB — only a descending
index on `sampled_at`. Declaring one here is right: it is the natural key, the
comparison joins on it, and without it a duplicate would compare as a missing
row rather than as the duplicate it is. It is also a constraint the source has
never enforced, so it was measured before being declared — 0 duplicate keys
across all five tables on the 2026-09-13 production backup, including the
composite `(sampled_at, path_group)`, whose seven path groups share one
`sampled_at` per sweep by design.

RETENTION EXISTS HERE, AND IT RULES OUT A WATERMARK OUTRIGHT

Unlike every other table in `_FULL_REPLACE`, all three watchdog tables are
pruned on a schedule — the memory sweep every thirty minutes. That is precisely
why they may not use a watermark: `id > MAX(id)` and `date >= MAX(date)` can
only ever add rows, so a pruned sample would stay in Postgres for ever and grow
without bound. A full replace propagates the sweep in the same tick as
everything else. What it does *not* fix is the hour between that tick and the
next check, which is what `prunes_by_age` is for — see above.

NO GRANTS, NO DEFAULTS ON THE TIMESTAMPS

`initdb` grants `ks_readonly` SELECT on everything `ks_app` creates in `app`,
and `ks_app` owns what it creates — revision 0025's note, unchanged. The
timestamps carry no `DEFAULT now()` for 0025's reason too: the value being
copied is the one DuckDB stamped, and a default would quietly substitute the
replication's clock for the watchdog's, so the column would answer "when was
this copied" while being read as "when was this measured".
"""
from alembic import op


revision = "0026_watchdogs_and_ledgers"
down_revision = "0025_forecast_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── the three watchdog samples ──
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.disk_samples (
            sampled_at    TIMESTAMPTZ      NOT NULL,
            db_size_mb    DOUBLE PRECISION NOT NULL,
            disk_pct_used DOUBLE PRECISION NOT NULL,
            disk_free_gb  DOUBLE PRECISION NOT NULL,
            PRIMARY KEY (sampled_at)
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.data_dir_samples (
            sampled_at TIMESTAMPTZ NOT NULL,
            path_group VARCHAR     NOT NULL,
            bytes      BIGINT      NOT NULL,
            PRIMARY KEY (sampled_at, path_group)
        )
    """)
    # `limit_mb` nullable: see the module docstring — an unlimited cgroup
    # writes NULL, and it has simply not happened on this host yet.
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.memory_samples (
            sampled_at     TIMESTAMPTZ      NOT NULL,
            working_set_mb DOUBLE PRECISION NOT NULL,
            page_cache_mb  DOUBLE PRECISION NOT NULL,
            limit_mb       DOUBLE PRECISION,
            oom_kills      INTEGER          NOT NULL DEFAULT 0,
            PRIMARY KEY (sampled_at)
        )
    """)

    # ── the two send ledgers ──
    #
    # Separate tables rather than one with a discriminator, because
    # `sales_type` means a sales type and "traffic" is not one. Identical
    # shape is not a reason to merge them; it is a consequence of both
    # answering the same question about different reports.
    for table in ("weekly_report_sends", "traffic_report_sends"):
        op.execute(f"""
            CREATE TABLE IF NOT EXISTS app.{table} (
                week_start DATE          NOT NULL,
                sales_type VARCHAR       NOT NULL,
                revenue    NUMERIC(14, 2) NOT NULL,
                orders     INTEGER       NOT NULL,
                sent_at    TIMESTAMPTZ,
                PRIMARY KEY (week_start, sales_type)
            )
        """)

    # The watchdogs are read newest-first by their own jobs, and the
    # reconciliation reads them whole. The descending index is the DuckDB
    # original's, carried across so a future reader of either store finds the
    # same access path rather than discovering one store is slower.
    op.execute("CREATE INDEX IF NOT EXISTS idx_disk_samples_at "
               "ON app.disk_samples (sampled_at DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_data_dir_samples_at "
               "ON app.data_dir_samples (sampled_at DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_memory_samples_at "
               "ON app.memory_samples (sampled_at DESC)")


def downgrade() -> None:
    for table in ("disk_samples", "data_dir_samples", "memory_samples",
                  "weekly_report_sends", "traffic_report_sends"):
        op.execute(f"DROP TABLE IF EXISTS app.{table}")
