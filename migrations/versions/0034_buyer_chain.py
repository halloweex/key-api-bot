"""Chain 4's lock on older images — three table comments and nothing else.

Revision ID: 0034_buyer_chain
Revises: 0033_derivation_signal
Created: 2026-10-01

WHY A REVISION WITH NO SCHEMA IN IT

Chain 4 (`core/pg_buyers_write.py`) moves the writes of `bronze.buyers`,
`bronze.buyer_contacts` and `app.buyer_gender` to Postgres. An image older than
the chain does not know it: its hourly `replicate_operational` would full-
replace `app.buyer_gender` out of a DuckDB that stopped receiving buyers, and
its buyers mirror would ship DuckDB's copy over the chain's rows. Owner rows
stand most of that down since PR-2 (DN-22b, `chain_latch.owned_tables`), but
an image older than those is not held by anything that reads them.

`REQUIRED_REVISION` is. Every path that writes these three tables calls
`require_revision()` before it writes — the buyers mirror, the backfills and
the hourly ids-diff, the replication that ships `app.buyer_gender`, the
chain's own writers — and so do the session read under `KS_USER_STORE=postgres`
and the bot store at `initialise()`. An image expecting 0033 refuses a database
at 0034. (Not every Postgres path gates: the catalogue and orders mirrors and
the alert journal do not, which is why the claim is about these tables.) Owner
decision 16: this revision exists to be that lock, and goes in directly before
the flip.

What the lock does NOT cover, said where it is created:

- **Images older than v3.0.249.** Their buyers mirror, `backfill_buyers` and
  `hourly_ids_diff` never called `require_revision`, so they would still ship
  DuckDB's buyers over the chain's. After this revision no image below
  v3.0.249 is a rollback target, whatever the database's revision.
- **An image that does check is not merely kept from writing.** Its session
  read raises under `KS_USER_STORE=postgres`, so the dashboard answers 401 to
  everybody, and the bot refuses to start. An image-only rollback therefore
  needs `alembic downgrade 0033_derivation_signal` first, run with the NEW
  migrate image (the old one does not know 0034), and then all THREE images
  moved back — keycrm-migrate with web and bot. Left new, `up -d` re-runs its
  `alembic upgrade head` (web and bot wait on it) and puts 0034 back, locking
  the old images out again. And only before the flip: once chain 4 has written
  Postgres, going below 0034 hands its tables to a build that will overwrite
  them. The way back after the flip is `scripts/chain_copy_back.py buyers`,
  never a downgrade.
- **A locked-out image still writes DuckDB.** An image from v3.0.249 on that
  predates chain 4, rolled back without the downgrade, cannot reach these
  tables, but its incremental sync still runs: `upsert_buyers` commits each
  portion to DuckDB before the mirror's revision check refuses. DuckDB then
  holds buyers the chain does not, and after the flip `chain_copy_back.py
  buyers --handover` refuses on exactly those ids. So after the flip, no image
  rollback at all — not even a brief one.

The comments say who writes each table now. The downgrade puts back exactly
what 0011 and 0024 wrote — `bronze.buyer_contacts` never had one, so it is
cleared rather than given an invented past.
"""
from alembic import op

revision = "0034_buyer_chain"
down_revision = "0033_derivation_signal"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "COMMENT ON TABLE bronze.buyers IS "
        "'Buyers as KeyCRM serves them, parsed once by core.landing_rows. Under "
        "KS_WRITE_BUYERS=postgres (chain 4) written here by "
        "core/pg_buyers_write.py and nowhere else; otherwise the mirror of "
        "DuckDB buyers (core/pg_buyers.py). Both run core.pg_buyer_rows. "
        "Nothing deletes a buyer.'"
    )
    op.execute(
        "COMMENT ON TABLE bronze.buyer_contacts IS "
        "'A buyer''s phones and emails, keyed on the natural triple. Replaced "
        "whole on every write of that buyer, by whichever writer holds "
        "bronze.buyers (chain 4 or the mirror), so a list that shrank keeps no "
        "old number.'"
    )
    op.execute(
        "COMMENT ON TABLE app.buyer_gender IS "
        "'Inferred customer gender. Under KS_WRITE_BUYERS=postgres (chain 4) "
        "written here by core/pg_buyers_write.py, beside the name it was decided "
        "from; otherwise replicated from DuckDB (core/pg_operational.py). "
        "KeyCRM has no gender field, so this can never be a mirror. NULL means "
        "the classifier refused, which is a decision and not an absence. A row "
        "with override_by_human is never rewritten.'"
    )


def downgrade() -> None:
    # 0011's text, verbatim.
    op.execute(
        "COMMENT ON TABLE bronze.buyers IS "
        "'Mirror of DuckDB buyers: the same parsed Buyer batch handed to two "
        "stores in the same call (core/pg_buyers.py). Delta-fed like orders; "
        "history arrives via the ids-diff backfill, and the comparison is "
        "gated on backfilled_at.'"
    )
    # Never had one.
    op.execute("COMMENT ON TABLE bronze.buyer_contacts IS NULL")
    # 0024's text, verbatim.
    op.execute(
        "COMMENT ON TABLE app.buyer_gender IS "
        "'Inferred customer gender, replicated from DuckDB (core/pg_operational.py). "
        "KeyCRM has no gender field, so this can never be a mirror. NULL means the "
        "classifier refused, which is a decision and not an absence. Never a column "
        "on bronze.buyers (zero-tolerance comparison) nor on app.customer_profile "
        "(TRUNCATEd every warehouse tick).'"
    )
