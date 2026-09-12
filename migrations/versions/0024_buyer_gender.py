"""The Postgres twin of `buyer_gender`: inferred customer gender.

Revision ID: 0024_buyer_gender
Revises: 0023_levels_and_areas
Created: 2026-09-12

REPLICATED, NOT MIRRORED — and this is the same distinction `app.manager_
classifications` turns on. Everything in `core/pg_landing.py` is a mirror: one
KeyCRM payload parsed once and handed to two stores, recoverable from the source
if Postgres is ever lost. Gender is not in any payload. KeyCRM has no gender
field at all — checked against the live buyer endpoint, whose keys are id,
full_name, birthday, phone, email, note, picture, image, orders_sum, discount,
currency, orders_count, has_duplicates, manager_id, deleted_at, created_at,
updated_at, loyalty, shipping — so a payload-fed mirror could never supply this
column, and `core/pg_operational.py` copies what DuckDB decided instead.

WHY IT IS NOT A COLUMN ON `bronze.buyers`

`BUYER_COLUMNS` in `core/pg_buyers.py` is the shared contract compared at a
tolerance of ZERO by the daily mirror check. A derived column there is either
outside that tuple — invisible to the comparison, so nothing would ever notice
it rotting — or inside it, and then the reconciliation compares a value KeyCRM
cannot serve and reports a difference the mirror itself created. Note that the
sync would NOT wipe it: `INSERT OR REPLACE` in DuckDB names a column subset and
`_UPSERT_BUYER` generates its SET over `BUYER_COLUMNS`, so an unlisted column
survives both writers. The reason to keep it out is the comparison, not the
write.

WHY IT IS NOT A COLUMN ON `app.customer_profile`

That one is genuinely destroyed: `core/pg_vitrina.py` TRUNCATEs and rebuilds it
whole on the two-minute warehouse tick, so a human's correction would live about
ninety seconds. The витрина also holds facts derivable from one Silver snapshot,
which is what licenses its own zero-tolerance check; an inference is neither.

THE COLUMNS, AND WHY EACH EXISTS

`gender` is NULL for "could not be decided" — 1.38% of the base on the day this
shipped. A company, a marketplace handle, a name whose two tokens are both
unknown. `method` says which layer decided and `confidence` how far to trust it,
so a campaign that spends money can demand certain/high while a dashboard tile
takes everything; a boolean could express none of that.

`override_by_human` is the lesson of `managers.is_retail`, which was recomputed
on every sync until a human's classification became impossible to keep and eight
managers sold ₴3.1M into the wrong bucket. The backfill never touches a row
where it is TRUE.

`decided_from` names the ROLE a verdict came from — "given", "patronymic",
"surname" — and never the token. This value is disclosable on a subject access
request, and this repository is public and has been scrubbed of customer data
once already.

PRIMARY KEY HERE, NONE IN DUCKDB. Postgres has no reason to refuse one and it is
what makes the row-by-row comparison meaningful. DuckDB's copy omits it because
an ART index there disables vacuum for the whole table, and that table is
rewritten wholesale whenever RULES_VERSION moves.
"""
from alembic import op


revision = "0024_buyer_gender"
down_revision = "0023_levels_and_areas"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app.buyer_gender (
            buyer_id          INTEGER     PRIMARY KEY,
            gender            TEXT,
            method            TEXT        NOT NULL,
            confidence        TEXT,
            decided_from      TEXT,
            rules_version     INTEGER     NOT NULL,
            override_by_human BOOLEAN     NOT NULL DEFAULT FALSE,
            decided_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

            CONSTRAINT buyer_gender_gender_check
                CHECK (gender IS NULL OR gender IN ('f', 'm')),
            CONSTRAINT buyer_gender_confidence_check
                CHECK (confidence IS NULL
                       OR confidence IN ('certain', 'high', 'medium'))
        )
        """
    )
    # The SMS audience is the one consumer, and it asks "everybody of this
    # gender", never "the gender of this buyer". A partial index on the two
    # real values keeps the 1.38% of NULLs out of it.
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_buyer_gender_gender "
        "ON app.buyer_gender (gender) WHERE gender IS NOT NULL"
    )
    op.execute(
        "COMMENT ON TABLE app.buyer_gender IS "
        "'Inferred customer gender, replicated from DuckDB (core/pg_operational.py). "
        "KeyCRM has no gender field, so this can never be a mirror. NULL means the "
        "classifier refused, which is a decision and not an absence. Never a column "
        "on bronze.buyers (zero-tolerance comparison) nor on app.customer_profile "
        "(TRUNCATEd every warehouse tick).'"
    )
    op.execute(
        "COMMENT ON COLUMN app.buyer_gender.override_by_human IS "
        "'TRUE means a human set this and the backfill must never overwrite it — "
        "the managers.is_retail lesson.'"
    )
    op.execute(
        "COMMENT ON COLUMN app.buyer_gender.decided_from IS "
        "'The ROLE the verdict came from (given/patronymic/surname/agreement), "
        "never the token. This value is disclosable on a subject access request.'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.buyer_gender")
