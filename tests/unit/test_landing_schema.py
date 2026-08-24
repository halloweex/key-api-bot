"""The first landing tables in Postgres, and the decisions inside them.

Verified against a real server before this file was written:
`postgres:17.2-alpine` with the real `postgres/initdb/*.sql`, the revision
applied as `ks_app`, and then the **actual production catalogue** from the
2026-08-24 backup loaded into it — 28 categories and 1,004 products, NULLs,
`Decimal` prices and Cyrillic names included. Read back and compared row by
row against DuckDB: **0 differing of 28, 0 differing of 1,004**.

What is worth pinning in a suite that must run without a server is the reasons,
because each one is a decision somebody could reverse while thinking they were
tightening the schema.
"""
from __future__ import annotations

import importlib.util
import inspect
import re
from pathlib import Path

from core import pg

REPO = Path(__file__).resolve().parents[2]
_PATH = REPO / "migrations" / "versions" / "0002_landing_catalog.py"

# The DDL alone. Asserting against the whole file would match the docstring
# that *explains* a decision — "NO FOREIGN KEYS, DELIBERATELY" contains the
# words a test looking for a foreign key is looking for, which is how the first
# version of this file failed.
_spec = importlib.util.spec_from_file_location("_rev0002", _PATH)
_rev = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_rev)
MIGRATION = inspect.getsource(_rev.upgrade)
DOWN = inspect.getsource(_rev.downgrade)


class TestItGoesInBronze:
    def test_both_tables_are_in_bronze(self):
        """`postgres/initdb/30-app.sql` draws the line: `app` is for what
        nothing can decide again — users, goals, manager classifications. The
        catalogue is landing from KeyCRM and rebuildable, which is `bronze`."""
        assert "CREATE TABLE bronze.categories" in MIGRATION
        assert "CREATE TABLE bronze.products" in MIGRATION
        assert "CREATE TABLE app." not in MIGRATION

    def test_it_creates_no_schema(self):
        """The platform owns schemas, the application owns tables — rule 11,
        and `bronze` is created by initdb."""
        assert "CREATE SCHEMA" not in MIGRATION


class TestTheTypesMirrorDuckDB:
    def test_price_is_numeric_with_the_same_scale(self):
        """DuckDB stores DECIMAL(12,2). A float here would make the daily
        reconciliation report differences that exist only in binary
        representation."""
        assert "NUMERIC(12,2)" in MIGRATION
        assert "double precision" not in MIGRATION.lower()
        assert "REAL" not in MIGRATION

    def test_text_columns_carry_no_invented_length(self):
        """DuckDB imposes none. A VARCHAR(n) here could reject a row DuckDB
        accepted — the longest product name today is 147 characters, and
        nothing says tomorrow's is shorter."""
        assert not re.search(r"VARCHAR\s*\(", MIGRATION, re.I)
        assert "TEXT" in MIGRATION

    def test_name_is_not_null_on_both_sides(self):
        """DuckDB declares `name VARCHAR NOT NULL`. Matching it keeps the two
        stores rejecting exactly the same payloads."""
        assert MIGRATION.count("NOT NULL") >= 4

    def test_the_nullable_columns_stay_nullable(self):
        """Measured on production: category_id NULL in 567 products, brand in
        400, sku in 54, parent_id in 8 categories. A NOT NULL on any of them
        would reject more than half the catalogue."""
        for col in ("parent_id", "category_id", "brand", "sku"):
            line = next(l for l in MIGRATION.splitlines() if re.search(rf"\b{col}\s+\w", l))
            assert "NOT NULL" not in line, f"{col} must stay nullable"


class TestNoForeignKeys:
    def test_none_are_declared(self):
        """Both would hold today — 0 orphans on `products.category_id` and 0
        on `categories.parent_id`, measured on the 2026-08-24 backup.

        They are still not declared. A mirror that rejects a row DuckDB
        accepted is not a mirror but a filter, and the reconciliation would
        then report a difference caused by this schema rather than by the
        data. DuckDB enforces no FK at all, so a delta sync carrying products
        whose categories have not arrived is an ordinary event.
        """
        assert "REFERENCES" not in MIGRATION.upper()
        assert "FOREIGN KEY" not in MIGRATION.upper()

    # There was a test here asserting the docstring records the orphan count,
    # so that whoever hardens this later finds the evidence. It was deleted:
    # a test that matches a phrase in prose breaks on rewording and stops
    # nothing, and the measurement is in the revision's docstring and in the
    # PR either way. Pin behaviour, not sentences.


class TestBookkeepingIsNamedApart:
    def test_the_mirror_stamps_mirrored_at_not_synced_at(self):
        """Two correct copies differ on their own bookkeeping by
        construction. A shared name would invite comparing them and finding
        every row in disagreement — the same reasoning that keeps these
        columns out of `core.landing_rows`."""
        assert "mirrored_at" in MIGRATION
        assert "synced_at" not in MIGRATION, (
            "the mirror must not reuse DuckDB's bookkeeping column name"
        )

    def test_the_shared_row_still_carries_neither(self):
        from core.landing_rows import CATEGORY_COLUMNS, PRODUCT_COLUMNS

        for col in ("mirrored_at", "synced_at"):
            assert col not in CATEGORY_COLUMNS
            assert col not in PRODUCT_COLUMNS


class TestTheChainIsIntact:
    def test_it_follows_the_first_revision(self):
        assert _rev.down_revision == "0001_mirror_state"
        assert _rev.revision == "0002_landing_catalog"

    def test_it_is_in_the_chain_the_code_requires(self):
        """This revision is no longer head — 0003 added the orders tables —
        so what matters here is that it is still an ancestor of what the code
        requires. `test_pg_plumbing` owns the head-vs-REQUIRED_REVISION pin;
        asserting head again here only meant every future revision broke a
        test about the catalogue."""
        from tests.unit.test_pg_plumbing import _revisions

        revs = _revisions()
        assert _rev.revision in revs
        seen, cursor = set(), pg.REQUIRED_REVISION
        while cursor:
            seen.add(cursor)
            cursor = revs.get(cursor)
        assert _rev.revision in seen

    def test_it_can_be_undone(self):
        assert "DROP TABLE IF EXISTS bronze.products" in DOWN
        assert "DROP TABLE IF EXISTS bronze.categories" in DOWN
