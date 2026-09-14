"""The H3 staging-merge subsystem is gone, and must not half-come-back.

`bronze_order_events` was built 2026-04-19 as the foundation of a
staging-merge ingest: sync would write only an append-only event log, and a
promotion job would be the single writer of `orders`. It reached Phase 5 and
`SYNC_MODE=staging` was never switched on in production for a single day. The
shadow write in legacy mode was made opt-in on 2026-05-19 and defaulted off,
and the table has held **0 rows** ever since — verified on the 2026-09-14
production backup and, live, by the invariant check passing that morning.

It is also the standing counter-example this codebase cites when it explains
why `app.order_versions` records a *change* rather than an *observation*: this
table keyed on the observation, wrote ~150 000 rows a day and took the database
to 43 GB. That argument outlives the table and is deliberately left in place in
`test_order_versions.py`.

WHAT THESE TESTS ACTUALLY PROTECT

Not the deletion — a deletion does not rot. The two things that can:

1. **The compaction exclusion.** It is what physically removes the table. The
   weekly compact exports every table it finds in the *source* database and
   imports it into a fresh one built from `_init_schema()`. With the DDL gone
   and the name absent from `DERIVED_TABLES`, the import runs
   `INSERT INTO "bronze_order_events"` against a schema that no longer defines
   it — an error that is not a duplicate-key error, so phase 2 exits 1 and the
   whole weekly compact aborts, taking the off-site export with it. That is
   `orders_v2`' exact recorded failure, and this is the guard it never had.

2. **The symbols.** A reintroduced `should_write_bronze` or `append_bronze_events`
   would be a write path to a table that no longer exists.
"""
from __future__ import annotations

import ast
import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parents[2]

# Every name the subsystem owned. A reference to any of them outside a comment
# is either dead code or a write to a table that is not there.
RETIRED = (
    "append_bronze_events",
    "get_bronze_stats",
    "promote_bronze_to_orders",
    "backfill_bronze_from_orders",
    "prune_bronze_events",
    "replay_bronze_events",
    "evaluate_bronze_invariant",
    "BRONZE_INVARIANT_THRESHOLDS",
    "should_write_bronze",
    "legacy_bronze_shadow",
    "SyncConfig",
)

# Searched in code only. `bronze_orders` (the warehouse's Bronze *layer* row
# count on `warehouse_refreshes`) and `bronze.orders` (the Postgres landing
# schema) are unrelated and must not be caught — which is why this matches
# whole identifiers out of the parse tree rather than the word "bronze".
SOURCE_DIRS = ("core", "web", "bot", "scripts", "deploy")


def _python_files():
    for d in SOURCE_DIRS:
        root = REPO / d
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            # The one file that must still name the table: its exclusion list
            # is what stops the weekly compact recreating it, and
            # `TestTheCompactionStillKnowsAboutIt` asserts the name is there.
            if path.name == "compact_duckdb.py":
                continue
            yield path


class TestTheSubsystemIsGone:
    def test_no_module_defines_or_calls_the_retired_names(self):
        """By tree, not by grep.

        The repository still discusses `bronze_order_events` in prose — the
        43 GB argument is load-bearing in two docstrings — and a text search
        cannot tell a name a module *calls* from a name it *explains*. That
        distinction has cost this suite a false failure before.
        """
        offenders = []
        for path in _python_files():
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except SyntaxError:                       # pragma: no cover
                continue
            docstrings = set()
            for node in ast.walk(tree):
                if isinstance(node, (ast.Module, ast.ClassDef,
                                     ast.FunctionDef, ast.AsyncFunctionDef)):
                    doc = ast.get_docstring(node, clean=False)
                    if doc:
                        docstrings.add(doc)
            for node in ast.walk(tree):
                name = None
                if isinstance(node, ast.Name):
                    name = node.id
                elif isinstance(node, ast.Attribute):
                    name = node.attr
                elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                                       ast.ClassDef)):
                    name = node.name
                elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                    # A string naming the table is a reference only when it is
                    # USED as a name: the bare identifier, or SQL around it.
                    # Prose is not — `mirror_reconciliation` cites the 43 GB
                    # incident in the flood alert's own description, which is
                    # the sentence a human reads when that check fires and is
                    # still true. Both halves of this distinction are real and
                    # were found by this test failing on the first run.
                    if node.value in docstrings:
                        continue
                    bare = node.value.strip() == "bronze_order_events"
                    sql = re.search(
                        r"\b(FROM|INTO|UPDATE|TABLE|JOIN)\s+\"?bronze_order_events",
                        node.value, re.I)
                    if bare or sql:
                        offenders.append(
                            f"{path.relative_to(REPO)}:{node.lineno} "
                            f"{'name' if bare else 'SQL'}")
                    continue
                if name in RETIRED:
                    offenders.append(
                        f"{path.relative_to(REPO)}:{node.lineno} {name}")
        assert offenders == [], (
            "the H3 subsystem is referenced by live code:\n  "
            + "\n  ".join(sorted(offenders))
        )

    def test_the_schema_no_longer_declares_it(self):
        src = (REPO / "core" / "duckdb_store.py").read_text(encoding="utf-8")
        for obj in ("bronze_order_events", "seq_bronze_order_events_id",
                    "idx_bronze_processed_at", "idx_bronze_order",
                    "idx_bronze_event_ts"):
            assert f"CREATE TABLE IF NOT EXISTS {obj}" not in src
            assert f"CREATE SEQUENCE IF NOT EXISTS {obj}" not in src
            assert f"CREATE INDEX IF NOT EXISTS {obj}" not in src


class TestTheCompactionStillKnowsAboutIt:
    """The one thing that must NOT be tidied up yet.

    The table is still in the production database — a DDL removal does not drop
    a table, and this project deliberately does not drop them by hand. The
    exclusion is what makes the next weekly compact build a database without
    it. Remove the exclusion before that compact has run and the compact
    aborts; remove it after, and nothing happens. The comment in
    `compact_duckdb.py` says which, and this pins it.
    """

    def test_it_is_excluded_from_the_export(self):
        src = (REPO / "scripts" / "compact_duckdb.py").read_text(encoding="utf-8")
        block = src[src.index("DERIVED_TABLES = frozenset({"):]
        block = block[:block.index("})")]
        assert '"bronze_order_events",' in block, (
            "without this the weekly compact exports the table and then fails "
            "to import it into a schema that no longer defines it — aborting "
            "the compact and the off-site export behind it"
        )

    def test_its_sequence_is_not_restored(self):
        """`seq_table_map` names a table for each sequence it repairs. Leaving
        the entry makes the restore query a table that is not there."""
        src = (REPO / "scripts" / "compact_duckdb.py").read_text(encoding="utf-8")
        assert '"seq_bronze_order_events_id"' not in src


class TestTheLessonOutlivesTheTable:
    def test_order_versions_still_cites_the_43gb_incident(self):
        """The reason `app.order_versions` records a change and not an
        observation. Deleting the table does not delete the evidence for the
        rule it taught."""
        src = (REPO / "tests" / "unit" / "test_order_versions.py").read_text(
            encoding="utf-8")
        assert "43 GB" in src
