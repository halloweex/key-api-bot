"""`app.customer_profile` is neither rebuilt nor checked (owner's decision O6).

Nothing read the витрина — not a page, not the bot, not a script — and every
derivation rebuilt it between Gold and the validation, about a tenth of the
run's row writes; the daily `dq_mirror_landing` rebuilt it again to check it.
A failure there skipped the validation of Silver and Gold and paged
`warehouse_pg:derive_failed`. On 2026-11-11 the owner decided to stop both.

What stays: the table (no DROP, no migration — the owner's standing rule), its
last contents, its `meta.mirror_state` row and the journal's `profile_rows`
column, written NULL. What went: `core/pg_vitrina.py`, the derivation's
stage, the daily check and its condition `customer_profile_mismatch`, which
is never reused (a deleted condition name is never given to another check).

The derivation leaving the table alone is held against a real Postgres in
`tests/integration/test_pg_own_derivation.py` (`TestTheProfileIsLeftAlone`).
"""
from __future__ import annotations

import ast
import re

TABLE = "app.customer_profile"
WRITE = re.compile(r"\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE(\s+TABLE)?)\s+"
                   + re.escape(TABLE) + r"\b", re.I)


def _strings(tree: ast.AST):
    """Every string literal, and every literal part of an f-string."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.value


class TestNothingWritesTheTable:

    def test_the_walk_can_see_a_writer(self):
        """The regex is not looking at nothing: it finds the statement the
        deleted module ran, in the form that module wrote it."""
        tree = ast.parse('X = """\nINSERT INTO app.customer_profile\n  (buyer_id) SELECT 1\n"""\n'
                         'Y = f"TRUNCATE {T}"\nZ = "TRUNCATE app.customer_profile"\n')
        assert [s for s in _strings(tree) if WRITE.search(s)] == [
            "\nINSERT INTO app.customer_profile\n  (buyer_id) SELECT 1\n",
            "TRUNCATE app.customer_profile"]
