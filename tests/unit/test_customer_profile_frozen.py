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
import importlib.util
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RETIRED = "customer_profile_mismatch"
TABLE = "app.customer_profile"
WRITE = re.compile(r"\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM|TRUNCATE(\s+TABLE)?)\s+"
                   + re.escape(TABLE) + r"\b", re.I)


def _strings(tree: ast.AST):
    """Every string literal, and every literal part of an f-string."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.value


def _python_files():
    for tree in ("core", "web", "bot", "scripts", "deploy"):
        yield from sorted((ROOT / tree).rglob("*.py"))


class TestNothingWritesTheTable:
    def test_the_module_that_rebuilt_it_is_gone(self):
        assert importlib.util.find_spec("core.pg_vitrina") is None

    def test_no_statement_in_the_code_writes_it(self):
        """Mutation: restore `core/pg_vitrina.py` (its TRUNCATE and INSERT),
        or any other statement that writes the table."""
        writers = sorted(
            str(path.relative_to(ROOT)) for path in _python_files()
            if any(WRITE.search(s) for s in _strings(ast.parse(path.read_text(encoding="utf-8")))))
        assert writers == []

    def test_the_walk_can_see_a_writer(self):
        """The regex is not looking at nothing: it finds the statement the
        deleted module ran, in the form that module wrote it."""
        tree = ast.parse('X = """\nINSERT INTO app.customer_profile\n  (buyer_id) SELECT 1\n"""\n'
                         'Y = f"TRUNCATE {T}"\nZ = "TRUNCATE app.customer_profile"\n')
        assert [s for s in _strings(tree) if WRITE.search(s)] == [
            "\nINSERT INTO app.customer_profile\n  (buyer_id) SELECT 1\n",
            "TRUNCATE app.customer_profile"]

    def test_no_migration_drops_it(self):
        """The table stays: the owner's standing rule is no DROP of any table."""
        for path in sorted((ROOT / "migrations" / "versions").glob("*.py")):
            text = path.read_text(encoding="utf-8")
            tree = ast.parse(text)
            # Revision 0011's downgrade drops the table it created; no upgrade may.
            upgrades = [ast.get_source_segment(text, n) or "" for n in tree.body
                        if isinstance(n, ast.FunctionDef) and n.name == "upgrade"]
            for body in upgrades:
                assert not re.search(r"DROP\s+TABLE[^;\"]*customer_profile", body, re.I), \
                    path.name


class TestTheConditionIsRetired:
    def test_it_is_not_registered_remediated_or_named(self):
        """Mutation: put back its REGISTRY entry, its `customer_profile_`
        REMEDIATION prefix, or a human name for it."""
        from core.alerting import REGISTRY
        from core.data_quality import HUMAN_CHECK_NAMES, REMEDIATION

        assert RETIRED not in REGISTRY
        assert not [prefix for prefix, _ in REMEDIATION if RETIRED.startswith(prefix)]
        assert RETIRED not in HUMAN_CHECK_NAMES

    def test_nothing_files_it(self):
        """No `check_name=` anywhere in the code hands it out again."""
        found = []
        for path in _python_files():
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if isinstance(node, ast.Call):
                    for kw in node.keywords:
                        if (kw.arg == "check_name" and isinstance(kw.value, ast.Constant)
                                and kw.value.value == RETIRED):
                            found.append(str(path.relative_to(ROOT)))
        assert found == []

    def test_the_soak_reads_it_nowhere(self):
        """D8 and D9 listed the name and D8 the table; a name nothing can file
        reads "nothing found" for ever. Comments may still tell its story."""
        for path in sorted((ROOT / "deploy" / "daily_soak").glob("*.sql")):
            code = "\n".join(line.split("--", 1)[0]
                             for line in path.read_text(encoding="utf-8").splitlines())
            assert RETIRED not in code, path.name
            assert TABLE not in code, path.name
