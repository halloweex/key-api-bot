"""No module outside the suite uses a name it never defined.

Found 2026-09-18, all at once, by one run of pyflakes — each a NameError on a
path no test had executed:

* the bot's per-tab gate called `surface_feature` without importing it, so
  from 2026-09-08 every approved person who is not an admin got an exception
  instead of a summary, a TOP-10, an Excel file or a search result;
* `_run_dq_reconciliation` used `DiscrepancyClass` and `get_sync_service`
  unimported since the repair was added (#43), so any discrepancy ended the
  job before its alert, its repair and its resolve;
* the Postgres and ClickHouse reconciliation arms read a `run_id` they never
  assigned, so their first CRITICAL would have raised before paging;
* two repositories logged a fallback through a `logger` they never defined,
  so the fallback raised at the one moment it existed for.

Tests catch a missing name only on a line they run, and these were the lines
nobody runs: an except branch, a non-admin, a day with a discrepancy. A static
check does not care which lines run. **It walks the tree rather than a list**,
because a guard that names its subjects only guards the ones somebody was
already thinking about.

Names inside annotations are not reported: under `from __future__ import
annotations`, and in string annotations, they are never evaluated at runtime.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pyflakes.checker
import pyflakes.messages

ROOT = Path(__file__).resolve().parents[2]

# Everything that is not application code: the suite itself (a missing name
# there fails the test that holds it), environments, build output, data.
SKIP_DIRS = {
    "tests", ".git", ".venv", "venv", "node_modules", "data", "__pycache__",
    ".planning", ".claude", ".idea", "static-v2", "storybook-static", "frontend",
}

REPORTED = (pyflakes.messages.UndefinedName, pyflakes.messages.UndefinedLocal)


def _sources():
    for path in sorted(ROOT.rglob("*.py")):
        rel = path.relative_to(ROOT)
        if SKIP_DIRS.intersection(rel.parts[:-1]):
            continue
        yield rel, path.read_text(encoding="utf-8")


def _annotation_positions(tree):
    """(line, col) of every node inside an annotation."""
    spots = set()

    def mark(annotation):
        if annotation is None:
            return
        for sub in ast.walk(annotation):
            if hasattr(sub, "lineno"):
                spots.add((sub.lineno, sub.col_offset))

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            a = node.args
            for arg in (*a.posonlyargs, *a.args, *a.kwonlyargs, a.vararg, a.kwarg):
                if arg is not None:
                    mark(arg.annotation)
            mark(node.returns)
        elif isinstance(node, ast.AnnAssign):
            mark(node.annotation)
    return spots


def _undefined(rel, source):
    tree = ast.parse(source, filename=str(rel))
    spots = _annotation_positions(tree)
    found = []
    for message in pyflakes.checker.Checker(tree, filename=str(rel)).messages:
        if not isinstance(message, REPORTED):
            continue
        if (message.lineno, message.col) in spots:
            continue
        found.append(f"{rel}:{message.lineno}: {message.message % message.message_args}")
    return found


def test_the_walk_reaches_the_code_that_broke():
    """A walk that silently skipped the application would pass forever."""
    walked = {str(rel) for rel, _ in _sources()}
    for expected in ("core/scheduler.py", "bot/handlers_legacy.py",
                     "core/repositories/customers.py", "web/main.py",
                     "deploy/restore_from_export.py"):
        assert expected in walked, expected
    assert not any(name.startswith("tests/") for name in walked)


def test_annotations_are_not_reported():
    """The filter is what keeps this quiet on ~35 harmless annotations; it
    must still let a real one through."""
    source = (
        "from __future__ import annotations\n"
        "def f(x: Missing) -> 'AlsoMissing':\n"
        "    return undefined_at_runtime\n"
    )
    assert _undefined(Path("probe.py"), source) == [
        "probe.py:3: undefined name 'undefined_at_runtime'"
    ]


def test_no_module_uses_a_name_it_never_defined():
    problems = [p for rel, source in _sources() for p in _undefined(rel, source)]
    assert problems == [], "\n".join(problems)
