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

Names inside a plain function's signature, or a variable annotation outside a
class body, are not reported: under `from __future__ import annotations`, and
in string annotations, nothing evaluates them. Everywhere an annotation *is*
evaluated they are reported like any other name — a class body (pydantic
models, dataclasses, TypedDicts), a decorated function (FastAPI resolves an
endpoint's annotations on the first request, not at import, so a missing name
there imports cleanly and answers 500), and anything under `web/`, where a
plain function can still be a `Depends` target.
"""
from __future__ import annotations

import ast
import os
from pathlib import Path

import pyflakes.checker
import pyflakes.messages

ROOT = Path(__file__).resolve().parents[2]

# Everything that is not application code: the suite itself (a missing name
# there fails the test that holds it), build output, data. Hidden directories
# and virtualenvs are pruned by rule rather than by name — a checkout can hold
# a `.venv-spike` or an `env311`, and third-party code is not ours to lint.
SKIP_DIRS = {
    "tests", "node_modules", "data", "__pycache__", "site-packages",
    "static-v2", "storybook-static", "frontend",
}

REPORTED = (
    pyflakes.messages.UndefinedName,
    pyflakes.messages.UndefinedLocal,
    pyflakes.messages.UndefinedExport,
    # A star import turns every undefined name in its module into "may be
    # defined from star imports" — the exact bug above, reported as something
    # else. None in shipped code; this keeps it that way.
    pyflakes.messages.ImportStarUsed,
    pyflakes.messages.ImportStarUsage,
)


def _sources(root=ROOT):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(
            d for d in dirnames
            if d not in SKIP_DIRS
            and not d.startswith(".")
            and not os.path.exists(os.path.join(dirpath, d, "pyvenv.cfg"))
        )
        for name in sorted(filenames):
            if name.endswith(".py"):
                path = Path(dirpath) / name
                yield path.relative_to(root), path.read_text(encoding="utf-8")


def _annotation_positions(tree):
    """(line, col) of every node inside an annotation nothing evaluates.

    That is a plain function's signature and a variable annotation outside a
    class body. A class body is evaluated by whatever builds the class, and a
    decorator may evaluate the signature it wraps."""
    spots = set()

    def mark(annotation):
        if annotation is None:
            return
        for sub in ast.walk(annotation):
            if hasattr(sub, "lineno"):
                spots.add((sub.lineno, sub.col_offset))

    in_class_body = {
        id(stmt)
        for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        for stmt in node.body
    }
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.decorator_list:
                continue
            a = node.args
            for arg in (*a.posonlyargs, *a.args, *a.kwonlyargs, a.vararg, a.kwarg):
                if arg is not None:
                    mark(arg.annotation)
            mark(node.returns)
        elif isinstance(node, ast.AnnAssign) and id(node) not in in_class_body:
            mark(node.annotation)
    return spots


def _undefined(rel, source):
    tree = ast.parse(source, filename=str(rel))
    # FastAPI evaluates the annotations of anything reachable through
    # `Depends`, decorated or not, so nothing under web/ is exempt.
    spots = set() if Path(rel).parts[0] == "web" else _annotation_positions(tree)
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
                     "core/repositories/customers.py", "web/main.py"):
        assert expected in walked, expected
    # Not in the runtime image, and not mounted by quick_gate: required only
    # where it exists, which is CI and the full gate.
    if (ROOT / "deploy").is_dir():
        assert "deploy/restore_from_export.py" in walked
    assert not any(name.startswith("tests/") for name in walked)


def test_the_walk_prunes_environments_by_rule(tmp_path):
    """A virtualenv under any name is third-party code; the owner's checkout
    holds a `.venv-spike` with 442 modules pyflakes would report."""
    for rel in ("core/a.py", ".venv-spike/lib/b.py", "env311/lib/c.py",
                "tests/d.py", "vendor/site-packages/e.py"):
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text("x = 1\n")
    (tmp_path / "env311" / "pyvenv.cfg").write_text("home = /usr\n")

    assert [str(rel) for rel, _ in _sources(tmp_path)] == ["core/a.py"]


def test_only_unevaluated_annotations_are_exempt():
    """The filter keeps this quiet on ~32 harmless signatures in core/. It
    must still report a runtime name, and every annotation something does
    evaluate."""
    source = (
        "from __future__ import annotations\n"
        "route = None\n"
        "def f(x: Missing) -> 'AlsoMissing':\n"
        "    return undefined_at_runtime\n"
        "class Body:\n"
        "    amount: Decimal\n"
        "@route\n"
        "def create(body: Payload): ...\n"
    )
    assert _undefined(Path("core/probe.py"), source) == [
        "core/probe.py:4: undefined name 'undefined_at_runtime'",
        "core/probe.py:6: undefined name 'Decimal'",
        "core/probe.py:8: undefined name 'Payload'",
    ]
    # Under web/ even a plain signature counts: it may be a Depends target.
    assert "web/probe.py:3: undefined name 'Missing'" in _undefined(
        Path("web/probe.py"), source)


def test_a_star_import_is_reported():
    """It would have hidden the bot's bug under a different message."""
    source = "from os import *\ndef f():\n    return surface_feature()\n"
    assert _undefined(Path("core/probe.py"), source)


def test_no_module_uses_a_name_it_never_defined():
    problems = [p for rel, source in _sources() for p in _undefined(rel, source)]
    assert problems == [], "\n".join(problems)
