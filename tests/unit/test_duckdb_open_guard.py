"""Every read-write open of a DuckDB file goes through the one opener that
checkpoints first, `core.duckdb_store.open_read_write`.

That opener is what keeps a killed writer's index entries (see its docstring
and `test_duckdb_kill_guard.py`). A second read-write `duckdb.connect`
anywhere — a script, a restore path, a one-off repair — would replay the same
WAL and could close through DuckDB's own lossy checkpoint before anything
bound the index. So this walks the code that runs against the analytics file
(`core/ web/ bot/ scripts/ deploy/`), never a list of known callers, and
holds every `duckdb.connect` it reaches through `import duckdb [as x]` or
`from duckdb import connect [as y]` to one of:

- `read_only=True` (a literal), or `config={"access_mode": "READ_ONLY"}`;
- in memory (no target, `""`, `":memory:…"`);
- inside `open_read_write`;
- a named exemption, each of which must match exactly one call.

`duckdb.connect` passed around as a value fails, because nothing could then
be said about the call it ends up in. And every SQL string that ATTACHes a
database file must say READ_ONLY: an ATTACH opens a file read-write as surely
as a connect does.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
ROOTS = ("core", "web", "bot", "scripts", "deploy")

OPENER = ("core/duckdb_store.py", "open_read_write")

# (file, function) → why it may open read-write without the guard.
EXEMPT = {
    ("scripts/compact_duckdb.py", "phase3_validate"): (
        "the write test on the file phase 2 just built, checkpointed and "
        "closed — no WAL, so there is nothing a lossy checkpoint could drop; "
        "it CHECKPOINTs itself before closing, and the script is "
        "do-not-touch (CLAUDE.md)"
    ),
}

_ATTACH = re.compile(
    r"\bATTACH\s+(?:DATABASE\s+)?(?:IF\s+NOT\s+EXISTS\s+)?['\"{]", re.IGNORECASE,
)


def _classify(call: ast.Call) -> str:
    kw = {k.arg: k.value for k in call.keywords if k.arg}
    ro = kw.get("read_only")
    if isinstance(ro, ast.Constant) and ro.value is True:
        return "read_only"
    cfg = kw.get("config")
    if isinstance(cfg, ast.Dict):
        for k, v in zip(cfg.keys, cfg.values):
            if (isinstance(k, ast.Constant) and k.value == "access_mode"
                    and isinstance(v, ast.Constant)
                    and str(v.value).upper() == "READ_ONLY"):
                return "read_only"
    target = call.args[0] if call.args else kw.get("database")
    if target is None or (
        isinstance(target, ast.Constant)
        and (target.value == "" or str(target.value).startswith(":memory:"))
    ):
        return "memory"
    return "read_write"


def _sql_text(node: ast.AST) -> "str | None":
    """A string literal, or an f-string with its holes as `{}`."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            v.value if isinstance(v, ast.Constant) else "{}" for v in node.values
        )
    return None


def scan(source: str, rel: str):
    """(calls, violations) for one module.

    calls: (rel, function, line, kind) for every duckdb.connect reached.
    violations: (rel, function, line, why).
    """
    tree = ast.parse(source)
    modules, functions = set(), set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            modules |= {a.asname or a.name for a in n.names if a.name == "duckdb"}
        elif isinstance(n, ast.ImportFrom) and n.module == "duckdb":
            functions |= {a.asname or a.name for a in n.names if a.name == "connect"}

    parents = {}
    for n in ast.walk(tree):
        for c in ast.iter_child_nodes(n):
            parents[c] = n

    def function_of(n):
        while n in parents:
            n = parents[n]
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return n.name
        return "<module>"

    def is_connect(f):
        return ((isinstance(f, ast.Attribute) and f.attr == "connect"
                 and isinstance(f.value, ast.Name) and f.value.id in modules)
                or (isinstance(f, ast.Name) and f.id in functions))

    calls, violations, called = [], [], set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Call) and is_connect(n.func):
            called.add(n.func)
            kind = _classify(n)
            where = (rel, function_of(n))
            calls.append((rel, where[1], n.lineno, kind))
            if kind == "read_write" and where != OPENER and where not in EXEMPT:
                violations.append((rel, where[1], n.lineno,
                                   "read-write duckdb.connect outside open_read_write"))

    in_fstring = {v for n in ast.walk(tree) if isinstance(n, ast.JoinedStr)
                  for v in n.values}
    for n in ast.walk(tree):
        if is_connect(n) and n not in called and not isinstance(
                parents.get(n), (ast.Import, ast.ImportFrom)):
            violations.append((rel, function_of(n), n.lineno,
                               "duckdb.connect used as a value, not called"))
        if n in in_fstring:
            continue
        text = _sql_text(n)
        if text and _ATTACH.search(text) and "READ_ONLY" not in text.upper():
            violations.append((rel, function_of(n), n.lineno,
                               "ATTACH without READ_ONLY"))
    return calls, violations


def walk_repository():
    calls, violations = [], []
    for root in ROOTS:
        for path in sorted((REPO / root).rglob("*.py")):
            rel = path.relative_to(REPO).as_posix()
            c, v = scan(path.read_text(encoding="utf-8"), rel)
            calls += c
            violations += v
    return calls, violations


@pytest.fixture(scope="module")
def walked():
    return walk_repository()


def test_every_read_write_open_goes_through_the_guard(walked):
    _, violations = walked
    assert not violations, (
        "a DuckDB file opened read-write without the checkpoint-first guard "
        "(use core.duckdb_store.open_read_write, or read_only=True):\n"
        + "\n".join(f"  {r}:{line} in {fn}: {why}" for r, fn, line, why in violations)
    )


def test_the_walk_found_the_opener_and_each_exemption_once(walked):
    calls, _ = walked
    read_write = [(r, fn) for r, fn, _, kind in calls if kind == "read_write"]
    assert read_write.count(OPENER) == 1, read_write
    for exempt in EXEMPT:
        assert read_write.count(exempt) == 1, (
            f"exemption {exempt} matches {read_write.count(exempt)} read-write "
            "calls; it must name exactly one, or be removed"
        )
    assert sorted(set(read_write)) == sorted({OPENER, *EXEMPT})
    # Not blind: the read-only opens are seen too (backups, compaction, ark).
    assert any(kind == "read_only" for *_, kind in calls)


def test_the_opener_checkpoints_before_anything_else():
    tree = ast.parse((REPO / OPENER[0]).read_text(encoding="utf-8"))
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == OPENER[1])
    executes = sorted(
        (n for n in ast.walk(fn) if isinstance(n, ast.Call)
         and isinstance(n.func, ast.Attribute) and n.func.attr == "execute"),
        key=lambda n: (n.lineno, n.col_offset),
    )
    assert executes, "open_read_write executes nothing"
    first = executes[0].args[0]
    assert isinstance(first, ast.Constant) and first.value == "CHECKPOINT", (
        "the first statement on the replayed instance must be CHECKPOINT"
    )


def test_the_store_opens_through_the_guard():
    tree = ast.parse((REPO / "core/duckdb_store.py").read_text(encoding="utf-8"))
    store = next(n for n in ast.walk(tree)
                 if isinstance(n, ast.ClassDef) and n.name == "DuckDBStore")
    connect = next(n for n in store.body
                   if isinstance(n, ast.AsyncFunctionDef) and n.name == "connect")
    called = {n.func.id for n in ast.walk(connect)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    assert "open_read_write" in called


# ─── the walk itself, on sources written to fool it ─────────────────────────

@pytest.mark.parametrize("source, verdict", [
    ("import duckdb\nduckdb.connect(p)", "read-write"),
    ("import duckdb as d\ndef f():\n    d.connect(database=p)", "read-write"),
    ("from duckdb import connect as c\nc(p)", "read-write"),
    ("import duckdb\nduckdb.connect(p, read_only=flag)", "read-write"),
    ("import duckdb\nduckdb.connect(p, read_only=False)", "read-write"),
    ("import duckdb\nopen_it = duckdb.connect", "value"),
    ("import duckdb\nduckdb.connect(p, read_only=True)", None),
    ("import duckdb\nduckdb.connect(p, config={'access_mode': 'READ_ONLY'})", None),
    ("import duckdb\nduckdb.connect(':memory:')", None),
    ("import duckdb\nduckdb.connect()", None),
    ("c.execute(f\"ATTACH '{backup}' AS b\")", "ATTACH"),
    ("c.execute(\"ATTACH DATABASE 'x.duckdb' AS b\")", "ATTACH"),
    ("c.execute(f\"ATTACH '{backup}' AS b (READ_ONLY)\")", None),
    ("import sqlite3\nsqlite3.connect(p)", None),
])
def test_the_walk_sees_what_it_must(source, verdict):
    _, violations = scan(source, "core/x.py")
    whys = " | ".join(v[3] for v in violations)
    if verdict is None:
        assert not violations, whys
    else:
        assert len(violations) == 1 and verdict in whys, whys
