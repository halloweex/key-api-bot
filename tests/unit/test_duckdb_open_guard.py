"""Every read-write open of a DuckDB file goes through the one opener that
checkpoints first, `core.duckdb_switch.open_file`.

It is the application's only opener for a second reason too: it refuses every
open under `KS_DUCKDB=off` (the week of silence), and
`tests/unit/test_duckdb_switch.py` holds `core/`, `web/` and `bot/` to it by
its own walk. One function carries both guards — the refusal first, then the
checkpoint — so neither walk can be satisfied by an opener the other does not
see.

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
- inside `open_file`;
- a named exemption, each of which must match exactly one call.

`duckdb.connect` passed around as a value fails, because nothing could then
be said about the call it ends up in — and so does the `duckdb` module
itself (`d = duckdb`, `getattr(duckdb, "connect")`). And every SQL string
that ATTACHes a database file must carry the READ_ONLY *option*: an ATTACH
opens a file read-write as surely as a connect does. The option is parsed,
not searched for — `AS read_only_copy`, `(READ_ONLY false)` and a comment
saying READ_ONLY all attach read-write (measured on 1.5.5) — and an ATTACH
built with `+` is read like an f-string, its holes as `{}`, one built with `%`
or `.format()` through its literal.

The code is not the only thing that opens the file: an agent or a person
follows the commands written down for them. So every document in the
repository — Markdown, shell, YAML, SQL, Dockerfiles — is read for
`duckdb.connect(...)`, an ATTACH, and the `duckdb` CLI on a `.duckdb` file,
under the same rules (`.claude/agents/analytics.md` told the analytics
agent to `duckdb.connect('data/analytics.duckdb')`, which on a killed
writer's file shortens the indexes).

What it cannot see: a module imported by name at run time
(`importlib.import_module("duckdb")`), and SQL assembled from pieces held
in variables.
"""
from __future__ import annotations

import ast
import os
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
ROOTS = ("core", "web", "bot", "scripts", "deploy")

OPENER = ("core/duckdb_switch.py", "open_file")

# (file, function) → why it may open read-write without the guard.
EXEMPT = {
    ("scripts/compact_duckdb.py", "phase3_validate"): (
        "the write test on the file phase 2 just built, checkpointed and "
        "closed — no WAL, so there is nothing a lossy checkpoint could drop; "
        "it CHECKPOINTs itself before closing, and the script is "
        "do-not-touch (CLAUDE.md)"
    ),
}

# An ATTACH statement: its target (a literal, or a hole where one goes), its
# alias and its option list. A hole is `{}`/`{name}` (an f-string, `.format`,
# a `+` concatenation) or a `%` placeholder.
_ATTACH = re.compile(
    r"\bATTACH\s+(?:OR\s+REPLACE\s+)?(?:DATABASE\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<target>'[^']*'|\"[^\"]*\"|\{[^}]*\}|%(?:\([^)]*\))?[sr])"
    r"(?:\s+AS\s+(?P<alias>\"[^\"]*\"|\{[^}]*\}|\w+))?"
    r"(?:\s*(?P<options>\([^)]*\)))?",
    re.IGNORECASE,
)
_SQL_COMMENT = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)
_TRUE = {"", "TRUE", "1", "'TRUE'"}


def attach_violations(text: str) -> list:
    """One entry per ATTACH in `text` that does not carry READ_ONLY as an
    option set to true. Comments are stripped first: a comment is not an
    option."""
    bad = []
    for found in _ATTACH.finditer(_SQL_COMMENT.sub(" ", text)):
        options = (found.group("options") or "()")[1:-1]
        values = [" ".join(o.split()[1:]).upper() for o in options.split(",")
                  if o.split() and o.split()[0].upper() == "READ_ONLY"]
        if not values or any(v not in _TRUE for v in values):
            bad.append(found.group(0))
    return bad


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
    """The text a string expression evaluates to, with every hole as `{}`: a
    literal, an f-string, or a `+` concatenation with at least one literal in
    it. None for anything else — a `%`-format or a `.format()` is read through
    its own literal, whose `%s` or `{}` stands where the target goes."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            v.value if isinstance(v, ast.Constant) else "{}" for v in node.values
        )
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _sql_text(node.left), _sql_text(node.right)
        if left is None and right is None:
            return None
        return (left if left is not None else "{}") + (right if right is not None else "{}")
    return None


def _driver_imports(tree: ast.AST):
    """(names bound to the `duckdb` module, names bound to its `connect`)."""
    modules, functions = set(), set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            modules |= {a.asname or a.name for a in n.names if a.name == "duckdb"}
        elif isinstance(n, ast.ImportFrom) and n.module == "duckdb":
            functions |= {a.asname or a.name for a in n.names if a.name == "connect"}
    return modules, functions


def program_violations(text: str) -> list:
    """`why` for every read-write open in a program held as text.

    Read the way a module is read when the text is a Python program that
    imports the driver — `import duckdb as d; d.connect(p)` included, which
    the literal `duckdb.connect(` the first form searched for never matched
    (batch-E review) — and by that literal call otherwise: a fragment that
    does not parse, or a snippet run where `duckdb` is already bound. An
    ATTACH is not read here; the caller reads the whole text for it."""
    if "duckdb" in text:
        try:
            tree = ast.parse(text)
        except (SyntaxError, ValueError):
            tree = None
        if tree is not None and any(_driver_imports(tree)):
            _calls, inner = scan(text, "<text>")
            return [why for *_where, why in inner
                    if not why.startswith("ATTACH without READ_ONLY")]
    return [why for _pos, why in connect_violations(text)]


def scan(source: str, rel: str):
    """(calls, violations) for one module.

    calls: (rel, function, line, kind) for every duckdb.connect reached.
    violations: (rel, function, line, why).
    """
    tree = ast.parse(source)
    modules, functions = _driver_imports(tree)

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
                                   "read-write duckdb.connect outside open_file"))

    # ast.walk is breadth-first, so an expression is read before its parts,
    # and the parts of one that yielded text are not read again on their own.
    inside_text = set()
    for n in ast.walk(tree):
        if is_connect(n) and n not in called and not isinstance(
                parents.get(n), (ast.Import, ast.ImportFrom)):
            violations.append((rel, function_of(n), n.lineno,
                               "duckdb.connect used as a value, not called"))
        if (isinstance(n, ast.Name) and n.id in modules
                and not (isinstance(parents.get(n), ast.Attribute)
                         and parents[n].value is n)):
            violations.append((rel, function_of(n), n.lineno,
                               "the duckdb module used as a value"))
        if n in inside_text:
            continue
        text = _sql_text(n)
        if text is None:
            continue
        inside_text.update(d for d in ast.walk(n) if d is not n)
        for attach in attach_violations(text):
            violations.append((rel, function_of(n), n.lineno,
                               f"ATTACH without READ_ONLY: {attach}"))
        # A program held as text and run elsewhere — `python -c`, a
        # subprocess — opens the file as surely as a call here does, and the
        # AST above never sees inside a string. The step-13 rehearsal's D1
        # DELETE was one: a bare read-write connect, in a string, run once per
        # table, whose lossy close cost the next table's index. Read as a
        # module is, so an alias does not pass (`program_violations`).
        for why in program_violations(text):
            violations.append((rel, function_of(n), n.lineno,
                               f"{why} in a program held as text"))
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


# ─── the documents: what an agent or a person is told to run ────────────────

DOC_SUFFIXES = {".md", ".sh", ".txt", ".yml", ".yaml", ".sql", ".toml", ".ini",
                ".cfg", ".conf", ".j2", ".service", ".path", ".timer"}
# Not documents of this repository: other worktrees, dependencies, data,
# build output, IDE state, and the untracked planning notes.
SKIP_DIRS = {".git", "node_modules", ".venv", "venv", "data", "__pycache__",
             ".pytest_cache", "worktrees", "static-v2", "storybook-static",
             "dist", ".idea", ".planning"}

_CONNECT_IN_TEXT = re.compile(r"\bduckdb\s*\.\s*connect\s*\(")
# The CLI on a database file; `-readonly` anywhere among its flags is fine.
_CLI_IN_TEXT = re.compile(
    r"(?:^|[\s;|&(`$])duckdb(?P<flags>(?:\s+-[-\w]+)*)\s+['\"]?[^\s'\"]*\.duckdb\b",
    re.MULTILINE,
)


def _documents():
    for root, dirs, files in os.walk(REPO):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)   # pruned, not walked
        for name in sorted(files):
            path = Path(root) / name
            if path.suffix in DOC_SUFFIXES or name.startswith("Dockerfile"):
                yield path.relative_to(REPO).as_posix(), path


def _call_text(text: str, start: int, open_paren: int) -> str:
    """`duckdb.connect(...)` from `start`, parentheses balanced, with the
    quotes a shell or a JSON string escaped put back."""
    depth, i = 1, open_paren
    while i < len(text) and depth:
        depth += {"(": 1, ")": -1}.get(text[i], 0)
        i += 1
    return text[start:i].replace('\\"', '"').replace("\\'", "'")


def connect_violations(text: str) -> list:
    """(offset, why) for every `duckdb.connect(...)` written out in `text`
    that is not read-only or in-memory — in a document, or in a program a
    module holds as a string."""
    violations = []
    for found in _CONNECT_IN_TEXT.finditer(text):
        snippet = _call_text(text, found.start(), found.end())
        try:
            call = ast.parse(snippet, mode="eval").body
        except SyntaxError:
            violations.append((found.start(),
                               f"duckdb.connect the walk cannot read: {snippet[:80]}"))
            continue
        kind = _classify(call) if isinstance(call, ast.Call) else "read_write"
        if kind == "read_write":
            violations.append((found.start(), f"read-write duckdb.connect: {snippet[:80]}"))
    return violations


def scan_document(text: str, rel: str) -> list:
    """(rel, line, why) for every read-write open a document tells somebody
    to run."""
    violations = []

    def line_of(pos):
        return text.count("\n", 0, pos) + 1

    for pos, why in connect_violations(text):
        violations.append((rel, line_of(pos), why))
    for attach in attach_violations(text):
        violations.append((rel, line_of(text.find(attach)),
                           f"ATTACH without READ_ONLY: {attach}"))
    for found in _CLI_IN_TEXT.finditer(text):
        if "-readonly" not in found.group("flags").split():
            violations.append((rel, line_of(found.start()),
                               f"the duckdb CLI without -readonly: {found.group(0).strip()}"))
    return violations


def walk_documents():
    violations, read = [], []
    for rel, path in _documents():
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        read.append(rel)
        violations += scan_document(text, rel)
    return read, violations


@pytest.fixture(scope="module")
def walked():
    return walk_repository()


def test_no_document_tells_anyone_to_open_it_read_write():
    read, violations = walk_documents()
    # Not blind: the agents' instructions and the host scripts are read.
    assert ".claude/agents/analytics.md" in read and "scripts/weekly_compact.sh" in read
    assert not violations, (
        "a document tells an agent or a person to open a DuckDB file "
        "read-write without the checkpoint-first guard (read_only=True, "
        "-readonly, or core.duckdb_switch.open_file):\n"
        + "\n".join(f"  {r}:{line}: {why}" for r, line, why in violations)
    )


def test_every_read_write_open_goes_through_the_guard(walked):
    _, violations = walked
    assert not violations, (
        "a DuckDB file opened read-write without the checkpoint-first guard "
        "(use core.duckdb_switch.open_file, or read_only=True):\n"
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
    assert executes, "open_file executes nothing"
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
    called = {ast.unparse(n.func) for n in ast.walk(connect) if isinstance(n, ast.Call)}
    assert "duckdb_switch.open_file" in called, called


def test_the_switch_refuses_before_the_checkpoint_opens_anything():
    """The two guards are one function and the refusal comes first: under
    `KS_DUCKDB=off` the driver is never reached, so no WAL is replayed and no
    file is created. Mutation: move `guard()` after `duckdb.connect`."""
    tree = ast.parse((REPO / OPENER[0]).read_text(encoding="utf-8"))
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == OPENER[1])
    calls = sorted((n for n in ast.walk(fn) if isinstance(n, ast.Call)),
                   key=lambda n: (n.lineno, n.col_offset))
    names = [ast.unparse(n.func) for n in calls]
    assert names.index("guard") < names.index("duckdb.connect"), names


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
    # Each of these attaches read-write, and a substring test passed them all.
    ("c.execute(\"ATTACH 'x.duckdb' AS read_only_copy\")", "ATTACH"),
    ("c.execute(f\"ATTACH '{p}' AS b (READ_ONLY false)\")", "ATTACH"),
    ("c.execute(f\"ATTACH '{p}' AS b (READ_ONLY 0)\")", "ATTACH"),
    ("c.execute(f\"ATTACH '{p}' AS b  -- not READ_ONLY\")", "ATTACH"),
    ("c.execute(f\"ATTACH '{p}' AS b /* (READ_ONLY) */\")", "ATTACH"),
    ("c.execute(f\"ATTACH '{p}' AS b (TYPE duckdb -- , READ_ONLY\\n)\")", "ATTACH"),
    ("c.execute(\"ATTACH %s AS b\" % repr(p))", "ATTACH"),
    ("c.execute(\"ATTACH \" + repr(str(p)) + \" AS b\")", "ATTACH"),
    ("c.execute(\"ATTACH '{}' AS b\".format(p))", "ATTACH"),
    ("c.execute(f\"ATTACH '{p}' AS b (READ_ONLY); ATTACH '{q}' AS c\")", "ATTACH"),
    # ...and these attach read-only.
    ("c.execute(f\"ATTACH '{p}' AS b (READ_ONLY true)\")", None),
    ("c.execute(f\"ATTACH '{p}' AS b (TYPE duckdb, READ_ONLY)\")", None),
    ("c.execute(\"ATTACH \" + repr(str(p)) + \" AS b (READ_ONLY)\")", None),
    ("c.execute(\"ATTACH %s AS b (READ_ONLY)\" % repr(p))", None),
    # The module itself, handed on.
    ("import duckdb\nddb = duckdb\nddb.connect(p)", "module used as a value"),
    ("import duckdb\ngetattr(duckdb, 'connect')(p)", "module used as a value"),
    # A program held as text, run by `python -c` or a subprocess: the AST
    # does not see inside a string, and the rehearsal's D1 DELETE hid there.
    ("PROG = '''\nimport duckdb\ncon = duckdb.connect(db)\n'''", "program held as text"),
    ("run([exe, '-c', f'import duckdb; duckdb.connect({p!r}).execute(q)'])",
     "program held as text"),
    ("PROG = '''\nimport duckdb\ncon = duckdb.connect(db, read_only=True)\n'''", None),
    ("PROG = '''\nfrom core.duckdb_switch import open_file\ncon = open_file(db)\n'''",
     None),
    # The batch-E review's: the driver aliased inside the program, which the
    # literal `duckdb.connect(` never matched — in core/, and as D1's DELETE
    # with the bare connect 2df7ed80 removed put back under an alias.
    ("_PROG = \"import duckdb as d, sys; "
     "print(d.connect(sys.argv[1]).execute('SELECT count(*) FROM orders').fetchone())\"",
     "program held as text"),
    ("_DELETE_ONE = r'''\nimport json, sys\nimport duckdb as ddb\n"
     "db, table = sys.argv[1:3]\ncon = ddb.connect(db)\n"
     "con.execute(f\"DELETE FROM {table}\")\n'''", "program held as text"),
    ("PROG = 'from duckdb import connect as c\\nc(db)'", "program held as text"),
    ("PROG = 'import duckdb as d\\nd.connect(db, read_only=True)'", None),
    # Prose naming the driver is not a program.
    ("doc = 'the one read-write `duckdb.connect` of the analytics file'", None),
])
def test_the_walk_sees_what_it_must(source, verdict):
    _, violations = scan(source, "core/x.py")
    whys = " | ".join(v[3] for v in violations)
    if verdict is None:
        assert not violations, whys
    else:
        assert len(violations) == 1 and verdict in whys, whys


@pytest.mark.parametrize("text, verdict", [
    # The command .claude/agents/analytics.md gave the analytics agent.
    ('python -c "import duckdb; conn = duckdb.connect(\'data/analytics.duckdb\'); '
     'print(conn.execute(\'SELECT 1\').fetchone())"', "read-write duckdb.connect"),
    ('python -c "import duckdb; duckdb.connect(\\"data/analytics.duckdb\\")"',
     "read-write duckdb.connect"),
    ("        self._connection = duckdb.connect(self._db_path)", "read-write duckdb.connect"),
    ("conn = duckdb.connect(str(Path('data') / 'analytics.duckdb'))", "read-write duckdb.connect"),
    ("duckdb data/analytics.duckdb -c 'SELECT 1'", "duckdb CLI"),
    ("docker exec web duckdb /app/data/analytics.duckdb", "duckdb CLI"),
    ("ATTACH 'data/analytics.duckdb' AS a;", "ATTACH"),
    ("conn = duckdb.connect('data/analytics.duckdb', read_only=True)", None),
    ('python -c "import duckdb; duckdb.connect(\'data/analytics.duckdb\', read_only=True)"', None),
    ("duckdb -readonly data/analytics.duckdb -c 'SELECT 1'", None),
    ("ATTACH 'data/analytics.duckdb' AS a (READ_ONLY);", None),
    ("conn = duckdb.connect(':memory:')", None),
    ("The one read-write `duckdb.connect` of the analytics file.", None),
    ("a backup named analytics-20260823.duckdb (556 MB)", None),
])
def test_the_document_walk_sees_what_it_must(text, verdict):
    whys = " | ".join(v[2] for v in scan_document(text, "docs/x.md"))
    if verdict is None:
        assert not whys, whys
    else:
        assert verdict in whys and whys.count(" | ") == 0, whys
