"""DN-20a: every read that falls back to DuckDB is counted, and found by a walk.

`core.read_fallback.fall_back` is the one place a fallback is counted and
logged. What makes it the *one* place is the walk below, which reads `core/`
and `web/` for the three shapes a fallback to DuckDB takes rather than
trusting a list of routers — a list only ever guards the routers somebody was
already thinking about, and `pg_expenses_read.backfilled()` is the proof: it
served DuckDB for weeks under log text no grep for "falling back to DuckDB"
could find, because it was neither a router nor a gate.

THE THREE SHAPES, AND WHAT EACH MUST DO

1. **A log call saying "falling back to DuckDB"** (docstrings are not calls,
   so they do not count). The handler it sits in must call `fall_back` or
   re-raise. After this change the only such call left is `fall_back`'s own.
2. **An `X.enabled() and X.available()` gate** — the shape every read switch
   uses to choose an engine. In the gate's function, every except handler that
   carries on toward DuckDB — it falls through past its `try`, or reads DuckDB
   itself — must call `fall_back` or re-raise. A handler that *returns* ends
   the function; in `sync_to_meilisearch` that is "skip the step", not DuckDB.
3. **Any except handler in a function that reaches another engine's read** —
   `get_pool`, a `*_run` router, or a function of a read-switch module — under
   `core/repositories/` or `core/pg_*_read*.py`. Here a handler that returns
   is judged too: `backfilled()` returns False and `_pg_gold_summary` returns
   None, and it is the *caller* that then reads DuckDB — invisible from the
   handler, so every handler in that scope has to decide.

"Re-raise" includes deciding for `ReadUnavailable` in the same `try`: that is
what DN-20b and DN-20c raise from `fall_back`, and a broad handler around a
router must not turn the refusal into a quiet answer.

A read-switch module is one whose `ENV` names a `KS_READ_*` variable, found by
walking `core/` — not listed either.

Two fallbacks are deliberately not counted per request, and each has another
answer: a gate whose switch is on and whose engine has no address
(`KS_READ_GOLD=postgres` without `KS_PG_DSN`) is a configuration, published at
startup under `read_fallback_mode.misconfigured`; and `backfilled()` answering
"no history yet" is a correct routing decision, not a failure.
"""
from __future__ import annotations

import ast
import asyncio
import logging
import pathlib
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Set
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core import read_fallback

REPO = pathlib.Path(__file__).resolve().parents[2]
WALKED = ("core", "web")
FALLBACK_TEXT = re.compile(r"falling back to DuckDB", re.IGNORECASE)
# What a handler that carries on toward DuckDB looks like when it reads it
# itself. Deliberately loose: a false positive costs a `fall_back` decision
# somebody has to write, a false negative costs a silent fallback.
DUCKDB_READS = {"connection", "get_store", "_fetch_one", "_fetch_all"}
EXITS = (ast.Return, ast.Raise, ast.Continue, ast.Break)


# ─── The walk ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Site:
    rule: str          # "log" | "gate" | "handler"
    path: str
    line: int
    function: str
    ok: bool


def _own_nodes(node: ast.AST) -> Iterable[ast.AST]:
    """Every node under `node`, not descending into nested functions,
    lambdas or classes — those are judged as functions of their own."""
    stack = list(ast.iter_child_nodes(node))
    while stack:
        child = stack.pop()
        yield child
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                              ast.Lambda, ast.ClassDef)):
            continue
        stack.extend(ast.iter_child_nodes(child))


def _call_name(call: ast.Call) -> Optional[str]:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _functions(tree: ast.AST) -> List[ast.AST]:
    return [n for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]


def _mentions_read_unavailable(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return False
    for n in ast.walk(handler.type):
        if isinstance(n, ast.Name) and n.id == "ReadUnavailable":
            return True
        if isinstance(n, ast.Attribute) and n.attr == "ReadUnavailable":
            return True
    return False


def _decides(handler: ast.ExceptHandler, try_node: ast.Try) -> bool:
    """The handler counts its fallback, re-raises, or the `try` it belongs to
    has already said what `ReadUnavailable` means."""
    for n in _own_nodes(handler):
        if isinstance(n, ast.Call) and _call_name(n) == "fall_back":
            return True
        if isinstance(n, ast.Raise):
            return True
    for earlier in try_node.handlers:
        if _mentions_read_unavailable(earlier):
            return True
        if earlier is handler:
            break
    return False


def _handlers(fn: ast.AST) -> List[tuple]:
    """(handler, its try) for every handler owned by `fn`."""
    out = []
    for n in _own_nodes(fn):
        if isinstance(n, (ast.Try, getattr(ast, "TryStar", ast.Try))):
            for h in n.handlers:
                out.append((h, n))
    return out


def _carries_on_to_duckdb(handler: ast.ExceptHandler) -> bool:
    if not handler.body or not isinstance(handler.body[-1], EXITS):
        return True
    return any(isinstance(n, ast.Call) and _call_name(n) in DUCKDB_READS
               for n in _own_nodes(handler))


def _is_gate(node: ast.AST) -> bool:
    if not (isinstance(node, ast.BoolOp) and isinstance(node.op, ast.And)):
        return False
    called = {_call_name(n) for v in node.values for n in ast.walk(v)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    return {"enabled", "available"} <= called


def _string_parts(node: ast.AST) -> List[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.JoinedStr):
        return [v.value for v in node.values
                if isinstance(v, ast.Constant) and isinstance(v.value, str)]
    return []


def read_switch_modules() -> Set[str]:
    """Modules in `core/` whose `ENV` names a `KS_READ_*` variable and which
    answer `enabled()` — a switch choosing an engine. `core/read_fallback.py`
    has the prefix and no `enabled`: it chooses how a failure degrades, not
    where a read goes."""
    found = set()
    for path in sorted((REPO / "core").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names_env = any(
            isinstance(stmt, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "ENV" for t in stmt.targets)
            and isinstance(stmt.value, ast.Constant)
            and str(stmt.value.value).startswith("KS_READ_")
            for stmt in tree.body)
        answers = any(isinstance(stmt, ast.FunctionDef) and stmt.name == "enabled"
                      for stmt in tree.body)
        if names_env and answers:
            found.add(path.stem)
    return found


def _switch_names(tree: ast.AST, switches: Set[str]) -> tuple:
    """(module aliases, function names) this file binds to a read switch."""
    modules, functions = set(), set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "core":
            for a in n.names:
                if a.name in switches:
                    modules.add(a.asname or a.name)
        elif (isinstance(n, ast.ImportFrom) and n.module
              and n.module.startswith("core.")
              and n.module.split(".", 1)[1] in switches):
            for a in n.names:
                functions.add(a.asname or a.name)
        elif isinstance(n, ast.Import):
            for a in n.names:
                if a.name.startswith("core.") and a.name.split(".", 1)[1] in switches:
                    modules.add(a.asname or a.name)
    return modules, functions


def _reaches_another_engine(fn: ast.AST, modules: Set[str], functions: Set[str]) -> bool:
    for n in ast.walk(fn):
        if not isinstance(n, ast.Call):
            continue
        name = _call_name(n)
        if name == "get_pool":
            return True
        if name and name.endswith("_run") and name != "_run":
            return True
        if (isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)
                and n.func.value.id in modules
                and name not in ("enabled", "available")):
            return True
        if isinstance(n.func, ast.Name) and n.func.id in functions:
            return True
    return False


def _in_handler_scope(relpath: str) -> bool:
    return (relpath.startswith("core/repositories/")
            or re.fullmatch(r"core/pg_[^/]*read[^/]*\.py", relpath) is not None)


def walk_source(source: str, relpath: str, switches: Set[str]) -> List[Site]:
    """The three shapes in one file. Pure, so it can be shown synthetic code."""
    tree = ast.parse(source)
    parent: Dict[ast.AST, ast.AST] = {}
    for p in ast.walk(tree):
        for c in ast.iter_child_nodes(p):
            parent[c] = p

    def enclosing(node, kinds):
        while node in parent:
            node = parent[node]
            if isinstance(node, kinds):
                return node
        return None

    def try_of(handler):
        return parent[handler]

    sites: List[Site] = []

    # 1. A log call saying "falling back to DuckDB".
    for call in (n for n in ast.walk(tree) if isinstance(n, ast.Call)):
        texts = [t for a in call.args for t in _string_parts(a)]
        if not any(FALLBACK_TEXT.search(t) for t in texts):
            continue
        fn = enclosing(call, (ast.FunctionDef, ast.AsyncFunctionDef))
        name = getattr(fn, "name", "<module>")
        if relpath == "core/read_fallback.py" and name == "fall_back":
            continue  # the one uniform line
        handler = enclosing(call, (ast.ExceptHandler,))
        if handler is not None and (fn is None or enclosing(handler, (
                ast.FunctionDef, ast.AsyncFunctionDef)) is fn):
            ok = _decides(handler, try_of(handler))
        else:
            ok = fn is not None and any(
                isinstance(n, ast.Call) and _call_name(n) == "fall_back"
                for n in _own_nodes(fn))
        sites.append(Site("log", relpath, call.lineno, name, ok))

    # 2. An enabled()-and-available() gate.
    for gate in (n for n in ast.walk(tree) if _is_gate(n)):
        fn = enclosing(gate, (ast.FunctionDef, ast.AsyncFunctionDef))
        if fn is None:
            sites.append(Site("gate", relpath, gate.lineno, "<module>", False))
            continue
        judged = [(h, t) for h, t in _handlers(fn) if _carries_on_to_duckdb(h)]
        ok = all(_decides(h, t) for h, t in judged)
        sites.append(Site("gate", relpath, gate.lineno, fn.name, ok))

    # 3. Every handler in a function that reaches another engine's read.
    if _in_handler_scope(relpath):
        modules, functions = _switch_names(tree, switches)
        for fn in _functions(tree):
            if not _reaches_another_engine(fn, modules, functions):
                continue
            for h, t in _handlers(fn):
                sites.append(Site("handler", relpath, h.lineno, fn.name,
                                  _decides(h, t)))
    return sites


def walk_tree() -> List[Site]:
    switches = read_switch_modules()
    sites: List[Site] = []
    for top in WALKED:
        for path in sorted((REPO / top).rglob("*.py")):
            if "frontend" in path.parts or "node_modules" in path.parts:
                continue
            relpath = path.relative_to(REPO).as_posix()
            sites.extend(walk_source(path.read_text(encoding="utf-8"),
                                     relpath, switches))
    return sites


@pytest.fixture(scope="module")
def sites() -> List[Site]:
    return walk_tree()


class TestTheWalk:
    def test_every_fallback_is_counted_or_re_raised(self, sites):
        bad = sorted(f"{s.rule} {s.path}:{s.line} in {s.function}"
                     for s in sites if not s.ok)
        assert not bad, (
            "a read that can fall back to DuckDB without core.read_fallback."
            "fall_back counting it, and without re-raising: " + "; ".join(bad))

    def test_it_finds_backfilled(self, sites):
        """The site critique 7 found by hand: neither a gate nor a log line
        with the phrase — the walk must reach it by the third rule alone."""
        assert any(s.rule == "handler" and s.path == "core/pg_expenses_read.py"
                   and s.function == "backfilled" for s in sites)

    def test_it_is_looking(self, sites):
        """Floors, not a list: the walk found the gates and the handlers it
        exists for. Twenty gates and seventeen handlers when it was written."""
        gates = {(s.path, s.function) for s in sites if s.rule == "gate"}
        handlers = [s for s in sites if s.rule == "handler"]
        assert len(gates) >= 15, sorted(gates)
        assert len(handlers) >= 15, handlers
        assert {"core/repositories", "core/chat_tools.py",
                "core/sync_service.py"} <= {
            p if not p.startswith("core/repositories/") else "core/repositories"
            for p, _ in gates}

    def test_the_read_switches_are_found_not_listed(self):
        switches = read_switch_modules()
        assert {"pg_expenses_read", "pg_gold_read", "ch_cohorts"} <= switches
        assert "read_fallback" not in switches
        assert "pg_sms_read" not in switches, (
            "the SMS tab fails closed on its own switch; it has no ENV here")

    def test_every_counted_surface_is_a_literal(self, sites):
        """A surface is a key in /api/health, so it must be readable from the
        code — never computed from a flag or an exception."""
        surfaces = set()
        for top in WALKED:
            for path in (REPO / top).rglob("*.py"):
                for n in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                    if (isinstance(n, ast.Call) and _call_name(n) == "fall_back"
                            and n.args):
                        arg = n.args[0]
                        if isinstance(arg, ast.Constant):
                            surfaces.add(arg.value)
                        else:
                            surfaces.add(ast.unparse(arg))
        # `_pg_silver` passes its keyword on, and `backfilled` counts under
        # the tab it gates.
        assert "surface" in surfaces
        literal = {s for s in surfaces if s != "surface"}
        assert all(re.fullmatch(r"[a-z_]+", s) for s in literal), literal
        assert {"dashboard", "expenses", "goals", "cohorts"} <= literal


# ─── The walk shown synthetic code, so each rule is seen to bite ───────────

class TestTheRules:
    SWITCHES = {"pg_x_read"}

    def _ok(self, source, relpath="core/repositories/x.py"):
        return [(s.rule, s.ok) for s in walk_source(source, relpath, self.SWITCHES)]

    def test_a_log_line_alone_is_not_counting(self):
        src = (
            "async def f(self):\n"
            "    try:\n"
            "        return await pg_x_read.fetch()\n"
            "    except Exception as exc:\n"
            "        logger.error('x: Postgres failed, falling back to DuckDB: %s', exc)\n"
            "    async with self.connection() as conn:\n"
            "        return conn.execute('x').fetchall()\n"
        )
        assert ("log", False) in self._ok(src, "core/somewhere.py")

    def test_a_gate_whose_handler_falls_through_must_count(self):
        src = (
            "async def f(self):\n"
            "    if pg_x_read.enabled() and pg_x_read.available():\n"
            "        try:\n"
            "            return await pg_x_read.fetch()\n"
            "        except Exception:\n"
            "            pass\n"
            "    async with self.connection() as conn:\n"
            "        return conn.execute('x').fetchall()\n"
        )
        assert self._ok(src, "core/somewhere.py") == [("gate", False)]
        counted = src.replace("            pass\n",
                              "            read_fallback.fall_back('x', None)\n")
        assert self._ok(counted, "core/somewhere.py") == [("gate", True)]

    def test_a_gate_whose_handler_ends_the_step_is_not_a_fallback(self):
        """`sync_to_meilisearch`'s shape: the handler returns, nothing after it
        reads DuckDB, and outside the handler scope that is the end of it."""
        src = (
            "async def f(self):\n"
            "    try:\n"
            "        use_pg = pg_x_read.enabled() and pg_x_read.available()\n"
            "        return await (pg_x_read.fetch() if use_pg else g())\n"
            "    except Exception as e:\n"
            "        logger.error('sync error: %s', e)\n"
            "        return {}\n"
        )
        assert self._ok(src, "core/sync_service.py") == [("gate", True)]

    def test_a_handler_returning_a_sentinel_is_judged_in_scope(self):
        """`backfilled()`'s shape: False from the handler, DuckDB in the caller."""
        src = (
            "async def backfilled():\n"
            "    try:\n"
            "        pool = await get_pool()\n"
            "    except Exception as exc:\n"
            "        logger.warning('cannot read the watermark: %s', exc)\n"
            "        return False\n"
            "    return True\n"
        )
        assert self._ok(src, "core/pg_x_read.py") == [("handler", False)]
        assert self._ok(src, "core/elsewhere.py") == [], (
            "outside core/repositories and core/pg_*_read* the third rule "
            "does not apply")

    def test_letting_read_unavailable_through_is_a_decision(self):
        src = (
            "async def f(self):\n"
            "    try:\n"
            "        return await self._goals_run('x')\n"
            "    except read_fallback.ReadUnavailable:\n"
            "        raise\n"
            "    except Exception:\n"
            "        return 0.0\n"
        )
        assert set(self._ok(src)) == {("handler", True)}
        without = src.replace("    except read_fallback.ReadUnavailable:\n"
                              "        raise\n", "")
        assert self._ok(without) == [("handler", False)]

    def test_a_switch_function_imported_by_name_is_reaching(self):
        src = (
            "from core.pg_x_read import fetch\n"
            "async def f():\n"
            "    try:\n"
            "        return await fetch()\n"
            "    except Exception:\n"
            "        return None\n"
        )
        assert self._ok(src) == [("handler", False)]


# ─── The mode, the counter, the startup check ──────────────────────────────

@pytest.fixture
def fresh_read_fallback(monkeypatch):
    """The module's caches, restored after the test; counts start from zero.
    `configure_modes` sets the derivation mode too, so that is kept as well."""
    from core import pg_derivation

    before = (read_fallback._mode, read_fallback._mode_error,
              list(read_fallback._misconfigured))
    derivation = (pg_derivation._mode, pg_derivation._mode_error)
    for name in [n for n in __import__("os").environ if n.startswith("KS_READ_")]:
        monkeypatch.delenv(name, raising=False)
    read_fallback._mode, read_fallback._mode_error = None, None
    read_fallback._misconfigured = []
    read_fallback.reset_counts()
    yield read_fallback
    pg_derivation._mode, pg_derivation._mode_error = derivation
    read_fallback._mode, read_fallback._mode_error = before[0], before[1]
    read_fallback._misconfigured = before[2]
    read_fallback.reset_counts()


class TestTheMode:
    def test_unset_is_duckdb(self, fresh_read_fallback):
        assert fresh_read_fallback.configure_mode() == "duckdb"
        assert fresh_read_fallback.mode_error() is None

    def test_off_is_read_and_still_falls_back(self, fresh_read_fallback, monkeypatch, caplog):
        """Refusing is DN-20b and DN-20c. Until then `off` is validated and
        published, and a fallback is still served — and says so at start."""
        monkeypatch.setenv("KS_READ_FALLBACK", "off")
        with caplog.at_level(logging.WARNING, logger="core.read_fallback"):
            assert fresh_read_fallback.configure_mode() == "off"
        assert "not enforced" in caplog.text
        fresh_read_fallback.fall_back("dashboard", RuntimeError("down"))
        assert fresh_read_fallback.counts()["dashboard"]["count"] == 1

    def test_an_unknown_value_runs_as_duckdb_and_never_raises(
        self, fresh_read_fallback, monkeypatch, caplog,
    ):
        monkeypatch.setenv("KS_READ_FALLBACK", "of")
        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            assert fresh_read_fallback.configure_mode() == "duckdb"
        assert "KS_READ_FALLBACK='of'" in fresh_read_fallback.mode_error()
        assert fresh_read_fallback.mode() == "duckdb"
        assert caplog.text.count("KS_READ_FALLBACK='of'") == 1
        # web's startup and the scheduler both configure: one ERROR per cause.
        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            fresh_read_fallback.configure_mode()
        assert caplog.text.count("KS_READ_FALLBACK='of'") == 1

    def test_configure_modes_reads_it(self, fresh_read_fallback, monkeypatch):
        """A cached mode is read in `configure_modes`, which web's startup
        calls before the boot sync — never at scheduler start (DN-05b)."""
        from core.runtime_modes import configure_modes

        monkeypatch.setenv("KS_READ_FALLBACK", "nope")
        modes = configure_modes()
        assert modes["KS_READ_FALLBACK"] == "duckdb"
        assert fresh_read_fallback.mode_error()


class TestMisconfiguredReads:
    def test_a_switch_naming_postgres_without_a_dsn_is_published(
        self, fresh_read_fallback, monkeypatch, caplog,
    ):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.setenv("KS_READ_GOLD", "postgres")
        monkeypatch.setenv("KS_READ_TRAFFIC", "duckdb")
        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            fresh_read_fallback.configure_mode()
        assert fresh_read_fallback.misconfigured() == [
            "KS_READ_GOLD=postgres without KS_PG_DSN"]
        assert "KS_READ_GOLD=postgres without KS_PG_DSN" in caplog.text

    def test_clickhouse_needs_its_url(self, fresh_read_fallback, monkeypatch):
        monkeypatch.delenv("KS_CH_URL", raising=False)
        monkeypatch.setenv("KS_READ_COHORTS", "clickhouse")
        fresh_read_fallback.configure_mode()
        assert fresh_read_fallback.misconfigured() == [
            "KS_READ_COHORTS=clickhouse without KS_CH_URL"]

    def test_with_the_address_nothing_is_published(self, fresh_read_fallback, monkeypatch):
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        monkeypatch.setenv("KS_CH_URL", "http://ch:8123")
        monkeypatch.setenv("KS_READ_GOLD", "postgres")
        monkeypatch.setenv("KS_READ_COHORTS", "clickhouse")
        fresh_read_fallback.configure_mode()
        assert fresh_read_fallback.misconfigured() == []

    def test_every_read_switch_in_the_code_is_covered(self, fresh_read_fallback, monkeypatch):
        """The check scans the environment by prefix, so a switch added later
        is covered the day it is set. Shown against every switch the walk
        finds, so the prefix and the modules cannot drift apart."""
        import importlib

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.delenv("KS_CH_URL", raising=False)
        envs = {}
        for name in read_switch_modules():
            module = importlib.import_module(f"core.{name}")
            engine = "clickhouse" if name.startswith("ch_") else "postgres"
            envs[module.ENV] = engine
            monkeypatch.setenv(module.ENV, engine)
        fresh_read_fallback.configure_mode()
        published = {line.split("=", 1)[0] for line in fresh_read_fallback.misconfigured()}
        assert published == set(envs)


class TestTheCounter:
    def test_it_counts_per_surface_and_logs_the_phrase(self, fresh_read_fallback, caplog):
        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            fresh_read_fallback.fall_back("goals", RuntimeError("pg down"))
            fresh_read_fallback.fall_back("goals", RuntimeError("pg down"))
            fresh_read_fallback.fall_back("traffic", None)
        counts = fresh_read_fallback.counts()
        assert counts["goals"]["count"] == 2 and counts["traffic"]["count"] == 1
        assert counts["goals"]["last_at"].endswith("+00:00")
        # The soak grep reads this phrase; the surface and the cause beside it.
        assert caplog.text.count("falling back to DuckDB") == 3
        assert "goals" in caplog.text and "pg down" in caplog.text

    def test_counts_is_a_copy(self, fresh_read_fallback):
        fresh_read_fallback.fall_back("x", None)
        snapshot = fresh_read_fallback.counts()
        snapshot["x"]["count"] = 99
        assert fresh_read_fallback.counts()["x"]["count"] == 1


# ─── backfilled(): the site nobody's grep found ────────────────────────────

class TestBackfilledCounts:
    @pytest.fixture(autouse=True)
    def _fresh(self, fresh_read_fallback):
        from core import pg_expenses_read as r

        before = (r._backfilled, r._checked_at, r._check_failed)
        r._backfilled, r._checked_at, r._check_failed = False, 0.0, None
        yield r
        r._backfilled, r._checked_at, r._check_failed = before

    @pytest.mark.asyncio
    async def test_an_unreadable_watermark_is_a_counted_fallback(self, _fresh):
        with patch("core.pg.get_pool", new=AsyncMock(side_effect=OSError("refused"))):
            assert await _fresh.backfilled() is False
        assert read_fallback.counts()["expenses"]["count"] == 1

    @pytest.mark.asyncio
    async def test_the_recheck_window_after_a_failure_counts_every_answer(self, _fresh):
        """Inside the minute after a failed check every call is served by
        DuckDB because of that failure — each one is counted."""
        with patch("core.pg.get_pool", new=AsyncMock(side_effect=OSError("refused"))):
            await _fresh.backfilled()
            await _fresh.backfilled()
            await _fresh.backfilled()
        assert read_fallback.counts()["expenses"]["count"] == 3

    @pytest.mark.asyncio
    async def test_no_history_yet_is_routing_not_a_fallback(self, _fresh):
        conn = MagicMock()
        conn.fetchval = AsyncMock(return_value=False)
        acquire = MagicMock()
        acquire.__aenter__ = AsyncMock(return_value=conn)
        acquire.__aexit__ = AsyncMock(return_value=False)
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=acquire)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            assert await _fresh.backfilled() is False
            assert await _fresh.backfilled() is False
        assert "expenses" not in read_fallback.counts()


# ─── The one handler around a router: ReadUnavailable passes through ───────

class TestTheForecastSignalLetsARefusalThrough:
    @pytest.mark.asyncio
    async def test_read_unavailable_is_not_a_missing_signal(self):
        from core.repositories.goals import GoalsMixin

        store = GoalsMixin.__new__(GoalsMixin)
        with patch.object(GoalsMixin, "_goals_run",
                          new=AsyncMock(side_effect=read_fallback.ReadUnavailable("goals"))), \
             patch.object(GoalsMixin, "_forecast_actual_revenue",
                          new=AsyncMock(return_value=0.0)):
            with pytest.raises(read_fallback.ReadUnavailable):
                await store._get_ml_forecast_total(2025, 3, "retail")

    @pytest.mark.asyncio
    async def test_any_other_failure_is_still_left_out(self):
        from core.repositories.goals import GoalsMixin

        store = GoalsMixin.__new__(GoalsMixin)
        with patch.object(GoalsMixin, "_goals_run",
                          new=AsyncMock(side_effect=RuntimeError("duckdb gone"))), \
             patch.object(GoalsMixin, "_forecast_actual_revenue",
                          new=AsyncMock(return_value=0.0)):
            assert await store._get_ml_forecast_total(2025, 3, "retail") == 0.0


# ─── The canary ────────────────────────────────────────────────────────────

class TestTheCanary:
    def test_an_error_is_a_failure_key(self):
        from bot.canary import check_read_fallback_mode

        payload = {"read_fallback_mode": {
            "mode": "duckdb", "error": "KS_READ_FALLBACK='of' is not one of ('duckdb', 'off')",
            "misconfigured": []}}
        assert [k for k, _ in check_read_fallback_mode(payload)] == [
            "read_fallback_mode_invalid"]

    def test_absent_or_clean_is_quiet(self):
        from bot.canary import check_read_fallback_mode

        assert check_read_fallback_mode({}) == []
        assert check_read_fallback_mode({"read_fallback_mode": {
            "mode": "off", "error": None, "misconfigured": []}}) == []

    @pytest.mark.asyncio
    async def test_the_wiring_warns(self):
        """Not only the judge: a typo reaches a failure key and a warn, and
        names its lever."""
        from datetime import datetime, timezone

        import httpx

        from bot import canary
        from tests.unit.test_canary import DASHBOARD, _healthy_payload, _mock_transport

        payload = _healthy_payload()
        payload["read_fallback_mode"] = {
            "mode": "duckdb", "error": "KS_READ_FALLBACK='of' is not one of ('duckdb', 'off')",
            "misconfigured": []}

        def handler(request):
            return httpx.Response(200, json=payload)

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with _mock_transport(handler) as client:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary(DASHBOARD, client=client)
        assert result.severity == "warn"
        assert result.failure_keys == ["read_fallback_mode_invalid"]
        assert "KS_READ_FALLBACK" in canary._what_to_do(result)


# ─── Through the app ───────────────────────────────────────────────────────

@pytest.fixture
def admin_client(monkeypatch):
    import time

    from fastapi.testclient import TestClient

    from core.permissions import ADMIN_USER_IDS
    from web.main import app
    from web.ratelimit import limiter
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    admin = sorted(ADMIN_USER_IDS)[0]

    async def _resolve(session):
        return {"user_id": admin, "role": "admin"}

    monkeypatch.setattr("web.routes.auth._resolve_session", _resolve)
    limiter.reset()
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, session_serializer.dumps(create_session_data(
        {"id": str(admin), "first_name": "T", "last_name": "U", "username": "t",
         "auth_date": str(int(time.time()))}, role="admin")))
    yield client
    limiter.reset()


def _seed_gold(day: date, revenue: float, orders: int) -> None:
    """One Gold cell in the per-test DuckDB file the app is about to open."""
    from core.duckdb_store import DuckDBStore

    async def seed():
        store = DuckDBStore()
        await store.connect()
        try:
            async with store.connection() as conn:
                conn.execute(
                    "INSERT INTO gold_daily_revenue (date, sales_type, revenue, "
                    "orders_count) VALUES (?, 'retail', ?, ?)",
                    [day, revenue, orders])
        finally:
            await store.close()

    asyncio.run(seed())


class TestThroughTheApp:
    def test_a_raising_pool_serves_duckdb_and_counts_one_dashboard_fallback(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        day = date.today() - timedelta(days=3)
        _seed_gold(day, 4321.5, 7)
        monkeypatch.setenv("KS_READ_GOLD", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(side_effect=OSError("connection refused")))

        body = admin_client.get(
            "/api/summary", params={"start_date": day.isoformat(),
                                    "end_date": day.isoformat()}).json()
        assert body["totalRevenue"] == 4321.5 and body["totalOrders"] == 7

        health = admin_client.get("/api/health").json()
        assert health["read_fallbacks"]["dashboard"]["count"] == 1
        assert health["read_fallbacks"]["dashboard"]["last_at"]
        assert "connection refused" not in str(health["read_fallbacks"]), (
            "/api/health is public; the error text stays in the log")

    def test_nothing_fell_back_is_an_empty_block(self, admin_client, fresh_read_fallback):
        health = admin_client.get("/api/health").json()
        assert health["read_fallbacks"] == {}
        assert health["read_fallback_mode"] == {
            "mode": "duckdb", "error": None, "misconfigured": []}

    def test_an_unknown_value_starts_web_and_is_published(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        """The startup itself, with what it would do to the world stubbed:
        a typo in KS_READ_FALLBACK must not be able to stop order intake.
        Then /api/health, which is where the canary reads it."""
        import web.main as main

        monkeypatch.setenv("KS_READ_FALLBACK", "offf")
        store = MagicMock()
        store.get_stats = AsyncMock(return_value={
            "orders": 1, "products": 1, "categories": 1, "db_size_mb": 1})
        store.backfill_sms_campaign_record = AsyncMock(return_value=False)
        prediction = MagicMock(is_ready=True)
        monkeypatch.setattr(main, "validate_config", MagicMock())
        monkeypatch.setattr(main, "init_database", MagicMock())
        monkeypatch.setattr(main, "init_and_sync", AsyncMock())
        monkeypatch.setattr(main, "get_store", AsyncMock(return_value=store))
        monkeypatch.setattr(main, "start_scheduler", AsyncMock())
        monkeypatch.setattr(main, "_register_event_handlers", MagicMock())
        monkeypatch.setattr("core.prediction_service.get_prediction_service",
                            MagicMock(return_value=prediction))

        asyncio.run(main.startup_event())
        main.init_and_sync.assert_awaited_once()  # it got past the modes

        assert fresh_read_fallback.mode() == "duckdb"
        published = admin_client.get("/api/health").json()["read_fallback_mode"]
        assert published["mode"] == "duckdb"
        assert "KS_READ_FALLBACK='offf'" in published["error"]
