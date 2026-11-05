"""The UTM verdicts' four doors, and where a full parse may be forced.

Postgres parses `silver.order_utm` itself (`core/pg_utm_parse.py`): the
derivation's last step, and four doors outside it — `POST
/api/traffic/refresh`, `/traffic/reclassify`, the `manager_comment` backfill
behind `/traffic/backfill-utm`, and `scripts/backfill_utm.py`. Until step
14's S14-15 each door ran DuckDB's parse first and `KS_UTM_PARSE` chose
whether the router shipped DuckDB's copy (`core/pg_order_utm.py`, deleted)
or parsed in Postgres; `tests/unit/test_order_utm_shipping.py` walked that
choice. What is left to pin, and is pinned here:

- the columns the parse writes are the migration's;
- every door reaches the table through `reparse_router`, and nothing outside
  the parse's own module and the derivation's last step calls a parse
  directly;
- the two doors that re-parse everything ask for a full parse, the two that
  add do not;
- only the CLI forces, behind its flag;
- the traffic router never takes the store lock, and no traffic read goes
  round it (moved here from the ship's tests, where it sat since S14-05).
"""
from __future__ import annotations

import ast
import inspect
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_utm_parse

REPO = Path(__file__).resolve().parents[2]
PRODUCTION_ROOTS = ("core", "web", "scripts", "bot", "deploy")

# The four doors the design names, and whether each re-parses everything.
DOORS = {
    ("web/routes/api/traffic.py", "_parse_in_postgres"): None,   # the two routes' helper
    ("web/routes/api/traffic.py", "_run_backfill_postgres"): False,
    ("scripts/backfill_utm.py", "_reparse"): True,
}
ROUTES = {"refresh_traffic_data": False, "reclassify_traffic": True}
DIRECT = {"parse_full", "parse_incremental", "parse_incremental_locked"}
# Where a parse may be called by name: its own module, and the derivation's
# last step (`BackgroundScheduler._parse_order_utm_in_postgres`).
ALLOWED_DIRECT = {("core/pg_utm_parse.py", None),
                  ("core/scheduler.py", "_parse_order_utm_in_postgres")}


def _trees():
    for root in PRODUCTION_ROOTS:
        for path in sorted((REPO / root).rglob("*.py")):
            yield str(path.relative_to(REPO)), ast.parse(path.read_text())


def _called(call: ast.Call):
    func = call.func
    return func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)


def _owner(node, parents):
    found = None
    while node in parents:
        node = parents[node]
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            found = node
    return found


def _parents(tree):
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    return parents


def _migration_columns() -> set:
    """The columns revision 0018 declares for `silver.order_utm`, lifted out
    with `ast`, comments stripped, the list delimited by parentheses."""
    src = (REPO / "migrations" / "versions" / "0018_traffic.py").read_text()
    statements = [
        node.value for node in ast.walk(ast.parse(src))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        and "CREATE TABLE silver.order_utm" in node.value]
    assert len(statements) == 1, "the CREATE TABLE moved or was duplicated"
    sql = "\n".join(line.split("--", 1)[0] for line in statements[0].splitlines())
    body, depth, started = [], 0, False
    for ch in sql[sql.index("silver.order_utm"):]:
        if ch == "(":
            depth += 1
            started = True
            if depth == 1:
                continue
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break
        if started:
            body.append(ch)
    columns = set()
    for part in "".join(body).split(","):
        words = part.split()
        if len(words) >= 2 and words[0].isidentifier():
            columns.add(words[0])
    return columns


def test_the_written_columns_are_the_migrations():
    """A column the table has and the parser does not carry would be written
    NULL for ever. `UTM_COLUMNS` moved here from `core/pg_order_utm.py`."""
    assert _migration_columns() - {"mirrored_at"} == set(pg_utm_parse.UTM_COLUMNS)
    assert pg_utm_parse.UTM_TABLE == "silver.order_utm"


class TestEveryDoorGoesThroughTheRouter:
    def _router_calls(self):
        out = {}
        for rel, tree in _trees():
            parents = _parents(tree)
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and _called(node) == "reparse_router":
                    fn = _owner(node, parents)
                    full = next((k.value for k in node.keywords if k.arg == "full"), None)
                    out.setdefault((rel, fn.name if fn else None), []).append(full)
        return out

    def test_the_doors_are_the_routers_callers(self):
        calls = self._router_calls()
        assert set(calls) == set(DOORS), sorted(calls)
        for door, full in DOORS.items():
            for value in calls[door]:
                if full is None:
                    assert isinstance(value, ast.Name) and value.id == "full", door
                else:
                    said = isinstance(value, ast.Constant) and value.value is True
                    assert said is full, door

    def test_the_two_routes_say_which_parse(self):
        """The reclassify is the only way a rule change reaches an order whose
        `updated_at` has not moved, so it replaces the table whole; the
        refresh only adds, or every refresh would re-parse 33 K comments."""
        tree = ast.parse((REPO / "web/routes/api/traffic.py").read_text())
        for fn in ast.walk(tree):
            if isinstance(fn, ast.AsyncFunctionDef) and fn.name in ROUTES:
                (call,) = [c for c in ast.walk(fn) if isinstance(c, ast.Call)
                           and _called(c) == "_parse_in_postgres"]
                full = next(k.value for k in call.keywords if k.arg == "full")
                assert full.value is ROUTES[fn.name], fn.name

    def test_nothing_else_calls_a_parse_directly(self):
        found = set()
        for rel, tree in _trees():
            parents = _parents(tree)
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and _called(node) in DIRECT:
                    fn = _owner(node, parents)
                    key = (rel, None) if rel == "core/pg_utm_parse.py" else (rel, fn.name if fn else None)
                    found.add(key)
        assert found == ALLOWED_DIRECT, sorted(found)

    def test_the_router_takes_no_store(self):
        """It took DuckDB's store until S14-15, for the ship."""
        assert list(inspect.signature(pg_utm_parse.reparse_router).parameters) == [
            "full", "force"]


class TestTheRouter:
    @pytest.mark.asyncio
    async def test_it_stands_down_without_a_dsn(self):
        with patch("core.mirror_reconciliation.configured", return_value=False):
            assert "skipped" in await pg_utm_parse.reparse_router()

    @pytest.mark.asyncio
    async def test_it_never_raises(self):
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch.object(pg_utm_parse, "parse_incremental_locked",
                          new=AsyncMock(side_effect=RuntimeError("pg is down"))):
            out = await pg_utm_parse.reparse_router()
        assert out == {"error": "RuntimeError: pg is down"}

    @pytest.mark.asyncio
    async def test_full_forwards_force_and_adding_does_not_force(self):
        full = AsyncMock(return_value={"rows": 3})
        add = AsyncMock(return_value={"parsed": 1})
        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch.object(pg_utm_parse, "parse_full", new=full), \
             patch.object(pg_utm_parse, "parse_incremental_locked", new=add):
            assert await pg_utm_parse.reparse_router(full=True, force=True) == {"rows": 3}
            assert await pg_utm_parse.reparse_router() == {"parsed": 1}
        full.assert_awaited_once_with(force=True)
        add.assert_awaited_once_with()


class TestOnlyTheCliForces:
    """`force=True` defeats the full parse's shrink guard, so where it may
    come from is pinned by structure."""

    TAKERS = {"reparse_router", "parse_full"}

    def _calls_passing_force(self, path):
        tree = ast.parse(path.read_text())
        parents = _parents(tree)

        def _enclosing_params(node):
            names = set()
            while node in parents:
                node = parents[node]
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    a = node.args
                    names |= {x.arg for x in a.posonlyargs + a.args + a.kwonlyargs}
            return names

        found = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _called(node) in self.TAKERS:
                for kw in node.keywords:
                    if kw.arg == "force":
                        forwards = (isinstance(kw.value, ast.Name)
                                    and kw.value.id == "force"
                                    and "force" in _enclosing_params(node))
                        found.append((path, node.lineno, kw.value, forwards))
        return found

    def test_nothing_in_the_application_originates_a_force(self):
        offenders = []
        for root in ("core", "web", "bot"):
            for path in (REPO / root).rglob("*.py"):
                offenders += [(str(p.relative_to(REPO)), line)
                              for p, line, _v, forwards in self._calls_passing_force(path)
                              if not forwards]
        assert offenders == [], f"force passed outside the CLI: {offenders}"

    def test_the_backfill_script_forces_only_behind_its_flag(self):
        path = REPO / "scripts" / "backfill_utm.py"
        tree = ast.parse(path.read_text())
        passes = self._calls_passing_force(path)
        assert len(passes) == 1, "the script should reach Postgres exactly once"
        value = passes[0][2]
        assert isinstance(value, ast.Name) and value.id == "force_ship"
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.AsyncFunctionDef) and n.name == "backfill_utm")
        names = [a.arg for a in fn.args.args]
        defaults = dict(zip(names[len(names) - len(fn.args.defaults):], fn.args.defaults))
        assert isinstance(defaults["force_ship"], ast.Constant)
        assert defaults["force_ship"].value is False
        flags = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                 and n.func.attr == "add_argument" and n.args
                 and isinstance(n.args[0], ast.Constant) and n.args[0].value == "--force-ship"]
        assert len(flags) == 1
        action = next(k.value for k in flags[0].keywords if k.arg == "action")
        assert action.value == "store_true"

    @pytest.mark.asyncio
    async def test_the_cli_reparses_whole_in_postgres_and_forwards_its_flag(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "backfill_utm_doors", REPO / "scripts" / "backfill_utm.py")
        bf = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(bf)
        router = AsyncMock(return_value={"rows": 3})
        with patch("core.pg_utm_parse.reparse_router", new=router):
            await bf._reparse(force_ship=True)
        router.assert_awaited_once_with(full=True, force=True)


class TestTheTrafficRouter:
    """Since step 14's S14-05 the traffic router asks Postgres and refuses a
    failure. What stays pinned is that it never takes the store lock and that
    no traffic read goes round it."""

    def test_it_does_not_take_the_store_lock_itself(self):
        from core.repositories.traffic import TrafficMixin

        tree = ast.parse(textwrap.dedent(inspect.getsource(TrafficMixin._traffic_run)))
        for node in ast.walk(tree):
            if isinstance(node, ast.AsyncWith):
                head = ast.unparse(node.items[0].context_expr)
                assert "connection()" not in head, f"the router opens the store: {head}"

    def test_no_traffic_read_bypasses_the_router(self):
        src = (REPO / "core" / "repositories" / "traffic.py").read_text()
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.AsyncFunctionDef) and node.name.startswith("get_traffic"):
                body = ast.get_source_segment(src, node) or ""
                assert "conn.execute" not in body, node.name
