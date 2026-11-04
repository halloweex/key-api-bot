"""Step 14's S14-05: six read routers ask Postgres and nothing else.

`_traffic_run`, `_margin_run`, `_intel_run`, `_reports_run`, `_marketing_run`
and `_lookups_run` each used to choose an engine by a `KS_READ_*` switch,
ask `read_fallback.no_address` about it, try Postgres, and on a failure call
`read_fallback.fall_back` — which under `KS_READ_FALLBACK=off`, production's
mode since 2026-10-09 and since S14-00 the only one, refused the read: a 503
naming the surface, counted under `read_fallback_mode.refused`. The DuckDB
tail below that call was reachable by nothing in production.

So each keeps exactly the path production took: the Postgres read, and on any
exception `raise read_fallback.refuse(surface, exc)`. What is held here,
without a database:

- the router does not read the switch — unset, `duckdb`, a typo or junk,
  the read goes to Postgres, and nothing reaches for the DuckDB store (step
  13's readiness on `/api/warehouse/status` still reads the six variables,
  as `reader:*` keys, until S14-15 — `tests/unit/test_warehouse_cutover.py`);
- a failure is the same refusal `fall_back` made under `off`: the surface,
  the count, the log line, the cause, and no fallback counted;
- without `KS_PG_DSN` the pool's own refusal to guess a database is that
  failure, so the answer is the same 503 — `no_address` named the switch in
  the log, the cause names the DSN now;
- junk in any of the six variables starts web and imports every module
  (rule 3 of the plan).

The numbers are the `tests/integration/*_two_engines.py` files', each against
a real Postgres and literals DuckDB answered before its side went; every
route through the real app, `tests/unit/test_read_fallback_http.py`'s; that
nothing in these routers can call an engine outside its `try`,
`tests/unit/test_read_fallback_sites.py`'s rule 5.
"""
from __future__ import annotations

import ast
import importlib
import logging
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from core import read_fallback

ROOT = Path(__file__).resolve().parents[2]

# (router, surface, module, the switch it no longer reads, a statement with
# the holes it renders, a table the Postgres rendering must name)
ROUTERS = [
    ("_traffic_run", "traffic", "pg_traffic_read", "KS_READ_TRAFFIC",
     "SELECT 1 FROM {silver_orders} s LEFT JOIN {order_utm} u ON s.id = u.order_id",
     "silver.order_utm"),
    ("_margin_run", "margin", "pg_margin_read", "KS_READ_MARGIN",
     "SELECT 1 FROM {order_lines} l LEFT JOIN {offer_stocks} os ON l.sku = os.sku",
     "bronze.offer_stocks"),
    ("_intel_run", "products_intel", "pg_products_intel_read",
     "KS_READ_PRODUCTS_INTEL", "SELECT 1 FROM {order_lines} l, {products} p",
     "silver.order_lines"),
    ("_reports_run", "reports", "pg_reports_read", "KS_READ_REPORTS",
     "SELECT 1 FROM {silver_orders} s, {order_products} op", "bronze.order_products"),
    # The goal read used to take a branch of its own here (chain 7a); it is
    # the same path now, so it is the statement asked.
    ("_marketing_run", "marketing", "pg_marketing_read", "KS_READ_MARKETING",
     "SELECT goal_amount FROM {revenue_goals} WHERE period_type = 'monthly'",
     "app.revenue_goals"),
    ("_lookups_run", "lookups", "pg_lookups_read", "KS_READ_LOOKUPS",
     "SELECT id, name FROM {categories} ORDER BY name", "bronze.categories"),
]
IDS = [r[0] for r in ROUTERS]

# The three tabs whose every reader lives in one module beside its router.
# (`revenue.py` holds the other three routers among the dashboard's gated
# reads, which are S14-06's.)
TAB_MODULES = [
    ("core/repositories/traffic.py", "_traffic_run"),
    ("core/repositories/margin.py", "_margin_run"),
    ("core/repositories/products_intel.py", "_intel_run"),
]
OLD_SHAPE = {"fall_back", "no_address", "no_engine", "enabled", "available"}
ENGINE_MODULES = ("core.pg", "asyncpg")


def _dotted(node) -> str:
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _what_reaches_round_the_router(source: str):
    """`(routers, reaches)` for one module: the `_*_run` functions — rule 5
    of `test_read_fallback_sites.py` judges those — and, for every other
    function, what it reaches that only a router may: an engine (a function
    of a `core.pg*` module, by the module or imported by name; `get_pool`;
    an import of `core.pg*` or `asyncpg` of its own), the old shape's
    machinery (`fall_back`, `no_address`, `no_engine`, `enabled`,
    `available`), or the environment (`os.getenv`, `os.environ`, a
    `KS_READ_*` name outside a docstring)."""
    tree = ast.parse(source)
    modules, functions = set(), set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module:
            if n.module == "core":
                modules |= {a.asname or a.name for a in n.names
                            if a.name.startswith("pg")}
            elif n.module.startswith(ENGINE_MODULES):
                functions |= {a.asname or a.name for a in n.names}
        elif isinstance(n, ast.Import):
            modules |= {a.asname for a in n.names
                        if a.asname and a.name.startswith(ENGINE_MODULES)}
    routers, reaches = set(), {}
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if fn.name.startswith("_") and fn.name.endswith("_run"):
            routers.add(fn.name)
            continue
        docstrings = {id(f.body[0].value) for f in ast.walk(fn)
                      if isinstance(f, (ast.FunctionDef, ast.AsyncFunctionDef))
                      and f.body and isinstance(f.body[0], ast.Expr)}
        found = []
        for n in ast.walk(fn):
            if isinstance(n, (ast.Import, ast.ImportFrom)):
                names = ([n.module or ""] if isinstance(n, ast.ImportFrom)
                         else [a.name for a in n.names])
                if isinstance(n, ast.ImportFrom) and n.module == "core":
                    names = [f"core.{a.name}" for a in n.names]
                found += [f"engine: imports {m}" for m in names
                          if m.startswith(ENGINE_MODULES)]
            elif isinstance(n, ast.Call):
                name = _dotted(n.func)
                head, last = name.split(".", 1)[0], name.rsplit(".", 1)[-1]
                if (head in modules or name in functions
                        or name.startswith(ENGINE_MODULES) or last == "get_pool"):
                    found.append(f"engine: {name}()")
                elif last in OLD_SHAPE:
                    found.append(f"old shape: {name}()")
                elif name in ("os.getenv", "getenv", "os.environ.get", "environ.get"):
                    found.append(f"environment: {name}()")
            elif isinstance(n, ast.Subscript) and _dotted(n.value) in ("os.environ", "environ"):
                found.append("environment: os.environ[...]")
            elif (isinstance(n, ast.Constant) and isinstance(n.value, str)
                  and "KS_READ_" in n.value and id(n) not in docstrings):
                found.append(f"environment: {n.value[:40]!r}")
        if found:
            reaches[fn.name] = found
    return routers, reaches
SWITCHES = [r[3] for r in ROUTERS]
SWITCH_VALUES = [None, "", "duckdb", "postgres", "postgress", "junk-s14-05"]


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A store that is never connected, whose connection is a trap: nothing
    here may reach for DuckDB, under any value of any switch."""
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / "never.duckdb")

    def trap(*_a, **_k):
        raise AssertionError("a Postgres-only router reached for DuckDB")

    for name in ("connection", "_fetch_one", "_fetch_all"):
        monkeypatch.setattr(type(store), name, trap)
    read_fallback.reset_counts()
    yield store
    read_fallback.reset_counts()


def _set(monkeypatch, switch, value):
    if value is None:
        monkeypatch.delenv(switch, raising=False)
    else:
        monkeypatch.setenv(switch, value)


class TestTheSwitchIsNotRead:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", SWITCH_VALUES,
                             ids=[repr(v) for v in SWITCH_VALUES])
    @pytest.mark.parametrize("router,surface,module,switch,sql,table", ROUTERS, ids=IDS)
    async def test_postgres_answers_whatever_it_says(
        self, store, monkeypatch, router, surface, module, switch, sql, table, value,
    ):
        _set(monkeypatch, switch, value)
        rows = [(1, "x")]
        fetch = AsyncMock(return_value=rows)
        monkeypatch.setattr(f"core.{module}.fetch", fetch)

        answer = await getattr(store, router)(sql, [7])

        assert answer == rows
        rendered, params = fetch.await_args.args
        assert table in rendered and "{" not in rendered
        assert list(params) == [7]
        assert read_fallback.refusals() == {} and read_fallback.counts() == {}

    @pytest.mark.parametrize("module", [r[2] for r in ROUTERS])
    def test_the_module_carries_no_switch(self, module):
        """Mutation: put `ENV`/`enabled`/`available` back — the sites walk
        would then take the module for a switch, and a router could start
        asking it again."""
        m = importlib.import_module(f"core.{module}")
        assert not any(hasattr(m, name) for name in ("ENV", "enabled", "available"))
        assert callable(m.fetch)

    def test_the_walk_finds_none_of_them_a_switch(self):
        from tests.unit.test_read_fallback_sites import read_switch_modules

        assert not read_switch_modules() & {r[2] for r in ROUTERS}


class TestAFailureIsTheRefusalOffMade:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("router,surface,module,switch,sql,table", ROUTERS, ids=IDS)
    async def test_a_503_naming_the_surface_counted_as_a_refusal(
        self, store, monkeypatch, caplog, router, surface, module, switch, sql, table,
    ):
        down = OSError("connection refused")
        monkeypatch.setattr(f"core.{module}.fetch", AsyncMock(side_effect=down))

        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            with pytest.raises(read_fallback.ReadUnavailable) as raised:
                await getattr(store, router)(sql)

        assert raised.value.surface == surface
        assert raised.value.__cause__ is down
        assert read_fallback.refusals()[surface]["count"] == 1
        assert read_fallback.counts() == {}
        # `fall_back`'s line under off, word for word: `read_refused`'s lever
        # greps "read refused", the soak a fallback by the phrase below.
        assert (f"read refused: {surface} — its engine failed, and "
                f"KS_READ_FALLBACK=off forbids answering from DuckDB "
                f"(1 refused since start): connection refused") in caplog.text
        assert "falling back to DuckDB" not in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("router,surface,module,switch,sql,table", ROUTERS, ids=IDS)
    async def test_no_dsn_is_the_same_refusal(
        self, store, monkeypatch, router, surface, module, switch, sql, table,
    ):
        """No pool patched and no DSN: `core.pg.dsn()` refuses to guess a
        database, and that is the failure refused. It was `no_address`'s
        before, naming the switch; the HTTP answer is the same 503."""
        from core import pg

        monkeypatch.delenv("KS_PG_DSN", raising=False)
        monkeypatch.setattr(pg, "_pool", None)
        monkeypatch.setenv(switch, "postgres")

        with pytest.raises(read_fallback.ReadUnavailable) as raised:
            await getattr(store, router)(sql)

        assert raised.value.surface == surface
        assert isinstance(raised.value.__cause__, RuntimeError)
        assert "KS_PG_DSN is not set" in str(raised.value.__cause__)
        assert read_fallback.refusals()[surface]["count"] == 1

    def test_refuse_is_fall_back_under_off(self, caplog):
        """The two produce one refusal: the same exception, cause, count and
        line. Mutation: let `refuse` drift from `_refuse`'s body."""
        cause = ValueError("boom")
        with caplog.at_level(logging.ERROR, logger="core.read_fallback"):
            try:
                read_fallback.fall_back("x", cause)
            except read_fallback.ReadUnavailable as exc:
                old = exc
            new = read_fallback.refuse("x", cause)
        assert type(new) is type(old) and new.surface == old.surface == "x"
        assert new.__cause__ is old.__cause__ is cause
        assert new.__suppress_context__ and old.__suppress_context__
        first, second = (r.getMessage() for r in caplog.records)
        assert first.replace("(1 refused", "(2 refused") == second
        assert read_fallback.refusals()["x"]["count"] == 2


class TestJunkStartsWeb:
    """Rule 3 of the plan: a variable the code stopped reading cannot stop
    web, whatever it holds — neither its import nor its start. The start
    that reads these variables is `configure_modes(web=True)`, which
    `web/main.py`'s startup calls first: `read_fallback.configure_mode` scans
    every `KS_READ_*` for a switch with no address. So both tests run it
    with junk in all six; a test that only imported, or asked `/api/health`
    of an app whose start never ran, would pass a start that raised."""

    def test_a_fresh_interpreter_imports_and_starts_with_junk_in_every_one(self, tmp_path):
        """The child's environment is exactly what is written here. It gets
        the suite's test signing secrets (`tests/conftest.py`), because
        `web/routes/auth.py` refuses to import without one. It runs from
        `tmp_path`, because under `-c` `load_dotenv()` walks up from the
        cwd: from a worktree that walk reaches the main checkout's real
        `.env`, so the child would have imported with a developer's secrets
        and passed only where such a file was found. The child asserts it
        found none."""
        code = (
            "import importlib, sys\n"
            f"sys.path.insert(0, {str(ROOT)!r})\n"
            "from dotenv import find_dotenv\n"
            "found = find_dotenv(usecwd=True)\n"
            "assert not found, 'a .env above the cwd would be loaded: ' + found\n"
            "import web.main\n"
            "for m in %r:\n"
            "    importlib.import_module('core.' + m)\n"
            "from core.runtime_modes import configure_modes\n"
            "configure_modes(web=True)\n"
            "configure_modes(web=True)\n"   # the scheduler's call, after the startup's
            "print('ok')\n" % [r[2] for r in ROUTERS]
        )
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path),
               "TZ": "UTC",
               "DASHBOARD_SECRET_KEY": os.environ["DASHBOARD_SECRET_KEY"],
               "BOT_TOKEN": os.environ["BOT_TOKEN"],
               **{s: "junk-s14-05" for s in SWITCHES}}
        run = subprocess.run([sys.executable, "-I", "-c", code], cwd=tmp_path, env=env,
                             capture_output=True, text=True, timeout=180)
        assert run.returncode == 0, run.stderr[-3000:]
        assert run.stdout.strip().endswith("ok")

    def test_health_answers_and_publishes_none_of_them(self, monkeypatch):
        from fastapi.testclient import TestClient

        from core.runtime_modes import configure_modes
        from web.main import app

        # What `configure_modes` caches here, restored after the test (the
        # duckdb switch and the warehouse writer are conftest's to reset).
        for name in ("_misconfigured", "_unreported", "_configured"):
            monkeypatch.setattr(read_fallback, name, getattr(read_fallback, name))
        for switch in SWITCHES:
            monkeypatch.setenv(switch, "junk-s14-05")
        configure_modes(web=True)
        response = TestClient(app).get("/api/health")
        assert response.status_code == 200, response.text
        block = response.json()["read_fallback_mode"]
        assert not any(s in line for line in block["misconfigured"] for s in SWITCHES)


class TestNothingGoesRoundTheRouter:
    @pytest.mark.parametrize("path", [
        "core/repositories/traffic.py", "core/repositories/margin.py",
        "core/repositories/products_intel.py",
    ])
    def test_no_method_of_these_tabs_opens_the_store(self, path):
        """Their methods read only through the router, so the router's one
        path is the tab's."""
        import ast

        source = (ROOT / path).read_text(encoding="utf-8")
        for node in ast.walk(ast.parse(source)):
            if not isinstance(node, ast.AsyncFunctionDef) or not node.name.startswith(
                    ("get_traffic", "get_margin", "get_basket", "get_frequently",
                     "get_category_combinations", "get_brand_affinity",
                     "get_product_momentum")):
                continue
            calls = {n.func.attr for n in ast.walk(node)
                     if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
            assert not calls & {"connection", "_fetch_one", "_fetch_all", "execute"}, (
                node.name)

    @pytest.mark.parametrize("path,router", TAB_MODULES, ids=[p for p, _ in TAB_MODULES])
    def test_nothing_but_the_router_reaches_an_engine(self, path, router):
        """The other half of "through the router": a method that called
        Postgres itself — `await pg_traffic_read.fetch(...)` with no `try` —
        would answer an engine failure with an uncounted 500, and neither the
        test above (it looks for DuckDB) nor rule 5 (it judges `_*_run`
        routers) would see it. Read off the module, so a reader added
        tomorrow — the pixel-only breakdown was one — is held the day it
        lands. Mutations: a bare engine call by module or by name in
        `get_traffic_pixel_only`, or a switch read there."""
        source = (ROOT / path).read_text(encoding="utf-8")
        routers, reaches = _what_reaches_round_the_router(source)
        assert routers == {router}, routers
        assert reaches == {}, reaches
        # Non-vacuity: the walk read the tab's readers, not nothing.
        readers = [n.name for n in ast.walk(ast.parse(source))
                   if isinstance(n, ast.AsyncFunctionDef)
                   and n.name.startswith("get_")]
        assert len(readers) >= 6, readers

    @pytest.mark.parametrize("body,kind", [
        ("from core import pg_traffic_read\n"
         "async def get_x(self):\n"
         "    return await pg_traffic_read.fetch('q', [])\n", "engine"),
        ("from core import pg_traffic_read as t\n"
         "async def get_x(self):\n"
         "    return await t.fetch('q', [])\n", "engine"),
        ("async def get_x(self):\n"
         "    from core.pg_traffic_read import fetch\n"
         "    return await fetch('q', [])\n", "engine"),
        ("import core.pg_traffic_read\n"
         "async def get_x(self):\n"
         "    return await core.pg_traffic_read.fetch('q', [])\n", "engine"),
        ("async def get_x(self):\n"
         "    from core.pg import get_pool\n"
         "    pool = await get_pool()\n", "engine"),
        ("async def get_x(self):\n"
         "    import asyncpg\n", "engine"),
        ("async def get_x(self):\n"
         "    read_fallback.no_address('traffic', None)\n", "old shape"),
        ("async def get_x(self):\n"
         "    if x.enabled() and x.available():\n"
         "        pass\n", "old shape"),
        ("import os\n"
         "async def get_x(self):\n"
         "    return os.getenv('KS_READ_TRAFFIC')\n", "environment"),
        ("import os\n"
         "async def get_x(self):\n"
         "    return os.environ['KS_READ_TRAFFIC']\n", "environment"),
        ("async def get_x(self):\n"
         "    return 'KS_READ_TRAFFIC'\n", "environment"),
    ], ids=["module", "module-alias", "imported-name", "dotted-module", "get_pool",
            "asyncpg", "no_address", "enabled-available", "getenv", "environ",
            "switch-name"])
    def test_the_walk_bites(self, body, kind):
        source = ("class T:\n"
                  "    async def _t_run(self, sql):\n"
                  "        return await pg_traffic_read.fetch(sql, [])\n\n"
                  + "\n".join("    " + line if line else line
                              for line in body.splitlines()) + "\n")
        routers, reaches = _what_reaches_round_the_router(source)
        assert routers == {"_t_run"}
        assert any(kind in found for found in reaches.get("get_x", [])), reaches

    def test_a_docstring_naming_a_switch_is_not_a_read(self):
        source = ("class T:\n"
                  "    async def get_x(self):\n"
                  "        '''`KS_READ_TRAFFIC` is not read here.'''\n"
                  "        return await self._t_run('q')\n")
        assert _what_reaches_round_the_router(source) == (set(), {})

    def test_the_duckdb_category_helper_is_gone(self):
        """`_get_category_with_children` ran the tree on a DuckDB connection
        its caller held. Its last callers were the dashboard's DuckDB tails,
        which resolve the tree through `_category_ids` now."""
        from core.duckdb_store import DuckDBStore

        assert not hasattr(DuckDBStore, "_get_category_with_children")
