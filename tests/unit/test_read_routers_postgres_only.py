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
  13's readiness on `/api/warehouse/status` read the six variables, as
  `reader:*` keys, until S14-15 deleted it);
- a failure is the same refusal `fall_back` made under `off`: the surface,
  the count, the log line and the cause (since step 14's S14-16 `fall_back`
  is gone, and the line no longer names `KS_READ_FALLBACK`);
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
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

# The three tabs whose every reader lives in one module beside its router.
# (`revenue.py` holds the other three routers among the dashboard's gated
# reads, which are S14-06's.)
TAB_MODULES = [
    ("core/repositories/traffic.py", "_traffic_run"),
    ("core/repositories/margin.py", "_margin_run"),
    ("core/repositories/products_intel.py", "_intel_run"),
]
OLD_SHAPE = {"fall_back", "no_address", "no_engine", "enabled", "available"}
# Postgres (`core.pg`, `core.pg_*`), the driver, and — since step 14's S14-07,
# whose cohorts read ClickHouse alone — ClickHouse's modules (`core.ch_*`).
ENGINE_MODULES = ("core.pg", "core.ch_", "asyncpg")


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
    of a `core.pg*` module — or, since step 14's S14-07, of a `core.ch_*`
    one — by the module or imported by name; `get_pool`; an import of
    `core.pg*`, `core.ch_*` or `asyncpg` of its own), the old shape's
    machinery (`fall_back`, `no_address`, `no_engine`, `enabled`,
    `available`), or the environment (`os.getenv`, `os.environ`, a
    `KS_READ_*` name outside a docstring)."""
    tree = ast.parse(source)
    modules, functions = set(), set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module:
            if n.module == "core":
                modules |= {a.asname or a.name for a in n.names
                            if a.name.startswith(("pg", "ch_"))}
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
        # ClickHouse, the cohorts' one engine since step 14's S14-07.
        ("from core import ch_cohorts\n"
         "async def get_x(self):\n"
         "    return await ch_cohorts.fetch('q', [], ())\n", "engine"),
        ("async def get_x(self):\n"
         "    from core.ch_cohorts import fetch\n"
         "    return await fetch('q', [], ())\n", "engine"),
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
            "asyncpg", "clickhouse-module", "clickhouse-imported-name",
            "no_address", "enabled-available", "getenv", "environ",
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
