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
from tests.helpers.router_walk import _what_reaches_round_the_router

ROOT = Path(__file__).resolve().parents[2]

# The three tabs whose every reader lives in one module beside its router.
# (`revenue.py` holds the other three routers among the dashboard's gated
# reads, which are S14-06's.)
TAB_MODULES = [
    ("core/repositories/traffic.py", "_traffic_run"),
    ("core/repositories/margin.py", "_margin_run"),
    ("core/repositories/products_intel.py", "_intel_run"),
]


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
