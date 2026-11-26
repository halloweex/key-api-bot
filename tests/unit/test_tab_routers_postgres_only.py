"""Step 14's S14-07: the third set of readers asks one engine and nothing else.

Four routers used to choose an engine by a `KS_READ_*` switch, ask
`read_fallback.no_address` about it, try that engine, and on a failure call
`read_fallback.fall_back` — which under `KS_READ_FALLBACK=off`, production's
mode since 2026-10-09 and since S14-00 the only one, refused the read: a 503
naming the surface, counted under `read_fallback_mode.refused`:

- `_expenses_run` (`/expenses`, `KS_READ_EXPENSES`), with the history gate:
  a statement reading `{expenses}` while `bronze.expenses` has no backfilled
  history had no engine that could answer it;
- `_inventory_batch` and `_inventory_rows` (`/inventory`, `KS_READ_INVENTORY`);
- `_goals_run` and `_goal_tables_run` (`/goals`, the smart goal, the
  forecast; `KS_READ_GOALS` and the chains' `reads_the_chain`);
- `_analytics_rows` (the cohorts, `KS_READ_COHORTS`), whose one engine is
  ClickHouse: under `off` a switch not naming it, or no `KS_CH_URL`, was
  refused by `no_engine`/`no_address`.

Each keeps exactly the path production took: the engine read, and on any
exception `raise read_fallback.refuse(surface, exc)`. Five readers never had
a fallback — the training frame (`KS_READ_FORECAST_INPUT`), the weekly report
(`KS_READ_WEEKLY`), the assistant's tools (`KS_READ_CHAT`), the buyer-sync
selection (`KS_READ_BUYER_SYNC`) and the search index
(`KS_READ_SEARCH_INDEX`) — and read Postgres alone now, a failure their own
error as before. What is held here, without a database:

- no switch is read — unset, `duckdb`, a typo or junk, Postgres (ClickHouse
  for the cohorts) is asked, and nothing reaches for the DuckDB store;
- a router's failure is the refusal `fall_back` made under `off`: the
  surface, the count, the log line, the cause, and no fallback counted; so is
  no `KS_PG_DSN` (no `KS_CH_URL` for the cohorts), and the expenses' history
  not across; chain 7a's `app.revenue_goals`, read raw before S14-07, is
  refused the same way, and through the app that is a `read_refused` page
  (an accepted change in failure mode);
- a reader with no fallback lets its own error through, uncounted, as it
  did;
- nothing in the routers' files reads an engine round them — S14-05's walk,
  which the merge of S14-06 brought: only a router, a reader rule 5 holds
  clean, or a function that writes, by its writes alone (never a writer
  module's reader); the cohorts' ClickHouse through `_analytics_rows` alone;
- no reader module carries `ENV`, `enabled` or `available` — these nine
  and every other `core/pg_*_read*.py` — and `ch_cohorts` no
  `KS_READ_COHORTS`;
- junk in every one of those variables, and in `KS_GOALS_HISTORY`, starts
  web and imports every module (rule 3 of the plan).

The numbers are the `tests/integration/*_two_engines.py` files', against a
real Postgres and literals DuckDB answered before its side went; every route
through the real app is `tests/unit/test_read_fallback_http.py`'s.
"""
from __future__ import annotations

import ast
import functools
import importlib
import re
from pathlib import Path
from typing import Dict, FrozenSet, List, Optional

import pytest

ROOT = Path(__file__).resolve().parents[2]


class TestChain7aAFailedReadPages:
    """An accepted change in failure mode (the review of S14-07, decided):
    a failed read of chain 7a's `app.revenue_goals` — `GET /api/goals`, the
    smart goal's goals block — was let through raw before S14-07, an
    uncounted 500 that paged nothing; it is the counted 503 naming `goals`
    now, published under `read_fallback_mode.refused`, and the canary pages
    `read_refused` (WARN) when it is the only read that fails. The same
    change S14-05 made for /marketing's goal line and S14-06 for the returns
    list. Mutation: read the chain's table raw again."""

    def test_only_the_stored_goals_failing_is_a_counted_503_and_a_page(
        self, admin_client, fresh_read_fallback, monkeypatch,
    ):
        from bot import canary
        from core import pg_goals_read

        async def fetch(sql, params):
            if "app.revenue_goals" in sql:
                raise OSError('relation "app.revenue_goals" does not exist')
            return []

        monkeypatch.setattr(pg_goals_read, "fetch", fetch)

        response = admin_client.get("/api/goals")
        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "goals"

        health = admin_client.get("/api/health").json()
        assert health["read_fallback_mode"]["refused"]["goals"]["count"] == 1
        assert "read_fallbacks" not in health
        [(key, message)] = canary.check_read_refusals(health)
        assert key == "read_refused"
        assert message.startswith(canary.READ_REFUSED_LINE + "goals ×1")


# The files of the routers above, with the `_*_run` routers in each and the
# readers there that refuse for themselves (rule 5 holds both).
THIRD_SET = [
    ("core/repositories/expenses.py", {"_expenses_run"}, set()),
    ("core/repositories/inventory.py", set(), {"_inventory_batch"}),
    ("core/repositories/goals.py", {"_goals_run", "_goal_tables_run"}, set()),
]
COHORTS_FILE = "core/repositories/customers.py"
# A chain's writer, which the files above call to write: a write is not a
# read round the router, and its failure is the writer's own (S14-11 takes
# the chains' mode away). Every other engine module is a read's. A module
# matching this is no licence by itself: it carries readers too
# (`pg_inventory_write.read_snapshot_calendar`, the chains' `preflight`), so
# what the files may call in one is decided by the function (`_writer_reaches`).
# `core.pg_operational` matched too, until step 14's S14-13 deleted it.
WRITE_MODULE = re.compile(r"core\.pg_[a-z0-9_]+_write")
# The writers' mode, which every write asks first; constant since S14-00.
MODE_PREDICATE = "writes_postgres"


def _engine_bindings(tree: ast.AST) -> Dict[str, str]:
    """Every name the file binds to an engine module or to a function of
    one, and the module: `from core import pg_x as y`, `from core.pg_x
    import f`, `import core.pg_x as y`."""
    from tests.helpers.engine_walk import ENGINE_PREFIXES, _is_engine_module

    out: Dict[str, str] = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "core":
            for a in n.names:
                if a.name == "pg" or a.name.startswith(ENGINE_PREFIXES):
                    out[a.asname or a.name] = f"core.{a.name}"
        elif isinstance(n, ast.ImportFrom) and n.module and _is_engine_module(n.module):
            for a in n.names:
                out[a.asname or a.name] = n.module
        elif isinstance(n, ast.Import):
            for a in n.names:
                if a.asname and _is_engine_module(a.name):
                    out[a.asname] = a.name
    return out


def _imported_names(tree: ast.AST) -> Dict[str, str]:
    """`{alias: name}` for every function the file imports out of an engine
    module by its name (`from core.pg_x import f as g`), so a reach through
    `g` is judged as `f`."""
    from tests.helpers.engine_walk import _is_engine_module

    return {a.asname or a.name: a.name
            for n in ast.walk(tree)
            if isinstance(n, ast.ImportFrom) and n.module and _is_engine_module(n.module)
            for a in n.names}


@functools.lru_cache(maxsize=None)
def _chain_writers(module: str) -> FrozenSet[str]:
    """The functions of a writer module that write: the latch walk's writers
    (`test_chain_latch._writers` — a public async function reaching a
    connection or a writing statement, bar the readers it names), then every
    function of the module calling one of them by name, to a fixed point
    (`pg_operational.replicate_after_manual_expense`, the hourly copy called
    sooner, was one until step 14's S14-13). A reader of the module — `read_snapshot_calendar`, `preflight`,
    `reading`, `reads_the_chain` — is none of them."""
    from tests.helpers.chain_writers import _writers

    mod = importlib.import_module(module)
    found = set(_writers(mod))
    tree = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
    functions = [n for n in tree.body
                 if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    grew = True
    while grew:
        grew = False
        for fn in functions:
            if fn.name in found:
                continue
            called = {getattr(c.func, "id", getattr(c.func, "attr", ""))
                      for c in ast.walk(fn) if isinstance(c, ast.Call)}
            if called & found:
                found.add(fn.name)
                grew = True
    return frozenset(found)


def _called_name(reach: str, names: Dict[str, str]) -> Optional[str]:
    """The function one engine reach calls or holds, by its name in its
    module; None for an import, which names no function."""
    what = reach[len("engine: "):]
    if what.startswith("imports "):
        return None
    name = (what[:-2] if what.endswith("()") else what).rsplit(".", 1)[-1]
    return names.get(name, name)


def _writer_reaches(seen: List[str], bindings: Dict[str, str],
                    names: Dict[str, str]) -> List[str]:
    """Which of one function's reaches its writing allows: none unless the
    function writes — calls a function `_chain_writers` counts in a writer
    module — and then its writes, its imports of writer modules and their
    mode predicate. A writer module's reader is never allowed, from a writer
    or a reader alike."""
    def allowed(reach: str, writes_only: bool) -> bool:
        module = _module_of(reach, bindings)
        if module is None or not WRITE_MODULE.fullmatch(module):
            return False
        name = _called_name(reach, names)
        if name is not None and name in _chain_writers(module):
            return True
        return not writes_only and (name is None or name == MODE_PREDICATE)

    if not any(allowed(r, writes_only=True) for r in seen):
        return []
    return [r for r in seen if allowed(r, writes_only=False)]


def _module_of(reach: str, bindings: Dict[str, str]) -> Optional[str]:
    """The engine module one reach of S14-05's walk names, or None for a
    reach that is not an engine's (the old shape, the environment)."""
    if not reach.startswith("engine: "):
        return None
    what = reach[len("engine: "):]
    if what.startswith("imports "):
        return what[len("imports "):]
    name = what[:-2] if what.endswith("()") else what
    if name.startswith("core."):
        return name.rsplit(".", 1)[0]
    head = name.split(".", 1)[0]
    if head in bindings:
        return bindings[head]
    return "core.pg" if name.rsplit(".", 1)[-1] == "get_pool" else None


def round_the_routers(source: str, relpath: str) -> List[str]:
    """What in `source` reads an engine round its routers: S14-06's
    `round_a_refusal` — S14-05's walk of the tab modules, allowing a router
    nothing and a reader rule 5 holds clean only the engine calls rule 5
    judges — less what a function that writes reaches to write
    (`_writer_reaches`). Judged per function: a module allowance let any
    reader call a writer module's own reader (`read_snapshot_calendar` in
    `get_stock_summary`) and pass."""
    from collections import Counter

    from tests.helpers.router_walk import round_a_refusal

    tree = ast.parse(source)
    bindings, names = _engine_bindings(tree), _imported_names(tree)
    by_function: Dict[str, List[str]] = {}
    for line in round_a_refusal(source, relpath):
        function, reach = line.split(": ", 1)
        by_function.setdefault(function, []).append(reach)
    out = []
    for function, seen in by_function.items():
        left = Counter(seen) - Counter(_writer_reaches(seen, bindings, names))
        out += [f"{function}: {r}" for r in sorted(left.elements())]
    return out


class TestNothingGoesRoundTheRouter:
    """The merge of S14-06 (S14-05's merge of main, #327's pixel-only
    breakdown) brought the walk that holds /traffic's, /margin's and product
    intelligence's readers to their router, and S14-06 held the dashboard to
    it: a reader calling its engine itself answers a failure with an
    uncounted 500, which rule 5 (it judges routers and what refuses) and the
    HTTP sweep's violations (they judge refusals) never see. Here the same
    walk holds this PR's: every function of `/expenses`', `/inventory`'s and
    `/goals`' files reaches an engine only as a router, as a reader rule 5
    holds clean, or as a function that writes — and that one only by its
    writes, by importing the writer's module and by asking its
    `writes_postgres`, never by a writer module's reader; the cohorts' file —
    half of it the SMS tab's, which keeps its own router until S14-08 — reads
    ClickHouse through `_analytics_rows` alone, and the cohort readers reach
    no engine at all. Mutations: `await pg_inventory_read.fetch_many(...)` in
    `get_stock_summary`, `await pg_goals_read.fetch(...)` in `set_goal`,
    `await ch_cohorts.fetch(...)` in `get_cohort_retention`, and
    `await pg_inventory_write.read_snapshot_calendar(date.today())` in
    `get_stock_summary` (X4 of the merge's review: a writer module's reader,
    which the module allowance let through)."""

    @pytest.mark.parametrize("path,routers,refusing", THIRD_SET,
                             ids=[p for p, _r, _f in THIRD_SET])
    def test_nothing_but_the_router_reads_an_engine(self, path, routers, refusing):
        from tests.helpers.engine_walk import read_switch_modules, walk_source
        from tests.helpers.router_walk import _what_reaches_round_the_router

        source = (ROOT / path).read_text(encoding="utf-8")
        found, reaches = _what_reaches_round_the_router(source)
        assert found == routers, found
        clean = {s.function for s in walk_source(source, path, read_switch_modules())
                 if s.rule == "refuse" and s.ok}
        assert refusing <= clean, clean
        assert round_the_routers(source, path) == []
        # Non-vacuity: the walk read the tab's readers, and saw the writers
        # beside them reach their chain's writer, which is all it allowed —
        # every one of them by a write, not by the module alone.
        tree = ast.parse(source)
        readers = [n.name for n in ast.walk(tree)
                   if isinstance(n, ast.AsyncFunctionDef) and n.name.startswith("get_")]
        assert len(readers) >= 4, readers
        bindings, names = _engine_bindings(tree), _imported_names(tree)
        writers = {fn for fn, seen in reaches.items() if fn not in refusing
                   and sorted(_writer_reaches(seen, bindings, names)) == sorted(seen)}
        assert len(writers) >= 3, sorted(reaches)
        assert not writers & set(readers), sorted(writers & set(readers))

    def test_only_the_cohort_router_reads_clickhouse(self):
        from tests.helpers.engine_walk import read_switch_modules, walk_source
        from tests.helpers.router_walk import _what_reaches_round_the_router

        source = (ROOT / COHORTS_FILE).read_text(encoding="utf-8")
        tree = ast.parse(source)
        bindings = _engine_bindings(tree)
        _routers, reaches = _what_reaches_round_the_router(source)
        clickhouse = {fn for fn, seen in reaches.items()
                      if any((_module_of(r, bindings) or "").startswith("core.ch_")
                             for r in seen)}
        assert clickhouse == {"_analytics_rows"}, clickhouse
        assert any(s.function == "_analytics_rows" and s.rule == "refuse" and s.ok
                   for s in walk_source(source, COHORTS_FILE, read_switch_modules()))
        cohort_readers = {
            fn.name for fn in ast.walk(tree)
            if isinstance(fn, ast.AsyncFunctionDef) and fn.name != "_analytics_rows"
            and any(isinstance(n, ast.Attribute) and n.attr == "_analytics_rows"
                    for n in ast.walk(fn))}
        assert len(cohort_readers) >= 4, cohort_readers
        assert not cohort_readers & set(reaches), {
            fn: reaches[fn] for fn in cohort_readers & set(reaches)}

    ROUTER = ("from core import read_fallback\n"
              "class T:\n"
              "    async def _t_run(self, sql):\n"
              "        from core import pg_goals_read\n"
              "        try:\n"
              "            return await pg_goals_read.fetch(sql, [])\n"
              "        except Exception as exc:\n"
              "            raise read_fallback.refuse('goals', exc)\n\n")
    WRITER = ("    async def set_x(self):\n"
              "        from core import pg_goals_write\n"
              "        if pg_goals_write.writes_postgres():\n"
              "            return await pg_goals_write.set_goal(1)\n")

    @pytest.mark.parametrize("writer", [
        WRITER,
        # Imported by its name and renamed: judged as `set_goal`.
        ("    async def set_x(self):\n"
         "        from core.pg_goals_write import set_goal as put, writes_postgres\n"
         "        if writes_postgres():\n"
         "            return await put(1)\n"),
    ], ids=["by_its_module", "by_another_name"])
    def test_a_writer_reaching_its_chain_passes(self, writer):
        assert round_the_routers(self.ROUTER + writer, "core/repositories/x.py") == []

    @pytest.mark.parametrize("method", [
        ("    async def get_x(self):\n"
         "        from core import pg_goals_read\n"
         "        return await pg_goals_read.fetch('q', [])\n"),
        ("    async def get_x(self):\n"
         "        from core import ch_cohorts\n"
         "        return await ch_cohorts.fetch('q', [], ())\n"),
        ("    async def get_x(self):\n"
         "        from core.pg import get_pool\n"
         "        return await get_pool()\n"),
        WRITER + "        return await pg_goals_read.fetch('q', [])\n",
        WRITER.replace("pg_goals_write.writes_postgres()",
                       "os.getenv('KS_READ_GOALS')"),
        # X4: a reader calling a writer module's reader.
        ("    async def get_x(self):\n"
         "        from core import pg_inventory_write\n"
         "        return await pg_inventory_write.read_snapshot_calendar(1)\n"),
        # A writer reaching the same reader by another name.
        WRITER + ("        from core.pg_inventory_write import read_snapshot_calendar as cal\n"
                  "        return await cal(1)\n"),
        # A writer reading through its own module's reader.
        WRITER + "        return await pg_inventory_write.read_snapshot_calendar(1)\n",
        # A function that writes nothing may not even ask the writers' mode.
        ("    async def get_x(self):\n"
         "        from core import pg_goals_write\n"
         "        return pg_goals_write.writes_postgres()\n"),
    ], ids=["bare_reader", "bare_clickhouse_reader", "bare_pool",
            "writer_reads_round_the_router", "writer_reads_a_switch",
            "reader_reads_a_writer_modules_reader",
            "writer_reads_it_by_another_name",
            "writer_reads_a_writer_modules_reader",
            "reader_asks_the_writers_mode"])
    def test_the_walk_bites(self, method):
        source = ("from core import pg_goals_read\nfrom core import pg_inventory_write\n"
                  + self.ROUTER + method)
        assert round_the_routers(source, "core/repositories/x.py") != []

    def test_a_writer_module_holds_readers_the_allowance_does_not_cover(self):
        """Non-vacuity of the per-function rule: the modules the files call
        carry readers, and `_chain_writers` counts none of them."""
        inventory = _chain_writers("core.pg_inventory_write")
        assert {"upsert_offers", "upsert_stocks", "record_inventory_snapshot"} <= inventory
        assert not {"read_snapshot_calendar", "preflight", MODE_PREDICATE} & inventory
        assert not {"reads_postgres", "reads_the_chain"} & _chain_writers("core.pg_goals_write")
