"""No read falls back to DuckDB, and every router refuses — found by a walk.

Every router reads one engine and refuses its failure through
`core.read_fallback.refuse`, which counts the refusal and hands back the
`ReadUnavailable` an HTTP route answers 503 naming the surface. What makes
that hold everywhere is the walk below, which reads `core/` and `web/` for the
shapes a read reaching DuckDB took rather than trusting a list of routers — a
list only ever guards the routers somebody was already thinking about, and
`pg_expenses_read.backfilled()` is the proof: it served DuckDB for weeks under
log text no grep for "falling back to DuckDB" could find, because it was
neither a router nor a gate.

THE SHAPES, AND WHAT EACH MUST DO

1. **A log call saying "falling back to DuckDB"** (docstrings are not calls,
   so they do not count). None may stand where a failure is handled without
   being raised again: there is no DuckDB left to fall back to.
2. **An `X.enabled() and X.available()` gate** — the shape every read switch
   used to choose an engine. None is left since step 14's S14-07, and none
   may come back: a gate anywhere is a violation.
3. **Any except handler in a function that reaches another engine's read** —
   `get_pool`, a `*_run` router, or a function of a read-switch module — under
   `core/repositories/` or `core/pg_*_read*.py`. A handler that returns is
   judged too: `backfilled()` returned False and `_pg_gold_summary` None, and
   it was the *caller* that then read DuckDB — invisible from the handler, so
   every handler in that scope has to decide: re-raise, or say in the same
   `try` what `ReadUnavailable` means.

5. **A router refuses every engine failure** (step 14, S14-05). A `_*_run`
   router under `core/repositories/` — `_traffic_run`, `_margin_run`,
   `_intel_run`, `_reports_run`, `_marketing_run` and `_lookups_run` first,
   the dashboard's `_dashboard_run` since S14-06 — or any function there that
   calls `refuse` at all (the dashboard's `_pg_gold_summary`,
   `_pg_gold_series`, `_pg_silver` and `get_manager_sales_365d`), must run
   every engine call
   (a function of a `core.pg_*` module it imports — or, since S14-07, of a
   `core.ch_*` one — however it is spelled, or `get_pool`) inside a
   `try` whose handler for `Exception` is exactly `raise
   read_fallback.refuse("<surface>", <the caught exception>)`, and reach
   DuckDB through nothing at all. One bare `await pg_x.fetch()` outside the
   `try` would turn the counted 503 naming the surface into an uncounted
   500; a handler that falls back, returns or raises something else would
   lose the surface or the count.

Rule 4 asked every gate whether its switch had an address
(`read_fallback.no_address`), until step 14's S14-16 deleted it: no gate is
left to ask. Rules 1 to 3 counted a fallback through `read_fallback.fall_back`
until then; it is deleted too, so re-raising is the only decision left.

A read-switch module is one whose `ENV` names a `KS_READ_*` variable, found by
walking `core/` — not listed either. None is left since S14-07.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core import read_fallback
from tests.helpers.engine_walk import sites, walk_source  # noqa: F401

# What a handler that carries on toward DuckDB looks like when it reads it
# itself. Deliberately loose: a false positive costs a decision somebody has
# to write, a false negative costs a silent fallback.
DUCKDB_READS = {"connection", "get_store", "_fetch_one", "_fetch_all"}


# ─── The walk ──────────────────────────────────────────────────────────────


# ─── The walk shown synthetic code, so each rule is seen to bite ───────────

class TestTheRules:
    SWITCHES = {"pg_x_read"}

    def _ok(self, source, relpath="core/repositories/x.py",
            rules=("log", "gate", "handler")):
        return [(s.rule, s.ok) for s in walk_source(source, relpath, self.SWITCHES)
                if s.rule in rules]

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

    def test_any_gate_is_found(self):
        """Rule 2 since step 14's S14-16: a gate is a read switch choosing an
        engine, and none is left — whatever its handlers do. (Until then a
        gate whose handler counted its fallback through `fall_back`, or
        ended the step, passed.)"""
        src = (
            "async def f(self):\n"
            "    if pg_x_read.enabled() and pg_x_read.available():\n"
            "        try:\n"
            "            return await pg_x_read.fetch()\n"
            "        except Exception:\n"
            "            raise\n"
            "    return None\n"
        )
        assert self._ok(src, "core/somewhere.py") == [("gate", False)]

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

    # Rule 5: a router with no tail.
    REFUSING = (
        "from core import read_fallback\n"
        "class M:\n"
        "    async def _x_run(self, sql):\n"
        "        from core import pg_x_read\n"
        "        try:\n"
        "            return await pg_x_read.fetch(sql)\n"
        "        except Exception as exc:  # noqa: BLE001\n"
        "            raise read_fallback.refuse('x', exc)\n"
    )

    def _refuse_ok(self, source):
        return [s.ok for s in walk_source(source, "core/repositories/x.py",
                                          self.SWITCHES) if s.rule == "refuse"]

    def test_a_router_that_refuses_passes(self):
        assert self._refuse_ok(self.REFUSING) == [True]

    def test_an_engine_call_outside_the_try_is_found(self):
        """The bare `await` the plan names: an uncounted 500, not a 503."""
        src = self.REFUSING.replace(
            "        try:\n",
            "        rows = await pg_x_read.fetch('SELECT 1')\n        try:\n")
        assert self._refuse_ok(src) == [False]

    # Every spelling of an engine call but `<alias>.<fn>(...)` and a bare
    # `get_pool(...)`, which the rule alone judged until the review of step
    # 14's S14-06 merge: the first passed a `fetch_series()` imported by its
    # name, outside the `try`, in a dashboard helper.
    SPELLINGS = [
        ("        from core.pg_x_read import fetch_y\n", "fetch_y('SELECT 1')"),
        ("        from core.pg_x_read import fetch_y as f\n", "f('SELECT 1')"),
        ("        from core.pg import get_pool as gp\n", "gp()"),
        ("        from core import pg\n", "pg.get_pool()"),
        ("        import core.pg_x_read\n", "core.pg_x_read.fetch('SELECT 1')"),
        # ClickHouse's modules, the cohorts' one engine since step 14's
        # S14-07, in each spelling: S14-07 taught the rule the module's
        # alias, and the merge of S14-06's review the rest.
        ("        from core import ch_x as c\n", "c.fetch('SELECT 1')"),
        ("        from core.ch_x import fetch\n", "fetch('SELECT 1')"),
        ("        import core.ch_x\n", "core.ch_x.fetch('SELECT 1')"),
    ]
    SPELLING_IDS = ["imported_name", "imported_alias", "get_pool_alias",
                    "get_pool_attribute", "spelled_out", "clickhouse_alias",
                    "clickhouse_imported_name", "clickhouse_spelled_out"]

    @pytest.mark.parametrize("binding,call", SPELLINGS, ids=SPELLING_IDS)
    def test_every_spelling_outside_the_try_is_found(self, binding, call):
        src = self.REFUSING.replace(
            "        try:\n", f"{binding}        rows = await {call}\n        try:\n")
        assert self._refuse_ok(src) == [False]

    @pytest.mark.parametrize("binding,call", SPELLINGS, ids=SPELLING_IDS)
    def test_every_spelling_inside_the_try_is_refused(self, binding, call):
        src = self.REFUSING.replace(
            "        try:\n", f"{binding}        try:\n            await {call}\n")
        assert self._refuse_ok(src) == [True]

    def test_a_name_imported_at_the_top_is_read_too(self):
        src = ("from core.pg_x_read import fetch_y\n"
               + self.REFUSING.replace(
                   "        try:\n", "        rows = await fetch_y(sql)\n        try:\n"))
        assert self._refuse_ok(src) == [False]

    @pytest.mark.parametrize("handler", [
        "            return []\n",
        "            read_fallback.fall_back('x', exc)\n",
        "            raise\n",
        "            raise read_fallback.ReadUnavailable('x')\n",
        "            raise read_fallback.refuse(surface, exc)\n",
        "            raise read_fallback.refuse('x', None)\n",
        "            raise read_fallback.refuse('x', exc) from exc\n",
    ], ids=["returns", "falls_back", "re_raises", "no_count", "computed_surface",
            "no_cause", "from"])
    def test_any_other_handler_is_found(self, handler):
        src = self.REFUSING.replace(
            "            raise read_fallback.refuse('x', exc)\n", handler)
        assert self._refuse_ok(src) == [False]

    def test_a_narrow_handler_does_not_refuse_everything(self):
        src = self.REFUSING.replace("except Exception as exc", "except OSError as exc")
        assert self._refuse_ok(src) == [False]

    def test_a_duckdb_tail_after_the_refusal_is_found(self):
        src = self.REFUSING + (
            "        async with self.connection() as conn:\n"
            "            return conn.execute(sql).fetchall()\n")
        assert self._refuse_ok(src) == [False]

    def test_a_chain_routed_tail_is_left_to_its_own_pr(self):
        """`_sms_run`'s shape: no gate, a branch on the chain's own question
        and a DuckDB tail — no refusal anywhere, so not this rule's yet."""
        src = (
            "class M:\n"
            "    async def _x_run(self, sql):\n"
            "        from core import pg_x_read\n"
            "        if store_is_postgres():\n"
            "            return await pg_x_read.fetch(sql)\n"
            "        async with self.connection() as conn:\n"
            "            return conn.execute(sql).fetchall()\n"
        )
        assert self._refuse_ok(src) == []

    def test_a_gated_router_is_left_to_the_old_rules(self):
        src = (
            "class M:\n"
            "    async def _x_run(self, sql):\n"
            "        from core import pg_x_read\n"
            "        if pg_x_read.enabled() and pg_x_read.available():\n"
            "            return await pg_x_read.fetch(sql)\n"
            "        return []\n"
        )
        assert self._refuse_ok(src) == []

    def test_any_function_that_refuses_is_held_to_it_whatever_its_name(self):
        """The dashboard's `_pg_gold_summary` and `get_manager_sales_365d` are
        not `_*_run` (step 14, S14-06). A refusal anywhere makes the function
        this rule's: the clean shape passes, a second engine call outside
        the `try` is found, and a name that neither ends in `_run` nor
        refuses is not this rule's at all."""
        clean = self.REFUSING.replace("_x_run", "get_x")
        assert self._refuse_ok(clean) == [True]
        bare = clean.replace(
            "        try:\n",
            "        rows = await pg_x_read.fetch('SELECT 1')\n        try:\n")
        assert self._refuse_ok(bare) == [False]
        silent = clean.replace(
            "            raise read_fallback.refuse('x', exc)\n", "            return []\n")
        assert self._refuse_ok(silent) == []

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
        assert "expenses" not in read_fallback.refusals()


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


# ─── Through the app ───────────────────────────────────────────────────────

