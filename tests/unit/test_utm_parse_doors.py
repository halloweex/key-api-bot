"""The UTM verdicts' four doors, and where a full parse may be forced.

Postgres parses `silver.order_utm` itself (`core/pg_utm_parse.py`): the
derivation's last step, and four doors outside it — `POST
/api/traffic/refresh`, `/traffic/reclassify`, the `manager_comment` backfill
behind `/traffic/backfill-utm`, and `scripts/backfill_utm.py`. What is pinned
here:

- nothing outside the parse's own module and the derivation's last step
  calls a parse directly: the doors go through `reparse_router`;
- the router's answers;
- the two routes, as an admin calls them over the test Postgres: the refresh
  only adds, the reclassify re-parses everything, and over a shrunk
  `bronze.orders` the reclassify is refused and the table kept;
- the backfill CLI, as an operator runs it: it re-parses whole, and a shrink
  replaces the table only behind `--force-ship`.

That the parse writes every column the table has is held in
`tests/integration/test_pg_utm_parse.py`.
"""
from __future__ import annotations

import ast
import os
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_utm_parse
from tests.helpers.traffic_utm_doors import (
    IDS,
    NEW,
    PAID_VERDICT,
    STALE_VERDICT,
    age_the_rules,
    commented,
    lose_comments,
    run_backfill_cli,
    stored_rows,
    ten_parsed_orders,
    utm_failures,
    verdicts,
)
from tests.integration.pg_tab_seed import needs_postgres

REPO = Path(__file__).resolve().parents[2]
PRODUCTION_ROOTS = ("core", "web", "scripts", "bot", "deploy")
DSN = os.getenv("KS_PG_DSN")

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


class TestEveryDoorGoesThroughTheRouter:
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


@needs_postgres
class TestTheTwoRoutes:
    """`POST /api/traffic/refresh` and `/traffic/reclassify`, an admin's.

    The reclassify is the only way a rule change reaches an order whose
    `updated_at` has not moved, so it re-parses everything; the refresh only
    adds, or every refresh would re-parse 33 K comments. Ten orders parsed,
    then the first one's verdict left as an earlier rule stored it and the
    last one's comment emptied in KeyCRM since."""

    @pytest.mark.asyncio
    async def test_the_refresh_adds_what_is_new_and_rewrites_nothing(self, monkeypatch):
        from tests.helpers.app_client import signed_in

        async with ten_parsed_orders(DSN, monkeypatch) as conn:
            await age_the_rules(conn)
            await commented(conn, NEW)
            async with signed_in("admin") as admin:
                response = await admin.post("/api/traffic/refresh")
            stored = await verdicts(conn)

        assert response.status_code == 200
        assert response.json()["utm_orders_parsed"] == 1
        assert stored == {**{oid: PAID_VERDICT for oid in IDS},
                          IDS[0]: STALE_VERDICT, NEW: PAID_VERDICT}

    @pytest.mark.asyncio
    async def test_the_reclassify_rewrites_a_stale_verdict_and_drops_an_emptied_one(
            self, monkeypatch):
        from tests.helpers.app_client import signed_in

        async with ten_parsed_orders(DSN, monkeypatch) as conn:
            await age_the_rules(conn)
            async with signed_in("admin") as admin:
                response = await admin.post("/api/traffic/reclassify")
            stored = await verdicts(conn)

        assert response.status_code == 200
        assert response.json()["utm_records"] == len(IDS) - 1
        assert stored == {oid: PAID_VERDICT for oid in IDS[:-1]}

    @pytest.mark.asyncio
    async def test_a_reclassify_over_a_shrunk_bronze_is_refused_and_the_table_kept(
            self, monkeypatch):
        """A `bronze.orders` truncated, half-restored or read from the wrong
        database would replace the verdicts with less than the truth, and a
        missing verdict is a gap nobody sees: the order falls through to
        organic or unattributed, on /traffic and in the Monday report."""
        from tests.helpers.app_client import signed_in

        async with ten_parsed_orders(DSN, monkeypatch) as conn:
            before = await stored_rows(conn)
            await conn.execute("DELETE FROM bronze.orders WHERE id <> $1", IDS[0])
            async with signed_in("admin") as admin:
                response = await admin.post("/api/traffic/reclassify")
            after = await stored_rows(conn)

        assert response.status_code == 409
        assert after == before


@needs_postgres
class TestTheBackfillCli:
    """`scripts/backfill_utm.py` ends on a full re-parse, because the comments
    it restores may change any verdict; and it is the one caller that may
    mean a shrink, so only it can force one — behind `--force-ship`, a flag
    an operator types. Run as the operator runs it, against a KeyCRM with no
    comment to restore."""

    @pytest.mark.asyncio
    async def test_it_reparses_whole(self, monkeypatch):
        async with ten_parsed_orders(DSN, monkeypatch) as conn:
            await age_the_rules(conn)
            await run_backfill_cli(monkeypatch)
            stored = await verdicts(conn)

        assert stored == {oid: PAID_VERDICT for oid in IDS[:-1]}

    @pytest.mark.asyncio
    async def test_without_its_flag_a_shrink_is_refused(self, monkeypatch):
        async with ten_parsed_orders(DSN, monkeypatch) as conn:
            before = await stored_rows(conn)
            await lose_comments(conn)
            await run_backfill_cli(monkeypatch)
            after, failures = await stored_rows(conn), await utm_failures(conn)

        assert after == before
        assert failures == 1, "the re-parse did not run, or was not refused"

    @pytest.mark.asyncio
    async def test_with_its_flag_a_shrink_replaces_the_table(self, monkeypatch):
        async with ten_parsed_orders(DSN, monkeypatch) as conn:
            await lose_comments(conn)
            await run_backfill_cli(monkeypatch, "--force-ship")
            stored = await verdicts(conn)

        assert stored == {IDS[0]: PAID_VERDICT}
