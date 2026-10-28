"""The carry of the catalogue rows only DuckDB holds (chain 6, OD-15 (a)).

`upsert_products` never deletes, so DuckDB keeps product 1055 — retired by
KeyCRM before the mirror existed — and the mirror can never ship it. Before
chain 6 moves the catalogue's writes to Postgres, a human carries it with
`POST /api/mirror/backfill/catalogue`. What is pinned here needs no database:
the selection rule, which is the daily comparison's own, and the route's and
the function's refusals, on the recording pool `test_write_chains` built for
every other backfill route.

Since step 14's S14-00 chain 6 writes the catalogue whatever its flag says,
so the carry refuses on the local answer every time — production's state
since the flip. The tests of a carry that runs (the dry run, the carry, the
pre-flip refusals on an owner row or the mirror off) went with that path,
as did `TestTheWayThereAndBack` in `tests/integration/test_catalogue_writer.py`.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core import pg_landing
from core.mirror_reconciliation import PRODUCTS_SPEC, compare_table
from tests.unit.test_write_chains import (  # noqa: F401 — fixtures
    CATEGORIES, PRODUCTS, _admin_client, _never_reached_postgres, flags,
    landing_chain, pool,
)

W = datetime(2026, 9, 30, 8, tzinfo=timezone.utc)


def _row(pid):
    return (pid, f"Product {pid}", None, None, None, None)


class TestTheSelectionIsTheComparisonsRule:
    def test_retired_is_at_or_before_the_last_ship_and_absent_from_postgres(self):
        rows = {1: _row(1), 2: _row(2), 1055: _row(1055), 3: _row(3), 4: _row(4)}
        synced = {1: W, 2: W, 1055: datetime(2026, 6, 13, tzinfo=timezone.utc),
                  3: W + timedelta(seconds=1), 4: W}
        retired, in_flight = pg_landing.retired_rows(rows, synced, frozenset({1, 2}), W)
        # 4 was written at exactly W and is absent: the mirror's last success
        # came after it, so `<=` — the comparison's own boundary.
        assert retired == [4, 1055]
        assert in_flight == [3]

    def test_a_missing_stamp_is_retired_and_a_naive_one_is_utc(self):
        rows = {5: _row(5), 6: _row(6), 7: _row(7)}
        synced = {5: None, 6: datetime(2026, 9, 30, 8), 7: datetime(2026, 9, 30, 8, 1)}
        retired, in_flight = pg_landing.retired_rows(rows, synced, frozenset(), W)
        assert retired == [5, 6] and in_flight == [7]

    def test_it_agrees_with_compare_table_row_for_row(self):
        """The check that named 1055 and the lever that carries it must not
        disagree about any row — so both are asked about the same rows."""
        rows = {i: _row(i) for i in range(1, 9)}
        synced = {1: W - timedelta(days=90), 2: W, 3: W + timedelta(minutes=1),
                  4: None, 5: W + timedelta(hours=3), 6: W, 7: W, 8: W}
        held = frozenset({6, 7, 8})
        retired, _ = pg_landing.retired_rows(rows, synced, held, W)
        issues = compare_table(
            PRODUCTS_SPEC, rows, synced, {k: rows[k] for k in held},
            {"last_ok_at": W, "failures_since_ok": 0},
            now=W + timedelta(hours=4), max_samples=100)
        (named,) = [i for i in issues if i.check_name == "mirror_retired_rows"]
        assert sorted(named.sample_ids) == retired == [1, 2, 4]


class _NoStore:
    """DuckDB must not be read on a refusal."""

    def connection(self):
        raise AssertionError("the carry read DuckDB before refusing")


class TestTheCarryRefusesBeforeItReads:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("table", [PRODUCTS, CATEGORIES])
    async def test_a_chain_on_either_table_refuses_on_the_local_answer(
            self, flags, pool, landing_chain, table):
        landing_chain(tables=(table,))
        with pytest.raises(pg_landing.CatalogueCarryRefused, match=table):
            await pg_landing.carry_retired_catalogue(_NoStore(), dry_run=False)
        _never_reached_postgres(pool)

    @pytest.mark.asyncio
    async def test_an_owner_row_no_chain_here_declares_still_refuses(self, flags, pool):
        """An image older than chain 6: the owner row counts as itself."""
        pool.owner_rows = {PRODUCTS: "2026-09-20T08:00:00+00:00"}
        with pytest.raises(pg_landing.CatalogueCarryRefused, match=PRODUCTS):
            await pg_landing.carry_retired_catalogue(_NoStore(), dry_run=False)


class TestTheRoute:
    """`POST /api/mirror/backfill/catalogue`: the expenses route's order."""

    @pytest.fixture
    def carry(self, flags):
        from unittest.mock import AsyncMock

        run = AsyncMock(return_value={PRODUCTS: {"would_carry": 1}})
        flags.setattr("core.pg_landing.carry_retired_catalogue", run)
        flags.setattr("web.routes.api.admin.get_store", AsyncMock(return_value=object()))
        return run

    def _post(self, flags, query=""):
        return _admin_client(flags).post(f"/api/mirror/backfill/catalogue{query}")

    def test_it_is_registered_post_only_and_admin_only(self, flags):
        from tests.routes_helper import iter_endpoints
        from web.main import app
        from web.routes.auth import require_admin

        found = [e for e in iter_endpoints(app)
                 if e.path == "/api/mirror/backfill/catalogue"]
        assert [e.methods for e in found] == [frozenset({"POST"})]
        assert require_admin in found[0].dependencies

    @pytest.mark.parametrize("table", [PRODUCTS, CATEGORIES])
    def test_409_on_the_chain_and_postgres_is_not_asked(
            self, flags, pool, landing_chain, carry, table):
        landing_chain(tables=(table,))
        res = self._post(flags, "?dry_run=false")
        assert res.status_code == 409, res.json()
        assert table in res.json()["detail"]
        carry.assert_not_called()
        _never_reached_postgres(pool)


