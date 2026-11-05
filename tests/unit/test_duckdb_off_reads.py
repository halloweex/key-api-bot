"""Stage 5, PR-8: the read path's last reaches for the file under `KS_DUCKDB=off`.

Under `off` (`core/duckdb_switch.py`) the store is handed out unconnected,
and the routers already ask Postgres before the store; what was left on the
read path is four places that let DuckDB answer by routing, not by failure
(design 2.7):

- `/api/summary?source_id=3` — Opencart has no column in Gold, so Postgres is
  not asked and DuckDB's Gold branch answered four zeros through a
  connection. Under `off` the same zeros, without one — and since step 14's
  S14-06, with no DuckDB branch left, a constant (the owner's decision 5).
- `/api/warehouse/status` nested DuckDB's last refresh out of the file. Under
  `off` it was read out of the hourly copy, `app.warehouse_refreshes`, which
  holds the same rows (OD-S5-4) — and since step 14's S14-15 not at all: the
  page answers the Postgres derivation alone.
- the expenses' history gate: a Postgres holding no history of
  `bronze.expenses` let DuckDB answer, uncounted and unrefused even under
  `KS_READ_FALLBACK=off`. Under `KS_DUCKDB=off` it is refused (`no_engine`),
  and since step 14's S14-07 always — its `on` twin, DuckDB answering, went
  with the DuckDB path.
- a read switch left at `duckdb` (or unset) was a routing decision `off`
  never refused. Under `KS_DUCKDB=off` it was refused before the gate
  (`no_address`), until no switch routed a read (S14-07) and S14-16 deleted
  the question with the canary's checks of it.

Each is proved by what it answers and by `duckdb_switch.opened() == {}`. The routes'
sweep under `off` is `tests/unit/test_read_fallback_http.py`
(`TestEveryRoute`, `duckdb_off`), and the stage's list of what is left is
`tests/unit/test_duckdb_off_sweep.py`.
"""
from __future__ import annotations

import contextlib
import logging
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from core import duckdb_switch, read_fallback
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    admin_client,
    fresh_read_fallback,
)

DAY = date.today() - timedelta(days=3)
WINDOW = {"start_date": (DAY - timedelta(days=6)).isoformat(),
          "end_date": DAY.isoformat()}
SUMMARY = "/api/summary"
OPENCART = {**WINDOW, "source_id": 3}
ZEROS = {"totalOrders": 0, "totalRevenue": 0, "avgCheck": 0,
         "totalReturns": 0, "returnsRevenue": 0}


@pytest.fixture
def pg_refuses(monkeypatch, fresh_read_fallback):  # noqa: F811
    """A DSN that nothing reaches and a pool that raises; every read switch
    unset, as `fresh_read_fallback` leaves them."""
    monkeypatch.delenv("KS_READ_FALLBACK", raising=False)
    monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
    monkeypatch.setattr("core.pg.get_pool",
                        AsyncMock(side_effect=OSError("connection refused")))


def _connections_taken(monkeypatch) -> list:
    """Every `DuckDBStore.connection()` the request asks for, under either
    mode — the store's own lock is where DuckDB is reached."""
    from core.duckdb_store import DuckDBStore

    taken, real = [], DuckDBStore.connection

    def counting(self, *args, **kwargs):
        taken.append(1)
        return real(self, *args, **kwargs)

    monkeypatch.setattr(DuckDBStore, "connection", counting)
    return taken


def _cards(body: dict) -> dict:
    return {key: body[key] for key in ZEROS}


# ─── /api/summary for a source with no column in Gold ──────────────────────

class TestTheSummaryForASourceGoldCannotName:
    @pytest.mark.parametrize("gold", [None, "postgres", "duckdb", "junk-s14-06"],
                             ids=["unset", "postgres", "duckdb", "junk"])
    def test_under_off_the_zeros_come_without_a_connection(
        self, gold, admin_client, pg_refuses, duckdb_off, monkeypatch,
    ):
        """The design's proof: 200, zeros, `opened == {}` — whatever
        `KS_READ_GOLD` says, since this answer never asked it, and with a
        Postgres that refuses, which it never asked either. Since step 14's
        S14-06 the zeros are a constant (`_NO_GOLD_SOURCE_TOTALS`, the
        owner's decision 5). Mutation: send source 3 to Gold — a 503 naming
        `dashboard`. (Source 5 shares the constant in the store —
        `tests/unit/test_step12_odna_bronza.py` — but this route's validator
        answers it 400 before anything is read.)"""
        if gold:
            monkeypatch.setenv("KS_READ_GOLD", gold)
        taken = _connections_taken(monkeypatch)
        response = admin_client.get(SUMMARY, params=OPENCART)
        assert response.status_code == 200, response.text
        assert _cards(response.json()) == ZEROS
        assert taken == []
        assert duckdb_switch.opened() == {}
        assert read_fallback.refusals() == {}
        assert not duckdb_off.exists()

    def test_a_source_gold_names_is_not_this_branch(
        self, admin_client, pg_refuses, duckdb_off, monkeypatch,
    ):
        """Only a source Gold is not asked about is answered with zeros:
        Instagram goes to Postgres, and a Postgres that fails is refused,
        not zeroed."""
        response = admin_client.get(SUMMARY, params={**WINDOW, "source_id": 1})
        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "dashboard"
        assert duckdb_switch.opened() == {}

    def test_a_product_filter_beside_it_is_not_this_branch(
        self, admin_client, pg_refuses, duckdb_off, monkeypatch,
    ):
        """A brand beside source 3 reads the order lines, as it always has:
        Postgres is asked, and refusing, refuses the card row."""
        response = admin_client.get(
            SUMMARY, params={**OPENCART, "brand": "BrandA"})
        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "dashboard"


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self.conn


# ─── /api/warehouse/status: the Postgres derivation alone ──────────────────

class TestTheWarehouseStatus:
    def test_under_off_it_reads_no_duckdb_and_no_copy_of_it(
        self, admin_client, duckdb_off, monkeypatch,
    ):
        """`duckdb_frozen` — DuckDB's last refresh, read from the copy under
        `off` — and `cutover` went with step 13's switch in step 14's S14-15.
        Mutation: put either back, and the answer grows a key."""
        from web.routes.api import admin

        monkeypatch.setattr(admin, "_pg_derivation_status",
                            AsyncMock(return_value={"owed": False}))
        monkeypatch.setattr("core.pg.get_pool", AsyncMock(
            side_effect=AssertionError("Postgres asked beyond the derivation block")))
        response = admin_client.get("/api/warehouse/status")
        assert response.status_code == 200, response.text
        assert response.json() == {"writer": "postgres", "postgres": {"owed": False}}
        assert duckdb_switch.opened() == {}
        assert not duckdb_off.exists()


# ─── The expenses' history gate ────────────────────────────────────────────

@pytest.fixture
def no_history(monkeypatch, fresh_read_fallback):  # noqa: F811
    """A Postgres that answers, and answers that `bronze.expenses` holds no
    backfilled history (`backfilled_at` NULL); the gate asks it afresh."""
    from core import pg_expenses_read

    monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
    monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
    monkeypatch.setattr(pg_expenses_read, "_backfilled", False)
    monkeypatch.setattr(pg_expenses_read, "_checked_at", 0.0)
    monkeypatch.setattr(pg_expenses_read, "_check_failed", None)
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=False)
    monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=_Pool(conn)))
    return conn


class TestTheExpensesHistoryGate:
    @pytest.mark.parametrize("fallback", [None, "off"], ids=["unset", "off"])
    def test_under_off_no_history_is_a_503_naming_expenses(
        self, fallback, admin_client, no_history, duckdb_off, monkeypatch, caplog,
    ):
        """NULL in `backfilled_at` is an answer, not a failure — so it was
        served from DuckDB uncounted, even under KS_READ_FALLBACK=off. Under
        KS_DUCKDB=off it is refused, whatever that said (nothing reads it
        since step 14's S14-16). Mutation: drop the `elif` in
        `_expenses_run` — a 503 naming `duckdb`, the file reached."""
        if fallback:
            monkeypatch.setenv("KS_READ_FALLBACK", fallback)
        with caplog.at_level(logging.WARNING):
            response = admin_client.get("/api/expenses/summary", params=WINDOW)
        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "expenses"
        assert no_history.fetchval.await_count == 1, "the gate asked Postgres"
        assert read_fallback.refusals()["expenses"]["count"] >= 1
        assert duckdb_switch.opened() == {}
        assert "no backfilled history" in caplog.text
        assert "serving from DuckDB" not in caplog.text
