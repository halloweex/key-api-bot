"""Stage 5, PR-8: the read path's last reaches for the file under `KS_DUCKDB=off`.

Under `off` (`core/duckdb_switch.py`) the store is handed out unconnected,
and the routers already ask Postgres before the store; what was left on the
read path is four places that let DuckDB answer by routing, not by failure
(design 2.7):

- `/api/summary?source_id=3` — Opencart has no column in Gold, so Postgres is
  not asked and DuckDB's Gold branch answered four zeros through a
  connection. Under `off` the same zeros, without one.
- `/api/warehouse/status` nested DuckDB's last refresh out of the file. Under
  `off` it is read out of the hourly copy, `app.warehouse_refreshes`, which
  holds the same rows (OD-S5-4), or says `off` and the error's class.
- the expenses' history gate: a Postgres holding no history of
  `bronze.expenses` let DuckDB answer, uncounted and unrefused even under
  `KS_READ_FALLBACK=off`. Under `KS_DUCKDB=off` it is refused (`no_engine`).
- a read switch left at `duckdb` (or unset) was a routing decision `off`
  never refused. Under `KS_DUCKDB=off` it is refused before the gate, and a
  failed engine is refused whatever `KS_READ_FALLBACK` says.

Each is proved by what it answers and by `duckdb_switch.opened() == {}`, and
each beside its `on` twin, which answers exactly what it did. The routes'
sweep under `off` is `tests/unit/test_read_fallback_http.py`
(`TestEveryRoute`, `duckdb_off`), and the stage's list of what is left is
`tests/unit/test_duckdb_off_sweep.py`.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

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
    and `KS_READ_FALLBACK` unset, as `fresh_read_fallback` leaves them."""
    monkeypatch.delenv("KS_READ_FALLBACK", raising=False)
    monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
    monkeypatch.setattr("core.pg.get_pool",
                        AsyncMock(side_effect=OSError("connection refused")))
    read_fallback.configure_mode()


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
    @pytest.mark.parametrize("gold", [None, "postgres"], ids=["unset", "postgres"])
    def test_under_off_the_zeros_come_without_a_connection(
        self, gold, admin_client, pg_refuses, duckdb_off, monkeypatch,
    ):
        """The design's proof: 200, zeros, `opened == {}` — whatever
        `KS_READ_GOLD` says, since this answer never asked it. Mutation: drop
        the `off` branch in `get_summary_stats` — a 503 naming `duckdb`."""
        if gold:
            monkeypatch.setenv("KS_READ_GOLD", gold)
        taken = _connections_taken(monkeypatch)
        response = admin_client.get(SUMMARY, params=OPENCART)
        assert response.status_code == 200, response.text
        assert _cards(response.json()) == ZEROS
        assert taken == []
        assert duckdb_switch.opened() == {}
        assert read_fallback.refusals() == {} and read_fallback.counts() == {}
        assert not duckdb_off.exists()

    @pytest.mark.parametrize("gold", [None, "postgres"], ids=["unset", "postgres"])
    def test_under_on_duckdb_answers_the_same_body(
        self, gold, admin_client, pg_refuses, duckdb_off, monkeypatch,
    ):
        """`on` is untouched: DuckDB's Gold branch answers, through its
        connection, and the body is byte for byte the one `off` gives."""
        if gold:
            monkeypatch.setenv("KS_READ_GOLD", gold)
        off = admin_client.get(SUMMARY, params=OPENCART).json()

        # The same process back under on: the store published unconnected
        # under off is forgotten, as a restart would.
        monkeypatch.setenv(duckdb_switch.ENV, "on")
        duckdb_switch.configure_mode()
        monkeypatch.setattr("core.duckdb_store._store_instance", None)
        taken = _connections_taken(monkeypatch)
        on = admin_client.get(SUMMARY, params=OPENCART)
        assert on.status_code == 200, on.text
        assert taken, "under on DuckDB answered this, through its connection"
        assert on.json() == off
        assert read_fallback.counts() == {}, "routing, not a fallback"

    def test_a_source_gold_names_is_not_this_branch(
        self, admin_client, pg_refuses, duckdb_off, monkeypatch,
    ):
        """Only Gold's missing column is answered with zeros: Instagram goes
        to Postgres, and a Postgres that fails is refused, not zeroed."""
        monkeypatch.setenv("KS_READ_GOLD", "postgres")
        response = admin_client.get(SUMMARY, params={**WINDOW, "source_id": 1})
        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "dashboard"
        assert duckdb_switch.opened() == {}


# ─── /api/warehouse/status: DuckDB's last refresh, from its copy ───────────

class _Conn:
    def __init__(self, row, recent):
        self.row, self.recent, self.sql = row, recent, []

    async def fetchrow(self, sql, *args):
        self.sql.append(sql)
        return self.row

    async def fetchval(self, sql, *args):
        self.sql.append(sql)
        return self.recent


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self.conn


AT = datetime(2026, 9, 30, 21, 58, 4, tzinfo=timezone.utc)
ROW = (AT, "dirty_flag", Decimal("1834.50"), 46620, 46620, 2106, 9000, True, True)


@pytest.fixture
def status_page(monkeypatch):
    """The status page with the readiness and the Postgres derivation block
    answered, so what is read is the `duckdb_frozen` part alone."""
    from core import warehouse_cutover
    from web.routes.api import admin

    monkeypatch.setattr(warehouse_cutover, "readiness",
                        AsyncMock(return_value={"mode": "postgres"}))
    monkeypatch.setattr(admin, "_pg_derivation_status",
                        AsyncMock(return_value={"mode": "own"}))
    monkeypatch.setattr(warehouse_cutover, "_mode", warehouse_cutover.POSTGRES)


class TestTheWarehouseStatus:
    def test_under_off_duckdbs_last_refresh_is_read_from_its_copy(
        self, admin_client, status_page, duckdb_off, monkeypatch,
    ):
        """Mutation: drop the `off` branch of `_duckdb_refresh_status` — the
        page is a 503 naming `duckdb`."""
        conn = _Conn(ROW, 0)
        monkeypatch.setattr("core.pg.get_pool", AsyncMock(return_value=_Pool(conn)))
        response = admin_client.get("/api/warehouse/status")
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["writer"] == "postgres" and body["postgres"] == {"mode": "own"}
        assert body["duckdb_frozen"] == {
            "status": "off", "source": "app.warehouse_refreshes",
            "last_refresh": AT.isoformat(), "last_trigger": "dirty_flag",
            "last_duration_ms": 1834.5, "bronze_orders": 46620,
            "silver_rows": 46620, "gold_revenue_rows": 2106,
            "gold_products_rows": 9000, "checksum_match": True,
            "validation_passed": True, "recent_refreshes": 0}
        assert all("app.warehouse_refreshes" in sql for sql in conn.sql)
        assert duckdb_switch.opened() == {}
        assert not duckdb_off.exists()

    def test_under_off_an_unreadable_copy_costs_the_block_not_the_page(
        self, admin_client, status_page, duckdb_off, monkeypatch, caplog,
    ):
        """The class alone, as every fact on this page: a driver's text names
        the database user, host and port. Whole in the log."""
        monkeypatch.setattr("core.pg.get_pool", AsyncMock(
            side_effect=OSError("password authentication failed for user ks_app")))
        with caplog.at_level(logging.ERROR, logger="web.routes.api.admin"):
            response = admin_client.get("/api/warehouse/status")
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["duckdb_frozen"] == {"status": "off", "error": "OSError"}
        assert "ks_app" not in response.text
        assert "ks_app" in caplog.text
        assert body["cutover"] == {"mode": "postgres"}
        assert duckdb_switch.opened() == {}

    def test_under_off_an_empty_copy_reads_as_duckdbs_empty_table(
        self, admin_client, status_page, duckdb_off, monkeypatch,
    ):
        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(return_value=_Pool(_Conn(None, None))))
        frozen = admin_client.get("/api/warehouse/status").json()["duckdb_frozen"]
        assert frozen["status"] == "off" and frozen["last_refresh"] is None
        assert frozen["recent_refreshes"] == 0 and frozen["bronze_orders"] == 0

    def test_under_off_without_the_switch_the_copy_is_the_top_level_answer(
        self, admin_client, status_page, duckdb_off, monkeypatch,
    ):
        """DuckDB still named the writer — a state stage 5's verdict holds
        back to `on` — and the page reads the copy all the same rather than
        reaching for the file."""
        from core import warehouse_cutover

        monkeypatch.setattr(warehouse_cutover, "_mode", warehouse_cutover.DUCKDB)
        monkeypatch.setattr("core.pg.get_pool",
                            AsyncMock(return_value=_Pool(_Conn(ROW, 2))))
        body = admin_client.get("/api/warehouse/status").json()
        assert body["status"] == "off" and body["last_trigger"] == "dirty_flag"
        assert body["recent_refreshes"] == 2 and "duckdb_frozen" not in body
        assert duckdb_switch.opened() == {}

    def test_under_on_the_store_answers_and_postgres_is_not_asked(
        self, admin_client, status_page, monkeypatch,
    ):
        from web.routes.api import admin

        store = MagicMock()
        store.get_warehouse_status = AsyncMock(return_value={"last_refresh": "x"})
        pool = AsyncMock(side_effect=AssertionError("Postgres asked under on"))
        monkeypatch.setattr("core.pg.get_pool", pool)
        monkeypatch.setattr(admin, "get_store", AsyncMock(return_value=store))
        body = admin_client.get("/api/warehouse/status").json()
        assert body["duckdb_frozen"] == {"last_refresh": "x"}
        store.get_warehouse_status.assert_awaited_once()
        pool.assert_not_awaited()

    @pytest.mark.parametrize("seeded", [False, True], ids=["empty", "one_refresh"])
    def test_the_copy_builds_duckdbs_own_answer(self, seeded):
        """`_refresh_fields` is DuckDB's dict, built twice — so it is held to
        DuckDB's own: the same nine columns read out of a real store give the
        same answer, key for key and value for value."""
        from core.duckdb_store import DuckDBStore
        from web.routes.api.admin import _refresh_fields

        async def run():
            store = DuckDBStore()
            await store.connect()
            try:
                async with store.connection() as conn:
                    if seeded:
                        conn.execute(
                            "INSERT INTO warehouse_refreshes (refreshed_at, trigger, "
                            "duration_ms, bronze_orders, silver_rows, "
                            "gold_revenue_rows, gold_products_rows, checksum_match, "
                            "validation_passed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            list(ROW))
                    last = conn.execute(
                        "SELECT refreshed_at, trigger, duration_ms, bronze_orders, "
                        "silver_rows, gold_revenue_rows, gold_products_rows, "
                        "checksum_match, validation_passed FROM warehouse_refreshes "
                        "ORDER BY id DESC LIMIT 1").fetchone()
                    recent = conn.execute(
                        "SELECT COUNT(*) FROM warehouse_refreshes WHERE refreshed_at "
                        "> CURRENT_TIMESTAMP - INTERVAL '1 hour'").fetchone()[0]
                return await store.get_warehouse_status(), _refresh_fields(last, recent)
            finally:
                await store.close()

        duckdbs, built = asyncio.run(run())
        assert built == duckdbs


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
        KS_DUCKDB=off it is refused, whatever that says. Mutation: drop the
        `elif` in `_expenses_run` — a 503 naming `duckdb`, the file reached."""
        if fallback:
            monkeypatch.setenv("KS_READ_FALLBACK", fallback)
        read_fallback.configure_mode()
        with caplog.at_level(logging.WARNING):
            response = admin_client.get("/api/expenses/summary", params=WINDOW)
        assert response.status_code == 503, response.text
        assert response.json()["surface"] == "expenses"
        assert no_history.fetchval.await_count == 1, "the gate asked Postgres"
        assert read_fallback.refusals()["expenses"]["count"] >= 1
        assert read_fallback.counts() == {}
        assert duckdb_switch.opened() == {}
        assert "no backfilled history" in caplog.text
        assert "serving from DuckDB" not in caplog.text

    def test_under_on_no_history_is_still_answered_by_duckdb(
        self, admin_client, no_history, monkeypatch, caplog,
    ):
        """`on` is untouched — the gap step 13's `expenses_backfilled`
        precondition closes, served from DuckDB, nothing counted or refused
        even under KS_READ_FALLBACK=off."""
        monkeypatch.setenv("KS_READ_FALLBACK", "off")
        read_fallback.configure_mode()
        with caplog.at_level(logging.WARNING):
            response = admin_client.get("/api/expenses/summary", params=WINDOW)
        assert response.status_code == 200, response.text
        assert read_fallback.refusals() == {} and read_fallback.counts() == {}
        assert "serving from DuckDB" in caplog.text


# ─── read_fallback itself ──────────────────────────────────────────────────

class TestTheRule:
    def test_under_on_refusing_is_the_mode(self, fresh_read_fallback, monkeypatch):  # noqa: F811
        for value, refusing in ((None, False), ("duckdb", False), ("off", True)):
            if value is None:
                monkeypatch.delenv("KS_READ_FALLBACK", raising=False)
            else:
                monkeypatch.setenv("KS_READ_FALLBACK", value)
            read_fallback.configure_mode()
            assert read_fallback.refusing() is refusing, value

    def test_under_off_it_refuses_whatever_the_mode_says(
        self, fresh_read_fallback, duckdb_off, monkeypatch,  # noqa: F811
    ):
        monkeypatch.setenv("KS_READ_FALLBACK", "duckdb")
        read_fallback.configure_mode()
        assert read_fallback.mode() == "duckdb", "the mode is what was configured"
        assert read_fallback.refusing() is True
        with pytest.raises(read_fallback.ReadUnavailable):
            read_fallback.fall_back("dashboard", OSError("down"))
        assert read_fallback.counts() == {}
        assert read_fallback.refusals()["dashboard"]["count"] == 1

    @pytest.mark.parametrize("value", [None, "duckdb", " DuckDB "],
                             ids=["unset", "duckdb", "spaced"])
    def test_under_off_a_switch_left_at_duckdb_is_refused_by_name(
        self, value, fresh_read_fallback, duckdb_off, monkeypatch, caplog,  # noqa: F811
    ):
        from core import pg_traffic_read

        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
        if value is None:
            monkeypatch.delenv(pg_traffic_read.ENV, raising=False)
        else:
            monkeypatch.setenv(pg_traffic_read.ENV, value)
        read_fallback.configure_mode()
        with caplog.at_level(logging.ERROR, logger="core.read_fallback"), \
                pytest.raises(read_fallback.ReadUnavailable) as raised:
            read_fallback.no_address("traffic", pg_traffic_read)
        assert raised.value.surface == "traffic"
        assert "KS_READ_TRAFFIC=duckdb while KS_DUCKDB=off" in caplog.text
        assert "falling back to DuckDB" not in caplog.text

    def test_under_off_a_switch_on_its_engine_passes(
        self, fresh_read_fallback, duckdb_off, monkeypatch,  # noqa: F811
    ):
        from core import pg_traffic_read

        monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
        monkeypatch.setenv(pg_traffic_read.ENV, "postgres")
        read_fallback.configure_mode()
        read_fallback.no_address("traffic", pg_traffic_read)
        assert read_fallback.refusals() == {}

    @pytest.mark.parametrize("fallback", ["duckdb", "off"])
    def test_under_on_a_switch_left_at_duckdb_is_routing(
        self, fallback, fresh_read_fallback, monkeypatch,  # noqa: F811
    ):
        from core import pg_traffic_read

        monkeypatch.setenv("KS_READ_FALLBACK", fallback)
        monkeypatch.setenv(pg_traffic_read.ENV, "duckdb")
        read_fallback.configure_mode()
        read_fallback.no_address("traffic", pg_traffic_read)
        assert read_fallback.refusals() == {}
