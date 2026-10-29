"""`scripts/weekly_report_preview.py` reads what the weekly jobs read: Postgres.

Until step 14's S14-03 the preview opened a DuckDB file of its own — usually a
backup — with `duckdb.connect(..., read_only=True)` and handed `build_report`
a connection to it. It now hands the jobs' own readers a store that refuses
to connect, and asks the two variables those readers route by before it
reads anything. Each test names the mutation it catches.
"""
from __future__ import annotations

import asyncio
import importlib.util
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import core.weekly_report as weekly
from core.sql_dialect import POSTGRES, channel_totals_items

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "weekly_report_preview_script", ROOT / "scripts" / "weekly_report_preview.py")
preview = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(preview)

FAKE_DSN = "postgresql://nobody@127.0.0.1:1/none"


def _no_switches(monkeypatch):
    for name in ("KS_READ_WEEKLY", "KS_READ_TRAFFIC", "KS_PG_DSN"):
        monkeypatch.delenv(name, raising=False)


def _forbid(monkeypatch, *targets):
    """Each target, called, fails the test: the preview must not get there."""
    for owner, name in targets:
        monkeypatch.setattr(owner, name, AsyncMock(
            side_effect=AssertionError(f"{name} reached")))


class TestItRefusesBeforeReading:
    @pytest.mark.parametrize("env, says", [
        ({"KS_PG_DSN": FAKE_DSN}, "KS_READ_WEEKLY is not postgres"),
        ({"KS_PG_DSN": FAKE_DSN, "KS_READ_WEEKLY": "duckdb"}, "KS_READ_WEEKLY is not postgres"),
        ({"KS_READ_WEEKLY": "postgres"}, "KS_PG_DSN is not set"),
        ({"KS_PG_DSN": FAKE_DSN, "KS_READ_WEEKLY": "postgress"}, "unknown engine"),
    ])
    def test_the_sales_report_without_its_switch_or_address(
            self, monkeypatch, capsys, env, says):
        """Mutation: drop the up-front `postgres_unready` — the read starts and
        either asks a store that refuses or a Postgres that is not there."""
        _no_switches(monkeypatch)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        _forbid(monkeypatch, (preview, "build_report"), (preview, "send"))

        assert preview.main(["--chat-id", "1"]) == 2
        err = capsys.readouterr().err
        assert says in err and "--fixture" in err, err

    def test_the_traffic_report_asks_its_own_switch(self, monkeypatch, capsys):
        """The weekly switch set is not enough: the traffic report's reads
        route by KS_READ_TRAFFIC. Mutation: ask KS_READ_WEEKLY for both."""
        _no_switches(monkeypatch)
        monkeypatch.setenv("KS_PG_DSN", FAKE_DSN)
        monkeypatch.setenv("KS_READ_WEEKLY", "postgres")
        _forbid(monkeypatch, (preview, "send_traffic"))

        assert preview.main(["--chat-id", "1", "--report", "traffic"]) == 2
        err = capsys.readouterr().err
        assert "KS_READ_TRAFFIC is not postgres" in err and "--fixture" not in err, err


class TestTheStoreItHandsTheReaders:
    def test_it_refuses_to_connect_and_creates_no_file(self):
        """Mutation: hand the readers a plain `DuckDBStore()` — its lazy
        connect opens (and creates) the file."""
        store = preview._store()
        path = Path(store.db_path)
        assert not path.exists()

        async def ask():
            async with store.connection():
                pass

        with pytest.raises(preview.PostgresOnly):
            asyncio.run(ask())
        assert not path.exists() and not path.parent.joinpath(path.name + ".wal").exists()

    def test_a_reader_routed_to_duckdb_stops_the_preview(self, monkeypatch):
        """Even past the up-front check — a switch flipped between the check
        and the read — the sales readers' DuckDB branch reaches the store and
        stops there."""
        from core import duckdb_switch

        monkeypatch.setenv("KS_READ_WEEKLY", "duckdb")
        monkeypatch.setattr(duckdb_switch, "_mode", duckdb_switch.ON)
        with pytest.raises(preview.PostgresOnly):
            asyncio.run(preview.live_report("retail", date(2026, 9, 7)))


class TestItReadsWhatTheJobReads:
    def test_the_sales_report_is_the_jobs_build_report(self):
        """Mutation: a reader of its own in the script."""
        assert preview.build_report is weekly.build_report

    def test_every_read_goes_to_postgres_through_the_jobs_fetchers(self, monkeypatch):
        """The live preview builds the week through `core.weekly_report`'s
        `fetch_*`, each statement rendered for Postgres and handed to
        `pg_weekly_read.fetch`. Mutation: read through anything else — a
        DuckDB body reaches the store, which refuses."""
        _no_switches(monkeypatch)
        monkeypatch.setenv("KS_PG_DSN", FAKE_DSN)
        monkeypatch.setenv("KS_READ_WEEKLY", "postgres")
        channels = len(channel_totals_items(POSTGRES))
        seen = []

        async def fetch(sql, params=()):
            seen.append(sql)
            if "FILTER (WHERE source_id" in sql:
                return [tuple([100.0, 1] * channels)]
            if "is_new_customer" in sql:
                return [(1, 3)]
            if "MIN(date)" in sql:
                return [(None,)]
            if "FULL OUTER JOIN" in sql:
                return [("Пінка", 300.0, 100.0)]
            if "SELECT date, revenue, orders_count" in sql:
                return [(date(2026, 8, 31), 400.0, 2)]
            if "SUM(orders_count)" in sql:
                return [(1000.0, 4)]
            raise AssertionError(f"an unexpected read: {sql}")

        from core import pg_weekly_read

        monkeypatch.setattr(pg_weekly_read, "fetch", fetch)
        report = asyncio.run(preview.live_report("retail", date(2026, 9, 7)))

        assert (report.start, report.end) == (date(2026, 8, 31), date(2026, 9, 6))
        assert report.current.revenue == 1000.0 and report.current.orders == 4
        assert report.current.new_customer_orders == 1
        assert report.movers and report.movers[0].name == "Пінка"
        assert report.days[0].revenue == 400.0 and len(report.days) == 7
        assert report.channels
        assert seen and all("gold_daily_revenue" not in sql and "silver_orders" not in sql
                            for sql in seen), seen
        assert any("gold.daily_revenue" in sql for sql in seen)

    def test_the_fixture_reads_nothing(self, monkeypatch):
        """`--fixture` needs no database and no switch. Mutation: ask
        `postgres_unready` (or a reader) before the fixture."""
        _no_switches(monkeypatch)
        _forbid(monkeypatch, (preview, "build_report"))
        monkeypatch.setattr(preview, "postgres_unready",
                            lambda report: pytest.fail("asked for Postgres"))
        send = AsyncMock()
        monkeypatch.setattr(preview, "send", send)

        assert preview.main(["--chat-id", "7", "--fixture", "--form", "text"]) == 0
        report, chat_id, _lang, forms, _url = send.await_args.args
        assert chat_id == 7 and forms == ["text"]
        assert (report.current.orders, round(report.current.revenue)) == (336, 805788)


class TestTheTrafficReport:
    def test_it_builds_the_week_on_the_refusing_store_and_sends_it(self, monkeypatch):
        """`send_traffic` hands the traffic report's builder the store that
        refuses to connect, and sends what it built to the one chat.

        Mutation: build the week on a plain `DuckDBStore()` that connects —
        the shape this script had before S14-03 — which opens (and creates)
        the file. Every connect of the base class fails the test here, so the
        mutated run creates nothing either; the store the builder was handed
        must refuse a connection the way `_store()` does.
        """
        from core import duckdb_store, telegram_alerts, traffic_report

        handed = {}

        async def build(store, today, sales_type=traffic_report.TRAFFIC_SALES_TYPE):
            handed["store"], handed["today"] = store, today
            return traffic_report.TrafficReport(
                start=date(2026, 8, 31), end=date(2026, 9, 6), sales_type="retail",
                revenue=1000.0, orders=4, previous_revenue=800.0, previous_orders=3)

        monkeypatch.setattr(traffic_report, "build_report", build)
        monkeypatch.setattr(duckdb_store.DuckDBStore, "connect", AsyncMock(
            side_effect=AssertionError("a DuckDB store connected")))
        sent = AsyncMock(return_value=1)
        monkeypatch.setattr(telegram_alerts, "send_rich_message_http", sent)

        asyncio.run(preview.send_traffic(7, "en", None, today=date(2026, 9, 7)))

        assert handed["today"] == date(2026, 9, 7)
        store = handed["store"]
        path = Path(store.db_path)

        async def ask():
            async with store.connection():
                pass

        with pytest.raises(preview.PostgresOnly):
            asyncio.run(ask())
        assert not path.exists()
        assert sent.await_count == 1
        assert sent.await_args.kwargs["chat_ids"] == [7]
        assert "31.08" in sent.await_args.args[0], sent.await_args.args[0]

    def test_main_sends_the_traffic_week_once_its_switch_is_set(self, monkeypatch):
        """`--report traffic` past the up-front check reaches `send_traffic`
        with the chat it was given. Mutation: return before sending, or send
        the sales report instead."""
        _no_switches(monkeypatch)
        monkeypatch.setenv("KS_PG_DSN", FAKE_DSN)
        monkeypatch.setenv("KS_READ_TRAFFIC", "postgres")
        _forbid(monkeypatch, (preview, "build_report"), (preview, "send"))
        traffic = AsyncMock()
        monkeypatch.setattr(preview, "send_traffic", traffic)

        assert preview.main(["--chat-id", "9", "--report", "traffic", "--lang", "uk"]) == 0
        chat_id, lang, _url = traffic.await_args.args
        assert (chat_id, lang) == (9, "uk")


def test_the_script_names_no_database_file():
    """No `--db`, no DUCKDB_PATH: the preview has no file to be pointed at."""
    text = (ROOT / "scripts" / "weekly_report_preview.py").read_text(encoding="utf-8")
    assert '"--db"' not in text and "DUCKDB_PATH" not in text
    assert "duckdb" not in vars(preview)
