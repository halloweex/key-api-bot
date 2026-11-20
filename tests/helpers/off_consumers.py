"""The consumers of a read refusal that are not HTTP requests, wired for a
test: the reports' jobs, the assistant's tools, a writer recorded.
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock


async def _duck(tmp_path, name="consumers.duckdb"):
    from core.store import AnalyticsStore

    store = AnalyticsStore()
    return store


def _ledger_answers(monkeypatch):
    """The report ledgers as production holds them: in Postgres (chains 11a
    and 11b, a constant since step 14's S14-00), asked before any read of the
    report's own. Here the week is unsent and a write is recorded, so the
    refusal under test is the report's read and "no ledger row" is "never
    marked"."""
    from core import report_ledger

    monkeypatch.setattr(report_ledger, "already_sent", AsyncMock(return_value=False))
    mark = AsyncMock(side_effect=AssertionError("the ledger was written"))
    monkeypatch.setattr(report_ledger, "mark_sent", mark)
    return mark


def _serve(monkeypatch, store) -> None:
    async def _get_store():
        return store

    monkeypatch.setattr("core.store.get_store", _get_store)


def _writer_recorded(monkeypatch) -> list:
    """Chain 7b-3's store of the goal tables, recorded: production's one
    writer of them (a constant since S14-00)."""
    from core import pg_forecast_write

    written = []

    async def _persist(*args, **kwargs):
        written.append(args)

    monkeypatch.setattr(pg_forecast_write, "persist_goal_tables", _persist)
    return written


class TrafficReportWiring:
    """The traffic report's job wired over a refusing store."""

    ADMINS = [111, 222]

    def _wire(self, monkeypatch, store, tmp_path):
        import importlib

        from bot.store_sqlite import SqliteBotStore
        from core.scheduler import BackgroundScheduler

        monkeypatch.delenv("KS_TRAFFIC_REPORT_FIRST_WEEK", raising=False)
        monkeypatch.delenv("KS_TRAFFIC_REPORT_RECIPIENTS", raising=False)
        monkeypatch.setattr("bot.store_sqlite.DB_PATH", tmp_path / "prefs.db")
        bot_store = SqliteBotStore()
        bot_store.initialise()
        monkeypatch.setattr("core.bot_store._store", bot_store)
        monkeypatch.setattr(importlib.import_module("core.config"),
                            "ADMIN_USER_IDS", self.ADMINS)
        _serve(monkeypatch, store)

        sent: list = []

        async def _rich(html, media=None, chat_ids=None, **kwargs):
            sent.append(html)
            return len(chat_ids or [])

        async def _text(body, chat_ids=None, **kwargs):
            sent.append(body)
            return len(chat_ids or [])

        monkeypatch.setattr("core.telegram_alerts.send_rich_message_http", _rich)
        monkeypatch.setattr("core.telegram_alerts.send_admin_message_http", _text)
        return BackgroundScheduler(), sent

    @staticmethod
    def _the_warehouse_is_ready(monkeypatch):
        """The readiness gate answering, past the reported week: it reads
        Postgres' Gold since step 14's S14-07, which the raising pool would
        stop first — and its own failure is no refusal."""
        from core.scheduler import SCHEDULER_TIMEZONE
        from core.weekly_report import last_complete_week

        start, end = last_complete_week(datetime.now(SCHEDULER_TIMEZONE).date())
        monkeypatch.setattr("core.weekly_report.warehouse_max_date",
                            AsyncMock(return_value=end))
        return start


class AssistantTools:
    """The assistant's tools served from a given store."""

    @staticmethod
    def _serve_tools(monkeypatch, store):
        async def _get_store():
            return store

        monkeypatch.setattr("core.chat_tools.get_store", _get_store)
