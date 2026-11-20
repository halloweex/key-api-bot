"""The weekly report's scheduler job, wired for a test: a store on the test
Postgres, a week of Gold seeded, the sends captured.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from core.store import AnalyticsStore
from core.weekly_report import last_complete_week


async def _store(tmp_path: Path) -> AnalyticsStore:
    """The store the job is handed: never connected, its DuckDB connection a
    trap — the report reads Postgres and the ledger is `ledger`'s dict."""
    s = AnalyticsStore()

    def _no_duckdb(*_a, **_k):
        raise AssertionError("the weekly report reached for DuckDB")

    s.connection = _no_duckdb
    return s


async def _gold_day(conn, day: date, revenue: float, orders: int, sales_type="retail"):
    """One roll-up cell of Postgres' Gold (`source_id IS NULL`)."""
    await conn.execute(
        "INSERT INTO gold.daily_revenue (date, sales_type, source_id, revenue,"
        " orders_count, unique_customers, new_customers, returning_customers,"
        " returns_count, returns_revenue, avg_order_value)"
        " VALUES ($1, $2, NULL, $3, $4, 0, 0, 0, 0, 0, 0)"
        " ON CONFLICT ON CONSTRAINT gold_daily_revenue_cell DO UPDATE"
        " SET revenue = EXCLUDED.revenue, orders_count = EXCLUDED.orders_count",
        day, sales_type, revenue, orders)


async def _silver_order(conn, oid: int, day: date, total: float, is_new: bool,
                        sales_type="retail", is_return=False):
    await conn.execute(
        "INSERT INTO silver.orders (id, source_id, status_id, grand_total,"
        " ordered_at, order_date, is_return, sales_type, is_active_source,"
        " source_name, is_new_customer)"
        " VALUES ($1, 1, 1, $2, $3, $4, $5, $6, TRUE, 'Instagram', $7)"
        " ON CONFLICT (id) DO UPDATE SET grand_total = EXCLUDED.grand_total,"
        " order_date = EXCLUDED.order_date, is_return = EXCLUDED.is_return,"
        " sales_type = EXCLUDED.sales_type, is_new_customer = EXCLUDED.is_new_customer",
        oid, total, datetime(day.year, day.month, day.day, 9, tzinfo=timezone.utc),
        day, is_return, sales_type, is_new)


class WeeklyReportJob:
    """The weekly report's job as the scheduler runs it: a week seeded, the job wired to a store and its sends captured."""

    async def _seed(self, pool, *, complete: bool):
        """A full baseline plus the reported week, from the real clock's view,
        in Postgres' Gold and Silver."""
        from core.scheduler import SCHEDULER_TIMEZONE

        today = datetime.now(SCHEDULER_TIMEZONE).date()
        start, end = last_complete_week(today)
        last_day = end if complete else end - timedelta(days=3)

        async with pool.acquire() as conn:
            for n in range(70):
                day = start - timedelta(days=70 - n)
                await _gold_day(conn, day, 10_000 + (n % 5) * 500, 4)
            day = start
            while day <= last_day:
                await _gold_day(conn, day, 5_000, 2)
                await _silver_order(conn, 900 + day.toordinal() % 1000, day, 2_500,
                                    is_new=True)
                day += timedelta(days=1)
        return start

    ADMINS = [111, 222]

    def _wire(self, monkeypatch, store, sent, tmp_path=None, languages=None,
              approved=None):
        """Point the job at this store, these admins, and a capturing transport.

        `languages` and `approved` are written through the bot store port into
        a throwaway SQLite adapter installed as the active store — without it
        the job would read the developer's real bot state and the test would
        pass or fail by whatever happens to be stored there.
        """
        import importlib

        from bot.store_sqlite import SqliteBotStore
        from core.scheduler import BackgroundScheduler

        # `core/__init__.py` re-exports the AppConfig instance as `config`, so
        # a dotted monkeypatch target resolves `core.config` to that object
        # rather than to the module. Only an explicit lookup gets the module.
        core_config = importlib.import_module("core.config")

        async def _get_store():
            return store

        async def _send_text(text, **kwargs):
            sent.append(text)
            return len(kwargs.get("chat_ids") or self.ADMINS)

        monkeypatch.setattr(
            "bot.store_sqlite.DB_PATH",
            (tmp_path or store.db_path.parent) / "prefs.db",
        )
        bot_store = SqliteBotStore()
        bot_store.initialise()
        monkeypatch.setattr("core.bot_store._store", bot_store)
        for uid in (approved or []):
            bot_store.access.request(uid, f"user{uid}", "First", "Last")
            bot_store.access.approve(uid, self.ADMINS[0])
        for uid, lang in (languages or {}).items():
            bot_store.preferences.set(uid, "language", lang)

        monkeypatch.setattr(core_config, "ADMIN_USER_IDS", self.ADMINS)
        monkeypatch.setattr("core.store.get_store", _get_store)
        monkeypatch.setattr("core.telegram_alerts.send_admin_message_http", _send_text)
        return BackgroundScheduler()
