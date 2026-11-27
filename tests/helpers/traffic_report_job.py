"""The traffic report's scheduler job, wired for a test: a store, a week of
Gold seeded, the sends captured.
"""
from __future__ import annotations

from datetime import date


def _analytics(scale=1.0, unknown_orders=40, totals_orders=300):
    return {
        "totals": {"orders": totals_orders, "revenue": 700_000 * scale},
        "summary": {
            "paid": {"orders": 120, "revenue": 380_000 * scale},
            "organic": {"orders": 90, "revenue": 190_000 * scale},
            "manager": {"orders": 30, "revenue": 70_000 * scale},
            "pixel_only": {"orders": 20, "revenue": 30_000 * scale},
            "unknown": {"orders": unknown_orders, "revenue": 30_000 * scale},
        },
        "by_platform": {
            "facebook": {"orders": 80, "revenue": 250_000 * scale},
            "google_ads": {"orders": 40, "revenue": 130_000 * scale},
            "instagram": {"orders": 70, "revenue": 150_000 * scale},
            "tiktok": {"orders": 20, "revenue": 60_000 * scale},
            "google_organic": {"orders": 15, "revenue": 40_000 * scale},
            "email": {"orders": 5, "revenue": 10_000 * scale},
        },
    }


class FakeStore:
    """Answers the two calls `build_report` makes."""

    THIS_WEEK = date(2026, 8, 31)

    def __init__(self, analytics=None, campaigns=None):
        self._analytics = analytics or (lambda cur: _analytics(1.0 if cur else 1.25))
        self._campaigns = campaigns or self._default_campaigns

    @staticmethod
    def _default_campaigns(cur):
        return [
            {"campaign": "bf_broad", "platform": "facebook",
             "revenue": 90_000 if cur else 40_000},
            {"campaign": "retarget_7d", "platform": "facebook",
             "revenue": 30_000 if cur else 75_000},
            {"campaign": "search_brand", "platform": "google_ads",
             "revenue": 45_000 if cur else 44_000},
            {"campaign": "noise", "platform": "tiktok",
             "revenue": 1_500 if cur else 500},
        ]

    async def get_traffic_analytics(self, start_date, end_date, sales_type=None,
                                    source_id=None):
        return self._analytics(start_date >= self.THIS_WEEK)

    async def get_traffic_utm_campaigns(self, start_date, end_date, sales_type=None,
                                        limit=50, **kwargs):
        return {"campaigns": self._campaigns(start_date >= self.THIS_WEEK),
                "total": 4}


async def _job_store(tmp_path):
    """The store the job is handed: never connected, its DuckDB connection a
    trap. The report's two reads are `FakeStore`'s (`_wire`), the gate's Gold
    is Postgres's, and the ledger is `ledger`'s dict."""
    from core.store import AnalyticsStore

    store = AnalyticsStore()

    def _no_duckdb(*_a, **_k):
        raise AssertionError("the traffic report reached for DuckDB")

    store.connection = _no_duckdb
    return store


async def _seed_gold(_store, complete=True):
    """Enough Gold for the readiness gate to pass — the job asks the warehouse
    how far it has got, not how much traffic there was.

    Postgres' Gold, roll-up rows, through the pool `core.pg.get_pool` answers
    with (`_warehouse_in_postgres`): the gate reads Postgres alone since step
    14's S14-07 (`core.weekly_report.warehouse_max_date`). The store argument
    is the job's (`_job_store`), and is not written here."""
    from datetime import datetime, timedelta

    from core.pg import get_pool
    from core.scheduler import SCHEDULER_TIMEZONE
    from core.weekly_report import last_complete_week

    today = datetime.now(SCHEDULER_TIMEZONE).date()
    start, end = last_complete_week(today)
    last = end if complete else end - timedelta(days=3)
    pool = await get_pool()
    async with pool.acquire() as conn:
        day = start
        while day <= last:
            await conn.execute(
                "INSERT INTO gold.daily_revenue (date, sales_type, source_id, revenue,"
                " orders_count, unique_customers, new_customers, returning_customers,"
                " returns_count, returns_revenue, avg_order_value)"
                " VALUES ($1, 'retail', NULL, $2, $3, 0, 0, 0, 0, 0, 0)",
                day, 5_000, 2)
            day += timedelta(days=1)
    return start


class TrafficReportJob:
    """The traffic report's job as the scheduler runs it, wired to a store, its sends captured."""

    ADMINS = [111, 222]

    def _wire(self, monkeypatch, store, tmp_path, *, analytics=None, campaigns=None):
        import importlib

        from bot.store_sqlite import SqliteBotStore
        from core.scheduler import BackgroundScheduler

        fake = FakeStore(analytics=analytics, campaigns=campaigns)
        monkeypatch.setattr(store, "get_traffic_analytics",
                            fake.get_traffic_analytics, raising=False)
        monkeypatch.setattr(store, "get_traffic_utm_campaigns",
                            fake.get_traffic_utm_campaigns, raising=False)

        monkeypatch.setattr("bot.store_sqlite.DB_PATH", tmp_path / "prefs.db")
        bot_store = SqliteBotStore()
        bot_store.initialise()
        monkeypatch.setattr("core.bot_store._store", bot_store)

        core_config = importlib.import_module("core.config")
        monkeypatch.setattr(core_config, "ADMIN_USER_IDS", self.ADMINS)

        async def _get_store():
            return store

        monkeypatch.setattr("core.store.get_store", _get_store)
        return BackgroundScheduler()

    @staticmethod
    def _capture(monkeypatch, rich=2, text=2):
        sent = {"rich": [], "text": []}

        async def _rich(html, media=None, chat_ids=None, **kwargs):
            sent["rich"].append({"html": html, "chat_ids": list(chat_ids or [])})
            return rich

        async def _text(body, chat_ids=None, **kwargs):
            sent["text"].append({"body": body, "chat_ids": list(chat_ids or [])})
            return text

        monkeypatch.setattr("core.telegram_alerts.send_rich_message_http", _rich)
        monkeypatch.setattr("core.telegram_alerts.send_admin_message_http", _text)
        return sent
