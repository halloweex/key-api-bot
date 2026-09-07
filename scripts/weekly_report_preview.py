"""Send a preview of the weekly report to one chat, in one language.

The report's three forms — rich (Bot API 10.1+), the card with the report as
its caption, and plain text — are rendered by the same code the scheduled
job uses, so what arrives is what the job would send. This exists for
looking at a layout on a real phone before a Monday, not for delivering the
report; it writes nothing to the send ledger.

Real numbers come from the DuckDB file when it holds the week; `--fixture`
renders the 31.08–06.09.2026 retail week from values that reproduce the
message actually delivered on 2026-09-07, for a host whose copy is stale.

    PYTHONPATH=. KS_ALERTS_DISABLED=0 KS_INSTANCE=dev-laptop \\
        python scripts/weekly_report_preview.py --chat-id <id> --lang ru --form rich,rich-nocard

The kill switch is honoured: without `KS_ALERTS_DISABLED=0` in the
environment (a dev `.env` sets it to 1) every send is suppressed and logged.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime

from core.weekly_report import (
    ChannelTotals,
    DayTotals,
    ProductMove,
    WeeklyReport,
    WeekTotals,
    build_report,
    format_report,
    format_report_rich,
)
from core.weekly_report_image import (
    render_channels_chart,
    render_days_chart,
    render_waterfall,
    render_weekly_card,
)

# rich: card + charts; rich-nocard: charts only; rich-text: no pictures at
# all; card / text: the two forms the scheduled job sends today.
FORMS = ("rich", "rich-nocard", "rich-text", "card", "text")


# gold.daily_revenue, retail, per (day, source): what Postgres held on
# 2026-09-07. Source 1 Instagram, 2 Telegram, 4 Shopify. The roll-ups add
# up to the delivered message to the hryvnia: ₴ 805 788 / 336 orders, and
# ₴ 1 121 403 / 433 the week before.
_FIXTURE_ROWS = [
    ("2026-08-24", 1, 42223.40, 12), ("2026-08-24", 4, 57275.50, 19),
    ("2026-08-25", 1, 35512.00, 9), ("2026-08-25", 2, 3629.00, 1), ("2026-08-25", 4, 53341.60, 24),
    ("2026-08-26", 1, 68658.00, 18), ("2026-08-26", 4, 140810.48, 55),
    ("2026-08-27", 1, 84691.00, 24), ("2026-08-27", 2, 2445.00, 1), ("2026-08-27", 4, 124132.44, 50),
    ("2026-08-28", 1, 55495.00, 22), ("2026-08-28", 4, 117535.43, 55),
    ("2026-08-29", 1, 65471.00, 22), ("2026-08-29", 2, 1646.00, 1), ("2026-08-29", 4, 165483.73, 70),
    ("2026-08-30", 1, 37670.00, 13), ("2026-08-30", 2, 2419.00, 1), ("2026-08-30", 4, 62964.45, 36),
    ("2026-08-31", 1, 45885.00, 20), ("2026-08-31", 2, 3852.00, 1), ("2026-08-31", 4, 74292.40, 37),
    ("2026-09-01", 1, 38303.00, 15), ("2026-09-01", 4, 47833.80, 24),
    ("2026-09-02", 1, 52893.00, 19), ("2026-09-02", 2, 11383.00, 4), ("2026-09-02", 4, 48559.90, 27),
    ("2026-09-03", 1, 33370.00, 10), ("2026-09-03", 4, 75269.26, 26),
    ("2026-09-04", 1, 73444.00, 25), ("2026-09-04", 4, 74909.33, 35),
    ("2026-09-05", 1, 35888.00, 14), ("2026-09-05", 4, 51937.70, 25),
    ("2026-09-06", 1, 38817.00, 18), ("2026-09-06", 2, 5065.00, 2), ("2026-09-06", 4, 94085.12, 34),
]
_SOURCE_NAMES = {1: "Instagram", 2: "Telegram", 4: "Shopify"}


def _days(rows, start: date, end: date) -> list[DayTotals]:
    out = []
    day = start
    while day <= end:
        mine = [r for r in rows if r[0] == day.isoformat()]
        out.append(DayTotals(day, sum(r[2] for r in mine), sum(r[3] for r in mine)))
        day = date.fromordinal(day.toordinal() + 1)
    return out


def _channels(rows, cur: tuple[str, str], prev: tuple[str, str]) -> list[ChannelTotals]:
    def in_window(r, w):
        return w[0] <= r[0] <= w[1]
    out = []
    for sid, name in _SOURCE_NAMES.items():
        now = [r for r in rows if r[1] == sid and in_window(r, cur)]
        before = [r for r in rows if r[1] == sid and in_window(r, prev)]
        out.append(ChannelTotals(name, sum(r[2] for r in now), sum(r[3] for r in now),
                                 sum(r[2] for r in before)))
    out.sort(key=lambda c: c.revenue, reverse=True)
    return out


def fixture_report() -> WeeklyReport:
    """The 31.08–06.09.2026 retail week, as delivered."""
    cur_w, prev_w = ("2026-08-31", "2026-09-06"), ("2026-08-24", "2026-08-30")
    days = _days(_FIXTURE_ROWS, date(2026, 8, 31), date(2026, 9, 6))
    prev_days = _days(_FIXTURE_ROWS, date(2026, 8, 24), date(2026, 8, 30))
    cur = WeekTotals(revenue=sum(d.revenue for d in days), orders=sum(d.orders for d in days),
                     new_customer_orders=131, repeat_orders=205)
    prev = WeekTotals(revenue=sum(d.revenue for d in prev_days),
                      orders=sum(d.orders for d in prev_days),
                      new_customer_orders=136, repeat_orders=297)
    year_ago = WeekTotals(revenue=cur.revenue / 1.186, orders=300,
                          new_customer_orders=120, repeat_orders=180)
    return WeeklyReport(
        start=date(2026, 8, 31), end=date(2026, 9, 6), sales_type="retail",
        current=cur, previous=prev, year_ago=year_ago,
        baseline_mean=cur.revenue / (1 - 0.081), baseline_sd=194_000,
        baseline_weeks=12,
        movers=[
            ProductMove("LALARECIPE Освітлювальна пінка 3-в-1 для обличчя", 1_000, 39_955),
            ProductMove("LALARECIPE Ліфтинг-крем для зони навколо очей", 500, 28_961),
            ProductMove("NEOGEN Зволожувальна есенція з гідролатом", 20_000, 1_184),
        ],
        product_move_total=-320_000,
        days=days, previous_days=prev_days,
        channels=_channels(_FIXTURE_ROWS, cur_w, prev_w),
    )


def live_report(db_path: str, sales_type: str) -> WeeklyReport:
    import duckdb

    conn = duckdb.connect(db_path, read_only=True)
    try:
        return build_report(conn, datetime.now().date(), sales_type)
    finally:
        conn.close()


async def send(report: WeeklyReport, chat_id: int, lang: str,
               forms: list[str], dashboard_url: str | None) -> None:
    from core.telegram_alerts import (
        send_admin_message_http,
        send_admin_photo_http,
        send_rich_message_http,
    )

    card = render_weekly_card(report, lang)
    if card is None:
        print("card: not rendered (no DejaVu on this host)")
    pictures = {
        "card": card,
        "days": render_days_chart(report, lang),
        "why": render_waterfall(report, lang),
        "channels": render_channels_chart(report, lang),
    }
    pictures = {k: v for k, v in pictures.items() if v}

    for form in forms:
        if form in ("rich", "rich-nocard", "rich-text"):
            media = dict(pictures)
            if form == "rich-nocard":
                media.pop("card", None)
            if form == "rich-text":
                media = {}
            html = format_report_rich(report, dashboard_url, lang,
                                      figures={k: k for k in media})
            n = await send_rich_message_http(html, media=media, chat_ids=[chat_id])
        elif form == "card":
            text = format_report(report, dashboard_url, lang)
            n = await send_admin_photo_http(card or b"", caption=text,
                                            chat_ids=[chat_id]) if card else 0
        elif form == "text":
            n = await send_admin_message_http(
                format_report(report, dashboard_url, lang), chat_ids=[chat_id])
        else:
            raise SystemExit(f"unknown form {form!r}; one of {', '.join(FORMS)}")
        print(f"{form:<12} lang={lang} delivered={n}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--chat-id", type=int, required=True)
    parser.add_argument("--lang", default="en")
    parser.add_argument("--form", default="rich",
                        help="comma-separated: " + ", ".join(FORMS))
    parser.add_argument("--fixture", action="store_true",
                        help="render the 31.08–06.09.2026 week from fixed values")
    parser.add_argument("--db", default=os.getenv("DUCKDB_PATH", "data/analytics.duckdb"))
    parser.add_argument("--sales-type", default="retail")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    # httpx logs every request URL at INFO, and the Bot API URL carries the
    # token. A preview run must not print it into a terminal.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    from core.config import DASHBOARD_URL

    report = fixture_report() if args.fixture else live_report(args.db, args.sales_type)
    if report.current.orders == 0:
        print("the week has no orders in this copy; use --fixture", file=sys.stderr)
        return 1
    forms = [f.strip() for f in args.form.split(",") if f.strip()]
    asyncio.run(send(report, args.chat_id, args.lang, forms, DASHBOARD_URL or None))
    return 0


if __name__ == "__main__":
    sys.exit(main())
