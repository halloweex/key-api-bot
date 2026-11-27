"""The rich form of the weekly report, and the transport that carries it.

Bot API 10.1 (June 2026) lets a bot send a document — headings, a table,
lists, a picture between paragraphs. The report's caption form stays as the
fallback, so what is pinned here is (1) the rich HTML uses only tags Telegram
documents, (2) the columns are a table and not padding, (3) the transport
uploads the card in the same request and signs only for admins, in a footer.
"""
from __future__ import annotations

import importlib
import json
import re
from datetime import date
from html.parser import HTMLParser

import os

import pytest
import pytest_asyncio

from core import telegram_alerts
from core.i18n import LANGUAGES, t
from core.telegram_alerts import sign_html_for, signature_line
from tests.integration.pg_tab_seed import needs_postgres
from core.weekly_report import (
    ChannelTotals,
    DayTotals,
    ProductMove,
    WeeklyReport,
    WeekTotals,
    fetch_channels,
    fetch_daily,
    format_report_rich,
)
from tests.helpers.rich_tags import SUPPORTED_TAGS

_REAL_RICH = telegram_alerts.send_rich_message_http
_CONFIG = importlib.import_module("core.config")

# <mark> is deliberately absent: its highlight does not show in the dark theme.


def _report(**overrides) -> WeeklyReport:
    cur = WeekTotals(revenue=805_788, orders=336,
                     new_customer_orders=131, repeat_orders=205)
    prev = WeekTotals(revenue=1_121_404, orders=433,
                      new_customer_orders=136, repeat_orders=297)
    fields = dict(
        start=date(2026, 8, 31), end=date(2026, 9, 6), sales_type="retail",
        current=cur, previous=prev, year_ago=WeekTotals(679_000, 300, 120, 180),
        baseline_mean=876_800, baseline_sd=194_000, baseline_weeks=12,
        movers=[ProductMove("Differ & Deeper serum", 1_000, 39_955)],
        product_move_total=-320_000,
    )
    fields.update(overrides)
    return WeeklyReport(**fields)


class _Read(HTMLParser):
    """A rich message as Telegram reads it: its tags, the pictures it names,
    its tables' rows (cell texts) and its text."""

    def __init__(self, html_text: str):
        super().__init__()
        self.tags: set = set()
        self.photos: list = []
        self.rows: list = []
        self.text = ""
        self._cell = None
        self.feed(html_text)
        self.close()

    def handle_starttag(self, tag, attrs):
        self.tags.add(tag)
        if tag == "img":
            src = dict(attrs).get("src") or ""
            self.photos.append(src[len("tg://photo?id="):]
                               if src.startswith("tg://photo?id=") else src)
        elif tag == "tr":
            self.rows.append([])
        elif tag in ("td", "th"):
            self._cell = ""

    def handle_endtag(self, tag):
        self.tags.add(tag)
        if tag in ("td", "th") and self._cell is not None:
            self.rows[-1].append(self._cell.strip())
            self._cell = None

    def handle_data(self, data):
        self.text += data
        if self._cell is not None:
            self._cell += data


_DAYS = [DayTotals(date(2026, 8, 31), 1, 1)]
# (report overrides, the pictures handed to the formatter)
SHAPES = {
    "bare": ({}, {}),
    "no_previous_week": (dict(previous=None), {}),
    "every_section_and_picture": (
        dict(days=_DAYS, previous_days=_DAYS, channels=[ChannelTotals("Shopify", 1, 1, 1)]),
        {"card": "card", "days": "days", "why": "why", "channels": "channels"}),
}

LEVER_CLAUSES = ("report.lever_orders_down", "report.lever_orders_up",
                 "report.lever_check_down", "report.lever_check_up")


class TestTelegramTakesIt:
    @pytest.mark.parametrize("lang", LANGUAGES)
    @pytest.mark.parametrize("shape", sorted(SHAPES))
    def test_documented_tags_and_exactly_the_pictures_it_is_handed(self, shape, lang):
        """Bot API 10.1 answers a tag it does not document with a 400, and the
        report falls back to the caption. A picture the message names and was
        not sent is a 400 too, and one sent and not named is never seen. In
        every language (a translation carrying markup counts), with no week
        to compare against, and with every section and picture present."""
        overrides, figures = SHAPES[shape]
        read = _Read(format_report_rich(
            _report(**overrides), "https://x.example", lang, figures=figures))
        assert read.tags <= SUPPORTED_TAGS, read.tags - SUPPORTED_TAGS
        assert sorted(read.photos) == sorted(figures.values())


class TestTheLever:
    @pytest.mark.parametrize("current, previous, named", [
        (None, None, "report.lever_orders_down"),
        (WeekTotals(revenue=1_000_000, orders=400), WeekTotals(revenue=800_000, orders=400),
         "report.lever_check_up"),
        (WeekTotals(revenue=880_000, orders=422), WeekTotals(revenue=800_000, orders=400),
         None),
    ], ids=["fewer_orders_steady_basket", "same_orders_bigger_basket", "split_55_45"])
    def test_the_summary_names_the_effect_that_carried_the_move(
            self, current, previous, named):
        """Named only when one effect carries 60 % of the move or more; a
        55/45 split moved both, and calling it either is a guess."""
        overrides = {} if current is None else dict(current=current, previous=previous)
        text = _Read(format_report_rich(_report(**overrides), None, "en")).text
        assert {k for k in LEVER_CLAUSES if t(k, "en") in text} == (
            {named} if named else set())
        assert (t("report.why_both", "en") in text) is (named is None)

    def test_each_effect_carries_its_share_of_the_move(self):
        """(433−336)·2 589.85 ≈ −251 215 of −315 616: the order count carries
        four fifths of the move, the basket the rest."""
        rows = _Read(format_report_rich(_report(), None, "en")).rows
        share = {row[0]: row[-1] for row in rows if row}
        assert share[t("report.effect_orders_down", "en")] == "80%"
        assert share[t("report.effect_check_down", "en")] == "20%"


class TestFormatReportRich:
    def test_columns_are_a_table_not_padding(self):
        html = format_report_rich(_report(), None, "uk")
        assert "<table" in html
        # Five headline rows plus a header, the two-row decomposition, and
        # the fixture's single mover.
        assert html.count("<tr>") == 6 + 2 + 1
        # The caption form padded labels with runs of spaces; the table must
        # not carry that habit across.
        assert "   " not in html

    def test_product_names_are_escaped(self):
        """A bare ampersand is a 400 from Telegram, same as in the caption."""
        html = format_report_rich(_report(), None, "en")
        assert "Differ &amp; Deeper" in html
        assert "Differ & Deeper" not in html

    def test_dashboard_is_a_button(self):
        html = format_report_rich(_report(), "https://x.example/?a=1&b=2", "en")
        assert '<tg-button type="url" style="primary" url="https://x.example/?a=1&amp;b=2">' in html
        assert "<tg-button-row>" in html

    def test_one_emoji_per_screen_and_headings_in_upper_case(self):
        """The shop bot's voice rules, carried over: one emoji, as a pointer
        (the verdict's), and no long dash in a sentence. Headings upper case
        is the brand book's Libre Franklin rule (p. 16)."""
        html = format_report_rich(_report(), None, "ru")
        assert html.startswith("<h1>НЕДЕЛЬНЫЙ ОТЧЁТ</h1>")
        visible = re.sub(r"<[^>]+>", "", html.split("<tg-reference")[0])
        assert visible.count("✅") == 1 and "📊" not in visible
        assert " — " not in visible

    def test_summary_speaks_plainly_and_links_the_footnote(self):
        html = format_report_rich(_report(), None, "en")
        assert ('<blockquote>✅ <a href="#note-z"><b>An ordinary week.</b></a> '
                "Revenue ₴ 805,788: ▼ 28.1% vs last week, ▼ 8.1% vs the 12-week average, "
                "▲ 18.7% vs the same week of 2025.<br/>") in html
        assert "z -0.4" not in html.split("<tg-reference")[0]  # jargon stays behind the tap
        assert ('<tg-reference name="note-z">An ordinary week: |z| &lt; 1.5. '
                "This week z = -0.4, σ = ₴ 194K over 12 weeks.</tg-reference>") in html
        assert "<details><summary>How to read this report</summary>" in html

    def test_the_reading_guide_is_two_formulas_and_four_lines(self):
        html = format_report_rich(_report(), None, "ru")
        guide = html.split("<summary>Как читать этот отчёт</summary>")[1].split("</details>")[0]
        assert guide.startswith(
            "<tg-math-block>\\text{Выручка} = \\text{Заказы} \\times \\text{Средний чек}</tg-math-block>")
        assert guide.count("<li>") == 4
        assert "<tg-math>|z| &lt; 1.5</tg-math>" in guide
        assert "<tg-math>z = \\frac{\\text{Выручка} - \\text{Среднее}_{12}}{\\sigma_{12}}</tg-math>" in guide
        # Short on purpose: the owner's complaint was paragraphs.
        assert len(re.sub(r"<[^>]+>", "", guide)) < 420

    def test_every_number_sits_beside_last_weeks(self):
        html = format_report_rich(_report(), None, "en")
        assert ("<tr><td>Orders</td><td align=\"right\"><b>336</b></td>"
                '<td align="right">433</td><td align="right">▼ 22.4%</td></tr>') in html
        assert "<tr><td>Repeat orders</td>" in html
        assert "<p>31.08 – 06.09.2026 · Retail</p>" in html

    def test_sparkline(self):
        from core.weekly_report import sparkline
        assert sparkline([0, 50, 100]) == "▁▅█"
        assert sparkline([0, 0]) == "▁▁"
        assert sparkline([]) == ""

    def test_days_and_channels_render_only_when_given(self):
        bare = format_report_rich(_report(), None, "en")
        assert "By day" not in bare and "By channel" not in bare

        days = [DayTotals(date(2026, 8, 31) + __import__("datetime").timedelta(days=i),
                          30_000 * (i + 1), 10) for i in range(7)]
        prev = [DayTotals(date(2026, 8, 24) + __import__("datetime").timedelta(days=i),
                          200_000, 20) for i in range(7)]
        channels = [ChannelTotals("Shopify", 466_887.51, 208, 721_543.63),
                    ChannelTotals("Instagram", 318_600, 121, 389_720.40)]
        html = format_report_rich(_report(days=days, previous_days=prev,
                                          channels=channels), None, "en")
        assert "<h3>BY DAY</h3>" in html and "<h3>BY CHANNEL</h3>" in html  # upper case: brand book p. 16
        assert "<p><code>▂▃▄▅▆▇█</code> Mon – Sun</p>" in html
        # Seven weekday rows, each beside the same weekday a week before.
        assert html.count("<tr><td>Mon</td>") + html.count("<tr><td><b>Mon</b></td>") == 1
        assert ('<tr><td>Mon</td><td align="right">₴ 30,000</td><td align="right">10</td>'
                '<td align="right">₴ 200,000</td></tr>') in html
        # The best day is bold; Sunday holds 210 000.
        assert "<td><b>Sun</b></td>" in html
        assert '<td align="right">59%</td>' in html  # Shopify's share of the two
        assert '<td align="right">₴ 721,544</td>' in html  # last week's Shopify


class TestFigures:
    def test_charts_replace_or_fold_their_tables(self):
        days = [DayTotals(date(2026, 8, 31), 1, 1)]
        channels = [ChannelTotals("Shopify", 1, 1, 1)]
        html = format_report_rich(
            _report(days=days, previous_days=days, channels=channels), None, "en",
            figures={"card": "card", "days": "days", "why": "why", "channels": "channels"})
        for role in ("card", "days", "why", "channels"):
            assert f'<img src="tg://photo?id={role}"/>' in html
        # The decomposition table is gone, the day and channel tables fold.
        assert "<tr><td>fewer orders</td>" not in html
        assert "<details><summary>Day table</summary>" in html
        assert "<details><summary>Channel table</summary>" in html
        assert "<code>" not in html  # no sparkline, no glyph bars: colour lives in pictures

    def test_the_three_charts_render(self):
        from core.weekly_report_image import (
            _load_chart_fonts, render_channels_chart, render_days_chart, render_waterfall,
        )
        if _load_chart_fonts() is None:
            pytest.skip("no DejaVu on this host")
        import datetime as dt
        days = [DayTotals(date(2026, 8, 31) + dt.timedelta(days=i), 1_000 * (i + 1), i + 1)
                for i in range(7)]
        prev = [DayTotals(date(2026, 8, 24) + dt.timedelta(days=i), 5_000, 5) for i in range(7)]
        channels = [ChannelTotals("Shopify", 466_887.51, 208, 721_543.63),
                    ChannelTotals("Instagram", 318_600, 121, 389_720.40),
                    ChannelTotals("Telegram", 20_300, 7, 10_139)]
        report = _report(days=days, previous_days=prev, channels=channels)
        for render in (render_days_chart, render_waterfall, render_channels_chart):
            png = render(report, "uk")
            assert png and png[:8] == b"\x89PNG\r\n\x1a\n", render.__name__

    def test_charts_return_none_without_their_data(self):
        from core.weekly_report_image import render_channels_chart, render_days_chart, render_waterfall
        assert render_days_chart(_report(), "en") is None
        assert render_channels_chart(_report(), "en") is None
        assert render_waterfall(_report(previous=None), "en") is None


class TestSignHtmlFor:
    def test_admin_gets_a_footer(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        out = sign_html_for("<p>x</p>", 1)
        assert out.endswith("<footer>· prod-vps</footer>")
        assert out.startswith("<p>x</p>")

    def test_non_admin_gets_no_footer(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        assert sign_html_for("<p>x</p>", 2) == "<p>x</p>"

    def test_idempotent(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        once = sign_html_for("<p>x</p>", 1)
        assert sign_html_for(once, 1) == once

    def test_instance_name_is_escaped(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "a<b")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        assert "<footer>· a&lt;b</footer>" in sign_html_for("<p>x</p>", 1)
        assert signature_line() == "· a<b"


class _FakeClient:
    def __init__(self, sent, status=200, body="ok"):
        self.sent, self.status, self.body = sent, status, body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, data=None, files=None, **kw):
        self.sent.append({"url": url, "data": data, "files": files})
        status, body = self.status, self.body

        class R:
            status_code = status
            text = body

            def raise_for_status(self):
                if status != 200:
                    raise RuntimeError(f"HTTP {status}")
        return R()


class TestRichTransport:
    @pytest.mark.asyncio
    async def test_uploads_media_and_signs_admins_only(self, monkeypatch):
        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setenv(telegram_alerts.INSTANCE_ENV, "prod-vps")
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        sent: list = []
        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient",
                            lambda **kw: _FakeClient(sent))

        delivered = await _REAL_RICH(
            '<p>x</p><figure><img src="tg://photo?id=card"/></figure>',
            media={"card": b"png"}, token="t", chat_ids=[1, 2],
        )
        assert delivered == 2
        assert {s["url"] for s in sent} == {
            "https://api.telegram.org/bott/sendRichMessage"}
        by_chat = {s["data"]["chat_id"]: s for s in sent}
        admin = json.loads(by_chat["1"]["data"]["rich_message"])
        user = json.loads(by_chat["2"]["data"]["rich_message"])
        assert admin["html"].endswith("<footer>· prod-vps</footer>")
        assert "footer" not in user["html"]
        # The card goes up once per request, under the id the HTML names.
        for s in sent:
            payload = json.loads(s["data"]["rich_message"])
            assert payload["media"] == [
                {"id": "card", "media": {"type": "photo", "media": "attach://card"}}]
            assert s["files"] == {"card": ("card.png", b"png", "image/png")}

    @pytest.mark.asyncio
    async def test_no_media_means_no_multipart_files(self, monkeypatch):
        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        sent: list = []
        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient",
                            lambda **kw: _FakeClient(sent))
        assert await _REAL_RICH("<p>x</p>", token="t", chat_ids=[1]) == 1
        assert sent[0]["files"] is None
        assert "media" not in json.loads(sent[0]["data"]["rich_message"])

    @pytest.mark.asyncio
    async def test_a_rejected_tag_counts_as_undelivered(self, monkeypatch):
        """0 is what lets the job fall back to the caption form."""
        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)
        monkeypatch.setattr(_CONFIG, "ADMIN_USER_IDS", {1})
        sent: list = []
        monkeypatch.setattr(
            telegram_alerts.httpx, "AsyncClient",
            lambda **kw: _FakeClient(sent, 400, "Bad Request: unsupported tag"))
        assert await _REAL_RICH("<h9>x</h9>", token="t", chat_ids=[1]) == 0

    @pytest.mark.asyncio
    async def test_kill_switch_wins(self, monkeypatch):
        monkeypatch.setenv(telegram_alerts.DISABLE_ENV, "1")

        def forbidden(*a, **kw):
            raise AssertionError("suppressed rich message opened an HTTP client")

        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient", forbidden)
        assert await _REAL_RICH("<p>x</p>", token="t", chat_ids=[1]) == 0

    @pytest.mark.asyncio
    async def test_the_suite_guard_covers_this_transport(self):
        """conftest replaces every transport with a stub; a third transport
        that slipped past it would be the cell-guard incident again."""
        assert await telegram_alerts.send_rich_message_http("<p>x</p>") == 0


# ─── The two new reads ──────────────────────────────────────────────────────

# Postgres' Gold, which the report reads alone since step 14's S14-07: a
# roll-up row per day (`source_id IS NULL`) and, for a channel, the fine row
# under it — a channel is `source_id` there, not a column.
CHANNEL_SOURCES = {"instagram": 1, "telegram": 2, "shopify": 4}


@pytest_asyncio.fixture
async def pg(tmp_path, monkeypatch):
    """A Postgres warehouse, emptied, and the trap store a read is handed."""
    from tests.integration.pg_tab_seed import (
        store_that_never_reaches_duckdb, tab_in_postgres,
    )

    async def _nothing(conn, pool):
        pass

    async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _nothing) as pool:
        yield pool, store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the report")


async def _gold(conn, day: date, revenue: float, orders: int, **channels):
    rows = [(None, revenue, orders)] + [
        (CHANNEL_SOURCES[name], channels[f"{name}_revenue"], channels[f"{name}_orders"])
        for name in CHANNEL_SOURCES if f"{name}_revenue" in channels]
    await conn.executemany(
        "INSERT INTO gold.daily_revenue (date, sales_type, source_id, revenue,"
        " orders_count, unique_customers, new_customers, returning_customers,"
        " returns_count, returns_revenue, avg_order_value)"
        " VALUES ($1, 'retail', $2, $3, $4, 0, 0, 0, 0, 0, 0)",
        [(day, source, rev, n) for source, rev, n in rows])


@needs_postgres
class TestFetchDaily:
    @pytest.mark.asyncio
    async def test_seven_rows_with_zeros_for_missing_days(self, pg):
        pool, reads = pg
        async with pool.acquire() as conn:
            await _gold(conn, date(2026, 8, 31), 1_000, 3)
            await _gold(conn, date(2026, 9, 6), 500, 1)
        days = await fetch_daily(reads, date(2026, 8, 31), date(2026, 9, 6), "retail")
        assert [d.day.weekday() for d in days] == list(range(7))
        assert [(d.revenue, d.orders) for d in days][1:6] == [(0.0, 0)] * 5
        assert (days[0].revenue, days[6].orders) == (1_000.0, 1)


@needs_postgres
class TestFetchChannels:
    @pytest.mark.asyncio
    async def test_sums_columns_and_drops_a_silent_channel(self, pg):
        pool, reads = pg
        async with pool.acquire() as conn:
            await _gold(conn, date(2026, 8, 31), 300, 3,
                        instagram_revenue=100, instagram_orders=1,
                        shopify_revenue=200, shopify_orders=2)
            await _gold(conn, date(2026, 8, 24), 50, 1,
                        shopify_revenue=50, shopify_orders=1)
        channels = await fetch_channels(
            reads, date(2026, 8, 31), date(2026, 9, 6),
            date(2026, 8, 24), date(2026, 8, 30), "retail")
        assert [(c.name, c.revenue, c.orders, c.previous_revenue) for c in channels] == [
            ("Shopify", 200.0, 2, 50.0), ("Instagram", 100.0, 1, 0.0),
        ]
