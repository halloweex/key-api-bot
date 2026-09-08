"""The week's traffic, for the people who watch where orders come from.

The sales report answers "how much"; this one answers "from where", and the
two are deliberately separate messages. Someone reading the traffic tab wants
the attribution mix and the campaigns that moved, and burying that under a
revenue headline would make both harder to read.

**It reads through the same repository the tab does.** Every number here comes
from `get_traffic_analytics` and `get_traffic_utm_campaigns` — the exact calls
`/api/traffic/*` makes, with the same `sales_type="retail"` default. A report
that disagreed with the screen would cost more trust than it delivers, and
the only way to guarantee it does not is to ask the same question.

**No spend, no ROAS** (owner, 2026-09-08). The tab has a ROAS block; ad spend
does not come from KeyCRM and is entered by hand, so a weekly figure built on
it would be as fresh as somebody remembered to be. Revenue and orders are
facts this system owns.

**Attribution quality is a headline, not a footnote.** Every share below is a
share of the orders we could attribute, so the orders we could not are what
says whether the rest of the message can be trusted at all. A week where
unattributed traffic grows is a week where every other line quietly got less
true, and that has to be visible without being looked for.

Everything is pure except `build_report`, which takes the store and awaits it.
"""
from __future__ import annotations

import html
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Mapping, Optional

from core.i18n import DEFAULT_LANGUAGE, fmt_int, fmt_window, normalize, t
from core.weekly_report import (
    ChannelTotals,
    _delta,
    _money,
    _name,
    _signed_money,
    _td_right,
    last_complete_week,
    pct_change,
)

# Which book the traffic report reads. The tab's own default, so the message
# and the screen answer the same question.
TRAFFIC_SALES_TYPE = "retail"

# The buckets the tab's cards show, in the order it shows them. `paid` is the
# repository's confirmed and likely rolled together, exactly as the card does.
BUCKETS = ("paid", "organic", "manager", "pixel_only", "unknown")

# Attribution is the point of the report, so its own quality gets a threshold
# rather than a mention: above this share of orders with no tracking at all,
# the message says so in its first lines.
UNATTRIBUTED_WARN_PCT = 25.0

TOP_PLATFORMS = 5
TOP_CAMPAIGNS = 5

# A campaign that moved less than this is not news at the scale this business
# runs at; without a floor the movers list fills with rounding.
CAMPAIGN_FLOOR = 2_000.0

# What the repository calls a row with no campaign at all. It is by far the
# largest "mover" most weeks — on the first production run it was −₴346,815,
# four times the largest real one — and it is not a campaign, so it would push
# every actionable line off a list of five. Untagged traffic is already
# reported, by name, in the attribution buckets above.
NO_CAMPAIGN = ("", "—", "none", "unknown", "(not set)")


@dataclass(frozen=True)
class Bucket:
    """One attribution bucket over a week, against the week before."""
    name: str
    revenue: float
    orders: int
    previous_revenue: float
    previous_orders: int


@dataclass(frozen=True)
class CampaignMove:
    """One UTM campaign's revenue this week against the week before."""
    campaign: str
    platform: str
    current: float
    previous: float

    @property
    def delta(self) -> float:
        return self.current - self.previous


@dataclass(frozen=True)
class TrafficReport:
    start: date
    end: date
    sales_type: str
    revenue: float
    orders: int
    previous_revenue: float
    previous_orders: int
    buckets: List[Bucket] = field(default_factory=list)
    platforms: List[ChannelTotals] = field(default_factory=list)
    movers: List[CampaignMove] = field(default_factory=list)

    @property
    def unattributed_pct(self) -> Optional[float]:
        if not self.orders:
            return None
        unknown = next((b.orders for b in self.buckets if b.name == "unknown"), 0)
        return unknown / self.orders * 100.0

    @property
    def previous_unattributed_pct(self) -> Optional[float]:
        if not self.previous_orders:
            return None
        unknown = next(
            (b.previous_orders for b in self.buckets if b.name == "unknown"), 0
        )
        return unknown / self.previous_orders * 100.0


# ─── Reads ──────────────────────────────────────────────────────────────────

def _bucket_of(summary: Mapping, name: str) -> Dict[str, float]:
    row = summary.get(name) or {}
    return {"orders": int(row.get("orders", 0) or 0),
            "revenue": float(row.get("revenue", 0) or 0)}


async def build_report(
    store, today: date, sales_type: str = TRAFFIC_SALES_TYPE,
) -> TrafficReport:
    """Assemble the week from the same calls the tab makes.

    Four reads: analytics and campaigns, for this week and the one before.
    The campaigns are asked for by revenue and cut client-side, because a
    mover is a *difference* between two weeks and neither query can rank by
    something it cannot see.
    """
    start, end = last_complete_week(today)
    prev_start, prev_end = start - timedelta(days=7), start - timedelta(days=1)

    now = await store.get_traffic_analytics(
        start_date=start, end_date=end, sales_type=sales_type)
    before = await store.get_traffic_analytics(
        start_date=prev_start, end_date=prev_end, sales_type=sales_type)

    now_summary, before_summary = now.get("summary", {}), before.get("summary", {})
    buckets = []
    for name in BUCKETS:
        cur, prev = _bucket_of(now_summary, name), _bucket_of(before_summary, name)
        if not any((cur["orders"], cur["revenue"], prev["orders"], prev["revenue"])):
            continue
        buckets.append(Bucket(
            name=name, revenue=cur["revenue"], orders=cur["orders"],
            previous_revenue=prev["revenue"], previous_orders=prev["orders"],
        ))

    now_platforms = now.get("by_platform", {}) or {}
    before_platforms = before.get("by_platform", {}) or {}
    platforms = [
        ChannelTotals(
            name=key,
            revenue=float(value.get("revenue", 0) or 0),
            orders=int(value.get("orders", 0) or 0),
            previous_revenue=float(
                (before_platforms.get(key) or {}).get("revenue", 0) or 0),
        )
        for key, value in now_platforms.items()
    ]
    platforms.sort(key=lambda p: p.revenue, reverse=True)

    movers = await _fetch_movers(
        store, start, end, prev_start, prev_end, sales_type)

    return TrafficReport(
        start=start, end=end, sales_type=sales_type,
        revenue=float((now.get("totals") or {}).get("revenue", 0) or 0),
        orders=int((now.get("totals") or {}).get("orders", 0) or 0),
        previous_revenue=float((before.get("totals") or {}).get("revenue", 0) or 0),
        previous_orders=int((before.get("totals") or {}).get("orders", 0) or 0),
        buckets=buckets,
        platforms=platforms[:TOP_PLATFORMS],
        movers=movers,
    )


async def _fetch_movers(
    store, start: date, end: date, prev_start: date, prev_end: date, sales_type: str,
) -> List[CampaignMove]:
    """Campaigns ranked by hryvnia moved, largest in magnitude first.

    By hryvnia and never by percent, for the sales report's reason: a campaign
    that went from ₴500 to ₴2,500 outranks one that lost ₴80,000 on any
    percentage scale, and only one of the two is worth a Monday morning.
    """
    async def week(a: date, b: date) -> Dict[str, Dict]:
        page = await store.get_traffic_utm_campaigns(
            start_date=a, end_date=b, sales_type=sales_type, limit=200)
        out: Dict[str, Dict] = {}
        for row in page.get("campaigns", []):
            name = row.get("campaign") or "—"
            slot = out.setdefault(
                name, {"revenue": 0.0, "platform": row.get("platform") or ""})
            slot["revenue"] += float(row.get("revenue", 0) or 0)
        return out

    now, before = await week(start, end), await week(prev_start, prev_end)
    moves = [
        CampaignMove(
            campaign=name,
            platform=(now.get(name) or before.get(name) or {}).get("platform", ""),
            current=(now.get(name) or {}).get("revenue", 0.0),
            previous=(before.get(name) or {}).get("revenue", 0.0),
        )
        for name in set(now) | set(before)
    ]
    moves = [
        m for m in moves
        if abs(m.delta) >= CAMPAIGN_FLOOR
        and m.campaign.strip().lower() not in NO_CAMPAIGN
    ]
    moves.sort(key=lambda m: abs(m.delta), reverse=True)
    return moves[:TOP_CAMPAIGNS]


# ─── Send ledger ────────────────────────────────────────────────────────────

def already_sent(conn, week_start: date, sales_type: str) -> bool:
    row = conn.execute("""
        SELECT 1 FROM traffic_report_sends
        WHERE week_start = ? AND sales_type = ?
    """, [week_start, sales_type]).fetchone()
    return row is not None


def mark_sent(
    conn, week_start: date, sales_type: str, revenue: float, orders: int,
) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO traffic_report_sends
            (week_start, sales_type, revenue, orders, sent_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [week_start, sales_type, revenue, orders])


# ─── Rendering ──────────────────────────────────────────────────────────────

def _bucket_label(name: str, lang: str) -> str:
    return t(f"traffic.bucket.{name}", lang)


def _platform_label(name: str, lang: str) -> str:
    """A platform key as the tab writes it, or the raw key when it is new.

    Falling back to the key is deliberate: the classifier can start emitting a
    platform before anybody translates it, and a report that hid the new one
    would be worse than a report with an English word in it.
    """
    from core.i18n import has_key

    key = f"traffic.platform.{name}"
    return t(key, lang) if has_key(key) else name


@dataclass(frozen=True)
class ChartView:
    """What `render_channels_chart` reads: a `.channels` list, already
    labelled for one language.

    The platform chart is the sales report's channel chart fed platform
    totals — the same picture of the same shape of fact — so the two reports
    do not grow two visual languages for one idea. A view rather than a
    property because the labels are per language and the report is not.
    """
    channels: List[ChannelTotals]


def chart_view(report: "TrafficReport", lang: str) -> ChartView:
    return ChartView([
        ChannelTotals(
            name=_platform_label(p.name, lang),
            revenue=p.revenue,
            orders=p.orders,
            previous_revenue=p.previous_revenue,
        )
        for p in report.platforms
    ])


def format_report_rich(
    report: TrafficReport,
    dashboard_url: Optional[str] = None,
    lang: str = DEFAULT_LANGUAGE,
    figures: Optional[Mapping[str, str]] = None,
) -> str:
    """Render the traffic week as rich HTML, in `lang`."""
    lang = normalize(lang)
    esc = html.escape
    figs = dict(figures or {})

    out: List[str] = [
        f"<h1>{esc(t('traffic.title', lang).upper())}</h1>",
        f"<p>{fmt_window(report.start, report.end, lang)}</p>",
    ]
    out += _rich_summary(report, lang)
    out += _rich_buckets(report, lang)
    out += _rich_platforms(report, lang, figs.get("platforms"))
    out += _rich_movers(report, lang)

    if dashboard_url:
        url = dashboard_url.rstrip("/") + "/traffic"
        out.append(
            '<tg-button-row><tg-button type="url" style="primary" '
            f'url="{esc(url, quote=True)}">'
            f"{esc(t('traffic.open_tab', lang))}</tg-button></tg-button-row>"
        )
    return "\n".join(out)


def _rich_summary(report: TrafficReport, lang: str) -> List[str]:
    """Two sentences: what came in, and whether it can be trusted."""
    esc = html.escape
    lines = [
        esc(t("traffic.summary", lang,
              revenue=_money(report.revenue, lang),
              orders=fmt_int(report.orders, lang),
              delta=_delta(pct_change(report.revenue, report.previous_revenue), lang)))
    ]
    share = report.unattributed_pct
    if share is not None:
        before = report.previous_unattributed_pct
        moved = "" if before is None else " " + t(
            "traffic.unattributed_was", lang, was=f"{before:.0f}")
        mark = "⚠️" if share >= UNATTRIBUTED_WARN_PCT else "✅"
        lines.append(
            f"{mark} {esc(t('traffic.unattributed', lang, share=f'{share:.0f}'))}"
            f"{esc(moved)}"
        )
    return ["<blockquote>" + "<br/>".join(lines) + "</blockquote>"]


def _rich_buckets(report: TrafficReport, lang: str) -> List[str]:
    """Where the orders came from, as the tab's five cards read."""
    if not report.buckets:
        return []
    esc = html.escape
    total = sum(b.revenue for b in report.buckets)
    out = [
        f"<h3>{esc(t('traffic.by_type', lang).upper())}</h3>",
        "<table bordered striped compact>",
        f"<tr><th></th><th>{esc(t('report.col_this_week', lang))}</th>"
        f"<th>{esc(t('report.col_share', lang))}</th>"
        f"<th>{esc(t('report.col_orders', lang))}</th>"
        f"<th>{esc(t('report.col_change', lang))}</th></tr>",
    ]
    for b in report.buckets:
        out.append(
            f"<tr><td>{esc(_bucket_label(b.name, lang))}</td>"
            f"{_td_right(_money(b.revenue, lang))}"
            f"{_td_right(f'{b.revenue / total * 100:.0f}%' if total else '')}"
            f"{_td_right(fmt_int(b.orders, lang))}"
            f"{_td_right(_delta(pct_change(b.revenue, b.previous_revenue), lang))}</tr>"
        )
    out.append("</table>")
    return out


def _rich_platforms(
    report: TrafficReport, lang: str, figure: Optional[str] = None,
) -> List[str]:
    if not report.platforms:
        return []
    esc = html.escape
    out = [f"<h3>{esc(t('traffic.by_platform', lang).upper())}</h3>"]
    if figure:
        out.append(
            f'<figure><img src="tg://photo?id={esc(figure, quote=True)}"/>'
            f"<figcaption>{esc(t('report.fig_channels', lang))}</figcaption></figure>"
        )
        out.append(f"<details><summary>{esc(t('traffic.table_platforms', lang))}</summary>")
    total = sum(p.revenue for p in report.platforms)
    out += [
        "<table bordered striped compact>",
        f"<tr><th></th><th>{esc(t('report.col_this_week', lang))}</th>"
        f"<th>{esc(t('report.col_share', lang))}</th>"
        f"<th>{esc(t('report.col_orders', lang))}</th>"
        f"<th>{esc(t('report.col_change', lang))}</th></tr>",
    ]
    for p in report.platforms:
        out.append(
            f"<tr><td>{esc(_platform_label(p.name, lang))}</td>"
            f"{_td_right(_money(p.revenue, lang))}"
            f"{_td_right(f'{p.revenue / total * 100:.0f}%' if total else '')}"
            f"{_td_right(fmt_int(p.orders, lang))}"
            f"{_td_right(_delta(pct_change(p.revenue, p.previous_revenue), lang))}</tr>"
        )
    out.append("</table>")
    if figure:
        out.append("</details>")
    return out


def _rich_movers(report: TrafficReport, lang: str) -> List[str]:
    if not report.movers:
        return []
    esc = html.escape
    out = [
        f"<details open><summary>{esc(t('traffic.movers', lang))}</summary>",
        "<table compact>",
    ]
    for m in report.movers:
        out.append(
            f"<tr>{_td_right(_signed_money(m.delta, lang))}"
            f"<td>{_name(m.campaign)}</td>"
            f"<td>{esc(_platform_label(m.platform, lang))}</td></tr>"
        )
    out.append("</table></details>")
    return out


def format_report(
    report: TrafficReport,
    dashboard_url: Optional[str] = None,
    lang: str = DEFAULT_LANGUAGE,
) -> str:
    """The same week as plain Telegram HTML — the fallback rung.

    Deliberately not a rendering of the rich document with the tags stripped:
    the tables become lines, and the movers keep their sign because that is
    the one thing a reader scans for.
    """
    lang = normalize(lang)
    esc = html.escape
    lines: List[str] = [
        f"📈 <b>{esc(t('traffic.title', lang))}</b>",
        fmt_window(report.start, report.end, lang),
        "",
        esc(t("traffic.summary", lang,
              revenue=_money(report.revenue, lang),
              orders=fmt_int(report.orders, lang),
              delta=_delta(pct_change(report.revenue, report.previous_revenue), lang))),
    ]
    share = report.unattributed_pct
    if share is not None:
        mark = "⚠️" if share >= UNATTRIBUTED_WARN_PCT else "✅"
        lines.append(f"{mark} {esc(t('traffic.unattributed', lang, share=f'{share:.0f}'))}")

    if report.buckets:
        lines += ["", f"<b>{esc(t('traffic.by_type', lang))}</b>"]
        for b in report.buckets:
            lines.append(
                f"• {esc(_bucket_label(b.name, lang))}: {_money(b.revenue, lang)} "
                f"({fmt_int(b.orders, lang)}) "
                f"{_delta(pct_change(b.revenue, b.previous_revenue), lang)}"
            )
    if report.platforms:
        lines += ["", f"<b>{esc(t('traffic.by_platform', lang))}</b>"]
        for p in report.platforms:
            lines.append(
                f"• {esc(_platform_label(p.name, lang))}: {_money(p.revenue, lang)} "
                f"{_delta(pct_change(p.revenue, p.previous_revenue), lang)}"
            )
    if report.movers:
        lines += ["", f"<b>{esc(t('traffic.movers', lang))}</b>"]
        for m in report.movers:
            lines.append(f"• {_signed_money(m.delta, lang)}  {_name(m.campaign)}")

    if dashboard_url:
        url = dashboard_url.rstrip("/") + "/traffic"
        lines += ["", f'🔗 <a href="{esc(url, quote=True)}">'
                      f"{esc(t('traffic.open_tab', lang))}</a>"]
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)
