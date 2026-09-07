"""The Monday morning number: one week of sales, with something to compare it to.

A revenue figure on its own is wallpaper. ₴968,639 is a good week or a bad one
depending on three things the reader does not carry in their head, so every
headline here comes with all three: the week before, the four-week average, and
the same ISO week a year ago.

Two things keep it from becoming noise:

**An anomaly gate.** Weekly retail revenue swings with σ ≈ ₴276K around a mean
of ₴1.1M — a 25% week-on-week drop is an ordinary week, not an event. Reporting
every delta as news trains people to stop reading, so the report states plainly
whether the week is inside its own normal range (|z| < 1.5) before anyone reacts
to the percentage.

**A decomposition.** Revenue = orders × basket, exactly, so a move splits into
an order-count effect and a basket-size effect with no residual. Orders then
split again into new-customer orders and repeat orders. That chain turns "down
25%" into "we acquired fewer customers", which is the only form of the sentence
anyone can act on.

Everything here is pure except the four `fetch_*` functions, which take a live
DuckDB connection and return plain values. Formatting never touches the
database, so the wording is tested without one.
"""
from __future__ import annotations

import html
import statistics
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import List, Mapping, Optional, Tuple

from core.i18n import (
    DEFAULT_LANGUAGE,
    fmt_int,
    fmt_money,
    fmt_window,
    normalize,
    t,
)

# ─── Tunables ───────────────────────────────────────────────────────────────

# Weeks of history behind the anomaly gate. A quarter is long enough for the
# spread to mean something and short enough that last winter's season does not
# set today's expectation.
BASELINE_WEEKS = 12

# Below this many complete weeks the standard deviation is not worth quoting,
# and the report says nothing about normality rather than guessing.
MIN_BASELINE_WEEKS = 6

# How far outside its own spread a week must land before it is called unusual.
ANOMALY_Z = 1.5

TOP_MOVERS = 3

# Percentage moves smaller than this read as "flat" — a 0.1% change in the
# average basket is not a signal, and rendering it as ▼ invites a reaction.
FLAT_PCT = 0.5

MAX_NAME_CHARS = 38


# ─── Windows ────────────────────────────────────────────────────────────────

def last_complete_week(today: date) -> Tuple[date, date]:
    """Monday–Sunday of the last week that had fully ended before `today`.

    On Monday this is the week that ended yesterday. Run any other day it is
    still that same week, which is what makes the job idempotent: a container
    that was down on Monday reports the identical window on Tuesday.
    """
    this_monday = today - timedelta(days=today.weekday())
    start = this_monday - timedelta(days=7)
    return start, start + timedelta(days=6)


def same_week_last_year(start: date) -> Tuple[date, date]:
    """The same ISO week one year earlier, aligned on weekday.

    Calendar-date alignment would compare a Monday against a Wednesday and
    hand back the day-of-week effect as if it were growth. ISO week 1 of one
    year is week 1 of the next, so the promo calendar lines up too.
    """
    iso_year, iso_week, _ = start.isocalendar()
    try:
        ly_start = date.fromisocalendar(iso_year - 1, iso_week, 1)
    except ValueError:
        # Week 53 exists only in long years; fall back to the last week that
        # does, which is the closest comparable seven days available.
        ly_start = date.fromisocalendar(iso_year - 1, iso_week - 1, 1)
    return ly_start, ly_start + timedelta(days=6)


# ─── Values ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class WeekTotals:
    """One week of sales, as the Gold layer has it."""
    revenue: float
    orders: int
    # Orders split by whether the buyer had ordered before. Read from Silver,
    # where the partition is exact; None when that read was not made.
    new_customer_orders: Optional[int] = None
    repeat_orders: Optional[int] = None

    @property
    def avg_check(self) -> float:
        return self.revenue / self.orders if self.orders else 0.0


@dataclass(frozen=True)
class ProductMove:
    """One product's revenue in the reported week against the week before."""
    name: str
    current: float
    previous: float

    @property
    def delta(self) -> float:
        return self.current - self.previous


@dataclass(frozen=True)
class DayTotals:
    """One day of the reported week."""
    day: date
    revenue: float
    orders: int


@dataclass(frozen=True)
class ChannelTotals:
    """One sales channel over the week, against the week before."""
    name: str
    revenue: float
    orders: int
    previous_revenue: float


@dataclass(frozen=True)
class WeeklyReport:
    start: date
    end: date
    sales_type: str
    current: WeekTotals
    previous: Optional[WeekTotals]
    year_ago: Optional[WeekTotals]
    baseline_mean: Optional[float]
    baseline_sd: Optional[float]
    baseline_weeks: int
    movers: List[ProductMove]
    product_move_total: float
    # The rich form only: seven days each for this week and the one before,
    # and the channel split. Empty lists render nothing, so the caption form
    # and every older caller are untouched.
    days: List[DayTotals] = field(default_factory=list)
    previous_days: List[DayTotals] = field(default_factory=list)
    channels: List[ChannelTotals] = field(default_factory=list)

    @property
    def z(self) -> Optional[float]:
        """How far outside its own spread this week landed."""
        if (
            self.baseline_mean is None
            or self.baseline_sd is None
            or self.baseline_sd <= 0
            or self.baseline_weeks < MIN_BASELINE_WEEKS
        ):
            return None
        return (self.current.revenue - self.baseline_mean) / self.baseline_sd


# ─── Arithmetic ─────────────────────────────────────────────────────────────

def pct_change(current: float, previous: Optional[float]) -> Optional[float]:
    """Percent change, or None when there is no base to divide by."""
    if previous is None or previous == 0:
        return None
    return (current - previous) / previous * 100.0


def decompose(current: WeekTotals, previous: WeekTotals) -> Tuple[float, float]:
    """Split the revenue move into an order-count effect and a basket effect.

    Exact, not an approximation:
        (O₁−O₀)·A₀ + (A₁−A₀)·O₁ ≡ O₁A₁ − O₀A₀ ≡ ΔRevenue

    so the two numbers always add up to the headline and no residual has to be
    explained away. Which of them carries the move is the whole point of the
    report: fewer orders and a steady basket is an acquisition problem;
    the same orders at a smaller basket is a pricing or mix problem.
    """
    orders_effect = (current.orders - previous.orders) * previous.avg_check
    check_effect = (current.avg_check - previous.avg_check) * current.orders
    return orders_effect, check_effect


def share_of(part: float, whole: float) -> Optional[float]:
    """`part` as a percentage of `whole`, when that percentage means anything.

    Returns None when the whole is zero, or when the part points the other way,
    or when it overshoots — a week whose gains and losses nearly cancel has a
    near-zero total, and "this product is 4,000% of the move" is arithmetic
    that is true and says nothing.
    """
    if whole == 0:
        return None
    ratio = part / whole
    if not 0 < ratio <= 1.5:
        return None
    return ratio * 100.0


# ─── Reads ──────────────────────────────────────────────────────────────────

def fetch_week_totals(conn, start: date, end: date, sales_type: str) -> WeekTotals:
    """Revenue and orders from Gold, split by customer type from Silver.

    Revenue and the order count come from `gold_daily_revenue` so the message
    agrees to the hryvnia with the dashboard — a Telegram number that disagrees
    with the screen costs more trust than it delivers. The new/repeat split has
    no Gold column: summing daily `returning_customers` over seven days counts
    a buyer once per day they ordered. Silver partitions the orders themselves,
    exactly, and Gold is built from Silver in the same tick, so the two are
    always equally fresh.
    """
    row = conn.execute("""
        SELECT COALESCE(SUM(revenue), 0), COALESCE(SUM(orders_count), 0)
        FROM gold_daily_revenue
        WHERE date BETWEEN ? AND ? AND sales_type = ?
    """, [start, end, sales_type]).fetchone()

    split = conn.execute("""
        SELECT COUNT(DISTINCT CASE WHEN is_new_customer THEN id END),
               COUNT(DISTINCT CASE WHEN NOT is_new_customer THEN id END)
        FROM silver_orders
        WHERE order_date BETWEEN ? AND ?
          AND NOT is_return AND is_active_source AND sales_type = ?
    """, [start, end, sales_type]).fetchone()

    return WeekTotals(
        revenue=float(row[0] or 0),
        orders=int(row[1] or 0),
        new_customer_orders=int(split[0] or 0),
        repeat_orders=int(split[1] or 0),
    )


def fetch_weekly_series(
    conn, before: date, sales_type: str, weeks: int = BASELINE_WEEKS,
) -> List[float]:
    """Weekly revenue for the `weeks` complete weeks ending before `before`.

    `before` is a Monday, so every bucket is a full Monday–Sunday week. Weeks
    that begin before the first date on record are dropped — a half-populated
    first week would drag the mean down — and weeks inside the record with no
    orders at all are kept as zeros, because for a quiet sales type a dead week
    is a real observation and dropping it would flatter the average.
    """
    first_row = conn.execute(
        "SELECT MIN(date) FROM gold_daily_revenue WHERE sales_type = ?",
        [sales_type],
    ).fetchone()
    if first_row is None or first_row[0] is None:
        return []
    first_date = first_row[0]

    window_start = before - timedelta(days=7 * weeks)
    rows = conn.execute("""
        SELECT date_trunc('week', date) AS wk, SUM(revenue)
        FROM gold_daily_revenue
        WHERE sales_type = ? AND date >= ? AND date < ?
        GROUP BY 1
    """, [sales_type, window_start, before]).fetchall()

    by_week = {}
    for wk, revenue in rows:
        week_start = wk.date() if hasattr(wk, "date") else wk
        by_week[week_start] = float(revenue or 0)

    return [
        by_week.get(week_start, 0.0)
        for week_start in (window_start + timedelta(days=7 * i) for i in range(weeks))
        if week_start >= first_date
    ]


def fetch_product_moves(
    conn, start: date, end: date, prev_start: date, prev_end: date, sales_type: str,
) -> Tuple[List[ProductMove], float]:
    """Every product's week-on-week revenue move, largest in magnitude first.

    Ranked by absolute hryvnia, never by percent. Percent ranking floats a
    product that went from ₴504 to ₴2,527 to the top at +401% and buries the
    one that went from ₴101,885 to ₴19,966 — which by itself was a quarter of
    that week's entire decline.

    Returns the ranked moves and the net move across all products, so a caller
    can say what fraction of the week the top few explain.
    """
    rows = conn.execute("""
        WITH cur AS (
            SELECT product_id, ANY_VALUE(product_name) AS name,
                   SUM(product_revenue) AS revenue
            FROM gold_daily_products
            WHERE date BETWEEN ? AND ? AND sales_type = ?
            GROUP BY product_id
        ),
        prev AS (
            SELECT product_id, ANY_VALUE(product_name) AS name,
                   SUM(product_revenue) AS revenue
            FROM gold_daily_products
            WHERE date BETWEEN ? AND ? AND sales_type = ?
            GROUP BY product_id
        )
        SELECT COALESCE(cur.name, prev.name),
               COALESCE(cur.revenue, 0),
               COALESCE(prev.revenue, 0)
        FROM cur FULL OUTER JOIN prev USING (product_id)
    """, [start, end, sales_type, prev_start, prev_end, sales_type]).fetchall()

    moves = [
        ProductMove(name=name or "(unnamed)", current=float(cur), previous=float(prev))
        for name, cur, prev in rows
    ]
    total = sum(m.delta for m in moves)
    moves.sort(key=lambda m: abs(m.delta), reverse=True)
    return moves, total


def fetch_daily(conn, start: date, end: date, sales_type: str) -> List[DayTotals]:
    """Every day of the window, zeros where Gold has no row.

    A quiet type has days with no orders; a missing row there is a real
    observation, and the day table must keep its seven rows either way.
    """
    rows = conn.execute("""
        SELECT date, revenue, orders_count FROM gold_daily_revenue
        WHERE date BETWEEN ? AND ? AND sales_type = ?
    """, [start, end, sales_type]).fetchall()
    by_day = {
        (d.date() if hasattr(d, "date") else d): (float(r or 0), int(o or 0))
        for d, r, o in rows
    }
    return [
        DayTotals(day, *by_day.get(day, (0.0, 0)))
        for day in (start + timedelta(days=i) for i in range((end - start).days + 1))
    ]


# Gold names a channel by column, so the split can only see the channels
# somebody wrote a column for. For retail that is all of them: the exhibition
# source is its own sales_type and never lands in a retail row.
_CHANNEL_COLUMNS = (
    ("Instagram", "instagram_revenue", "instagram_orders"),
    ("Telegram", "telegram_revenue", "telegram_orders"),
    ("Shopify", "shopify_revenue", "shopify_orders"),
)


def fetch_channels(
    conn, start: date, end: date, prev_start: date, prev_end: date, sales_type: str,
) -> List[ChannelTotals]:
    """Revenue and orders per channel, this week and the week before.

    Largest first. A channel with nothing in either week is dropped rather
    than rendered as a row of zeros.
    """
    def totals(a: date, b: date):
        cols = ", ".join(
            f"COALESCE(SUM({rev}), 0), COALESCE(SUM({orders}), 0)"
            for _, rev, orders in _CHANNEL_COLUMNS
        )
        return conn.execute(
            f"SELECT {cols} FROM gold_daily_revenue "
            "WHERE date BETWEEN ? AND ? AND sales_type = ?",
            [a, b, sales_type],
        ).fetchone()

    cur, prev = totals(start, end), totals(prev_start, prev_end)
    out = [
        ChannelTotals(name, float(cur[2 * i]), int(cur[2 * i + 1]), float(prev[2 * i]))
        for i, (name, _, _) in enumerate(_CHANNEL_COLUMNS)
    ]
    out = [c for c in out if c.revenue or c.orders or c.previous_revenue]
    out.sort(key=lambda c: c.revenue, reverse=True)
    return out


def warehouse_max_date(conn) -> Optional[date]:
    """The last date the Gold layer knows about, across every sales type.

    The readiness gate. Asking whether the reported week has seven rows for
    *this* sales type would misfire on any type quiet enough to have a
    zero-order day — b2b runs nine orders a week. Asking whether the warehouse
    has moved past the week end is the same question without that trap.
    """
    row = conn.execute("SELECT MAX(date) FROM gold_daily_revenue").fetchone()
    return row[0] if row else None


def build_report(
    conn, today: date, sales_type: str = "retail", weeks: int = BASELINE_WEEKS,
) -> WeeklyReport:
    """Assemble every number the message needs, in four queries per window."""
    start, end = last_complete_week(today)
    prev_start, prev_end = start - timedelta(days=7), start - timedelta(days=1)
    ly_start, ly_end = same_week_last_year(start)

    current = fetch_week_totals(conn, start, end, sales_type)
    previous = fetch_week_totals(conn, prev_start, prev_end, sales_type)
    year_ago = fetch_week_totals(conn, ly_start, ly_end, sales_type)

    series = fetch_weekly_series(conn, start, sales_type, weeks)
    mean = statistics.fmean(series) if series else None
    # Sample standard deviation, not population: with a dozen points the wider
    # estimator is the honest one, and it errs towards calling a week normal.
    sd = statistics.stdev(series) if len(series) > 1 else None

    movers, product_total = fetch_product_moves(
        conn, start, end, prev_start, prev_end, sales_type
    )

    has_previous = bool(previous.orders or previous.revenue)
    return WeeklyReport(
        start=start,
        end=end,
        sales_type=sales_type,
        current=current,
        previous=previous if has_previous else None,
        year_ago=year_ago if year_ago.orders or year_ago.revenue else None,
        baseline_mean=mean,
        baseline_sd=sd,
        baseline_weeks=len(series),
        movers=movers[:TOP_MOVERS],
        product_move_total=product_total,
        days=fetch_daily(conn, start, end, sales_type),
        previous_days=(
            fetch_daily(conn, prev_start, prev_end, sales_type) if has_previous else []
        ),
        channels=fetch_channels(conn, start, end, prev_start, prev_end, sales_type),
    )


# ─── Send ledger ────────────────────────────────────────────────────────────

def already_sent(conn, week_start: date, sales_type: str) -> bool:
    """Has this week already gone out?

    The job runs daily and reports the last *complete* week, so six of seven
    firings find their week already sent and go quiet. That is the design: a
    weekly CronTrigger that misses its instant does not run late, it does not
    run at all — the next fire is computed a week out, and misfire grace has
    nothing to forgive. A daily tick plus this ledger turns a missed Monday
    into a Tuesday delivery instead of a silent gap.
    """
    row = conn.execute("""
        SELECT 1 FROM weekly_report_sends
        WHERE week_start = ? AND sales_type = ?
    """, [week_start, sales_type]).fetchone()
    return row is not None


def mark_sent(
    conn, week_start: date, sales_type: str, revenue: float, orders: int,
) -> None:
    """Record the delivery, with the numbers as sent."""
    conn.execute("""
        INSERT OR REPLACE INTO weekly_report_sends
            (week_start, sales_type, revenue, orders, sent_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [week_start, sales_type, revenue, orders])


# ─── Rendering ──────────────────────────────────────────────────────────────


def _money(value: float, lang: str) -> str:
    return fmt_money(value, lang)


def _signed_money(value: float, lang: str) -> str:
    return f"{'+' if value >= 0 else '-'}₴ {fmt_int(abs(value), lang)}"


def _compact(value: float, lang: str) -> str:
    """A rounded magnitude for σ, where the exact hryvnia is beside the point.

    K and M are left untranslated: they read the same in all three languages,
    and a translated suffix beside a Latin σ would be the odder thing.
    """
    if abs(value) >= 10_000_000:
        return f"₴ {value / 1_000_000:.1f}M"
    if abs(value) >= 1_000_000:
        return f"₴ {value / 1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"₴ {value / 1_000:.0f}K"
    return f"₴ {value:.0f}"


# Public name for the chart renderers; the underscore form stays for the
# callers that already use it.
def compact_money(value: float, lang: str) -> str:
    return _compact(value, lang)


def _delta(pct: Optional[float], lang: str) -> str:
    if pct is None:
        return t("report.no_base", lang)
    if abs(pct) < FLAT_PCT:
        return t("report.flat", lang)
    return f"{'▲' if pct > 0 else '▼'} {abs(pct):.1f}%"


def _name(raw: str) -> str:
    """Trim a product name and make it safe for an HTML-parsed message.

    The catalogue is full of names like "Differ & Deeper …". A bare ampersand
    makes Telegram reject the whole message as unparseable entities, so this is
    correctness, not politeness.
    """
    trimmed = raw.strip()
    if len(trimmed) > MAX_NAME_CHARS:
        trimmed = trimmed[:MAX_NAME_CHARS - 1].rstrip() + "…"
    return html.escape(trimmed)


def format_report(
    report: WeeklyReport,
    dashboard_url: Optional[str] = None,
    lang: str = DEFAULT_LANGUAGE,
) -> str:
    """Render the report as Telegram HTML, in `lang`."""
    lang = normalize(lang)
    cur, prev, ly = report.current, report.previous, report.year_ago

    # Label widths come from the translations, not from the English ones a
    # column layout was once eyeballed against: "Замовлення" is half again as
    # long as "Orders", and a hardcoded pad would stagger the whole block.
    labels = [t(k, lang) for k in
              ("report.revenue", "report.orders", "report.avg_check")]
    pad = max(len(label) for label in labels)

    lines: List[str] = [
        f"📊 <b>{t('report.title', lang)}</b>",
        f"{fmt_window(report.start, report.end, lang)} · "
        f"{html.escape(report.sales_type)}",
        "",
        f"{labels[0]:<{pad}}   <b>{_money(cur.revenue, lang)}</b>   "
        f"{_delta(pct_change(cur.revenue, prev.revenue if prev else None), lang)}",
        f"{labels[1]:<{pad}}   <b>{fmt_int(cur.orders, lang)}</b>   "
        f"{_delta(pct_change(cur.orders, prev.orders if prev else None), lang)}",
        f"{labels[2]:<{pad}}   <b>{_money(cur.avg_check, lang)}</b>   "
        f"{_delta(pct_change(cur.avg_check, prev.avg_check if prev else None), lang)}",
        "",
    ]

    if report.baseline_mean is not None and report.baseline_weeks >= MIN_BASELINE_WEEKS:
        lines.append(
            f"{t('report.vs_average', lang, weeks=report.baseline_weeks)}   "
            f"{_delta(pct_change(cur.revenue, report.baseline_mean), lang)}"
        )
    if ly:
        ly_year = same_week_last_year(report.start)[0].year
        lines.append(
            f"{t('report.vs_last_year', lang, year=ly_year)}   "
            f"{_delta(pct_change(cur.revenue, ly.revenue), lang)}"
        )

    z = report.z
    if z is not None:
        if abs(z) < ANOMALY_Z:
            mark, verdict = "✅", t("report.normal_range", lang)
        elif z > 0:
            mark, verdict = "🚀", t("report.unusually_high", lang)
        else:
            mark, verdict = "⚠️", t("report.unusually_low", lang)
        lines.append(
            f"{mark} {verdict} · z {z:+.1f} · σ {_compact(report.baseline_sd, lang)} "
            f"{t('report.over_weeks', lang, weeks=report.baseline_weeks)}"
        )

    lines += _what_moved(report, lang)
    lines += _top_movers(report, lang)

    if dashboard_url:
        lines += ["", f'🔗 <a href="{html.escape(dashboard_url, quote=True)}">'
                      f"{t('report.open_dashboard', lang)}</a>"]

    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)


def _what_moved(report: WeeklyReport, lang: str) -> List[str]:
    """The chain: revenue → orders or basket → new or repeat."""
    cur, prev = report.current, report.previous
    if prev is None:
        return []

    delta = cur.revenue - prev.revenue
    orders_effect, check_effect = decompose(cur, prev)

    headline = t("report.revenue_wow", lang, delta=_signed_money(delta, lang))
    # Name the lever only when one of the two clearly carries the move.
    # A 55/45 split is genuinely both, and saying otherwise is a guess.
    orders_share = share_of(orders_effect, delta)
    check_share = share_of(check_effect, delta)
    if orders_share is not None and orders_share >= 60:
        headline += t("report.lever_orders", lang)
    elif check_share is not None and check_share >= 60:
        headline += t("report.lever_basket", lang)

    effects = [t("report.effect_orders", lang), t("report.effect_check", lang)]
    pad = max(len(label) for label in effects)
    out = ["", f"<b>{t('report.what_moved', lang)}</b>", headline,
           f"• {effects[0]:<{pad}}  {_signed_money(orders_effect, lang)}",
           f"• {effects[1]:<{pad}}  {_signed_money(check_effect, lang)}"]

    if cur.new_customer_orders is None or prev.new_customer_orders is None:
        return out

    split = [t("report.new_orders", lang), t("report.repeat_orders", lang)]
    pad = max(len(label) for label in split)
    out += [
        "",
        f"{split[0]:<{pad}}   <b>{fmt_int(cur.new_customer_orders, lang)}</b>   "
        f"{_delta(pct_change(cur.new_customer_orders, prev.new_customer_orders), lang)}",
        f"{split[1]:<{pad}}   <b>{fmt_int(cur.repeat_orders, lang)}</b>   "
        f"{_delta(pct_change(cur.repeat_orders, prev.repeat_orders), lang)}",
    ]

    order_delta = cur.orders - prev.orders
    new_delta = cur.new_customer_orders - prev.new_customer_orders
    new_share = share_of(new_delta, order_delta)
    if new_share is not None and abs(order_delta) >= 5:
        key = "report.new_share_gain" if order_delta > 0 else "report.new_share_drop"
        out.append(t(key, lang, share=f"{new_share:.0f}"))
    return out


def _top_movers(report: WeeklyReport, lang: str) -> List[str]:
    if not report.movers:
        return []
    out = ["", f"<b>{t('report.top_movers', lang)}</b>"]
    for m in report.movers:
        out.append(f"• {_signed_money(m.delta, lang)}  {_name(m.name)}")

    top_sum = sum(m.delta for m in report.movers)
    top_share = share_of(top_sum, report.product_move_total)
    if top_share is not None:
        out.append(t("report.movers_share", lang,
                     count=len(report.movers), share=f"{top_share:.0f}"))
    return out


# ─── The rich form ──────────────────────────────────────────────────────────
#
# Bot API 10.1 (June 2026) lets a bot send a document — headings, a real
# table, lists, a picture between paragraphs — instead of a caption under a
# photo. The caption form above is kept as the fallback: a client that cannot
# render rich messages, or a tag the API refuses, costs the shape of the
# report and never the report.
#
# Written for the reader who is not an analyst. The caption form says
# "z −0.4 · σ ₴194K"; this one says "an ordinary week" and keeps z and σ
# behind a tap. Every comparison shows both numbers, because "336 against
# 433" is understood by everyone and "▼ 22.4%" by fewer than it seems.

_NOTE_Z = "note-z"
_SPARK = "▁▂▃▄▅▆▇█"
_LEVER = "\x00lever\x00"


def _td_right(inner: str) -> str:
    return f'<td align="right">{inner}</td>'


def _pct_of(part: float, whole: float) -> str:
    """A share for a table cell, blank when the whole is nothing."""
    return f"{part / whole * 100:.0f}%" if whole else ""


def sparkline(values: List[float]) -> str:
    """Seven days as seven block glyphs, tallest bar for the best day."""
    top = max(values) if values else 0
    if top <= 0:
        return _SPARK[0] * len(values)
    return "".join(_SPARK[min(7, int(v / top * 7 + 0.5))] for v in values)


def _sales_type_label(sales_type: str, lang: str) -> str:
    """"Розница" for retail; the raw name for anything the table lacks."""
    from core.i18n import has_key

    key = f"report.sales_type.{sales_type}"
    return t(key, lang) if has_key(key) else sales_type


def _lever(report: WeeklyReport, lang: str) -> Optional[Tuple[str, str]]:
    """(the marked clause, the sentence key it belongs in), or None.

    Named only when one effect carries at least 60% of the move; a 55/45
    split is both, and calling it one of them is a guess."""
    cur, prev = report.current, report.previous
    if prev is None:
        return None
    delta = cur.revenue - prev.revenue
    orders_effect, check_effect = decompose(cur, prev)
    orders_share = share_of(orders_effect, delta)
    check_share = share_of(check_effect, delta)
    if orders_share is not None and orders_share >= 60:
        key = "report.lever_orders_down" if orders_effect < 0 else "report.lever_orders_up"
        return t(key, lang), "report.why_lever_orders"
    if check_share is not None and check_share >= 60:
        key = "report.lever_check_down" if check_effect < 0 else "report.lever_check_up"
        return t(key, lang), "report.why_lever_check"
    return None


def _lever_sentence(report: WeeklyReport, lang: str) -> Optional[str]:
    """The one sentence that says why, with the lever marked."""
    esc = html.escape
    if report.previous is None:
        return None
    lever = _lever(report, lang)
    if lever is None:
        return esc(t("report.why_both", lang))
    clause, sentence_key = lever
    # The template is escaped whole, then the clause is dropped in — so the
    # markup never passes through the escaper. Bold, not <mark>: the marker's
    # highlight is invisible in Telegram's dark theme (owner, 2026-09-07).
    template = esc(t(sentence_key, lang, lever=_LEVER))
    return template.replace(_LEVER, f"<b>{esc(clause)}</b>")


# The pictures a rich report may carry, by role. The transport uploads each
# under its media id; the HTML refers to it as tg://photo?id=<id>.
FIGURE_CARD, FIGURE_DAYS, FIGURE_WHY, FIGURE_CHANNELS = "card", "days", "why", "channels"


def _figure(media_id: str, caption: Optional[str] = None) -> str:
    esc = html.escape
    img = f'<img src="tg://photo?id={esc(media_id, quote=True)}"/>'
    cap = f"<figcaption>{caption}</figcaption>" if caption else ""
    return f"<figure>{img}{cap}</figure>"


def format_report_rich(
    report: WeeklyReport,
    dashboard_url: Optional[str] = None,
    lang: str = DEFAULT_LANGUAGE,
    card_id: Optional[str] = None,
    figures: Optional[Mapping[str, str]] = None,
) -> str:
    """Render the report as rich HTML, in `lang`.

    `figures` maps a role (`FIGURE_CARD`, `FIGURE_DAYS`, `FIGURE_WHY`) to the
    media id the transport will upload that picture under. A chart that is
    present replaces the table it stands for — the day table folds into a
    collapsed block, the decomposition table goes — because a reader who
    can see the shape does not need the seven numbers, and the one who does
    is one tap away. `card_id` is the older spelling of `figures["card"]`.
    """
    lang = normalize(lang)
    esc = html.escape
    figs = dict(figures or {})
    if card_id:
        figs.setdefault(FIGURE_CARD, card_id)

    out: List[str] = [
        # Headings in upper case, the brand's Libre Franklin rule (p. 16), and
        # no emoji on them: one emoji per screen, and it is the verdict's.
        f"<h1>{esc(t('report.title', lang).upper())}</h1>",
        f"<p>{fmt_window(report.start, report.end, lang)} · "
        f"{esc(_sales_type_label(report.sales_type, lang))}</p>",
    ]
    if FIGURE_CARD in figs:
        out.append(_figure(figs[FIGURE_CARD]))

    out += _rich_summary(report, lang)
    out += _rich_numbers(report, lang)
    out += _rich_why(report, lang, figs.get(FIGURE_WHY))
    out += _rich_days(report, lang, figs.get(FIGURE_DAYS))
    out += _rich_channels(report, lang, figs.get(FIGURE_CHANNELS))
    out += _rich_top_movers(report, lang)
    out += _rich_footnotes(report, lang)

    if dashboard_url:
        out.append(
            '<tg-button-row><tg-button type="url" style="primary" '
            f'url="{esc(dashboard_url, quote=True)}">'
            f"{esc(t('report.open_dashboard', lang))}</tg-button></tg-button-row>"
        )
    return "\n".join(out)


def _rich_summary(report: WeeklyReport, lang: str) -> List[str]:
    """Three sentences a reader can stop after: the verdict, the revenue
    against everything it can be compared with, and why it moved."""
    cur, prev, ly = report.current, report.previous, report.year_ago
    esc = html.escape

    z = report.z
    if z is None:
        verdict = ""
    else:
        if abs(z) < ANOMALY_Z:
            mark, key = "✅", "report.summary_normal"
        elif z > 0:
            mark, key = "🚀", "report.summary_high"
        else:
            mark, key = "⚠️", "report.summary_low"
        # The verdict is the link to the footnote that says how it is judged.
        verdict = f'{mark} <a href="#{_NOTE_Z}"><b>{esc(t(key, lang))}</b></a> '

    clauses: List[str] = []
    if prev:
        clauses.append(t("report.clause_vs_prev", lang,
                         delta=_delta(pct_change(cur.revenue, prev.revenue), lang)))
    if report.baseline_mean is not None and report.baseline_weeks >= MIN_BASELINE_WEEKS:
        clauses.append(t("report.clause_vs_avg", lang, weeks=report.baseline_weeks,
                         delta=_delta(pct_change(cur.revenue, report.baseline_mean), lang)))
    if ly:
        clauses.append(t("report.clause_vs_ly", lang,
                         year=same_week_last_year(report.start)[0].year,
                         delta=_delta(pct_change(cur.revenue, ly.revenue), lang)))
    revenue = (
        esc(t("report.summary_revenue", lang, revenue=_money(cur.revenue, lang),
              clauses=", ".join(clauses)))
        if clauses else f"{esc(t('report.revenue', lang))} {_money(cur.revenue, lang)}."
    )
    parts = [verdict + revenue]
    why = _lever_sentence(report, lang)
    if why:
        parts.append(why)
    return ["<blockquote>" + "<br/>".join(parts) + "</blockquote>"]


def _rich_numbers(report: WeeklyReport, lang: str) -> List[str]:
    """Every headline number beside last week's, then the change."""
    cur, prev = report.current, report.previous
    esc = html.escape

    def money(v: Optional[float]) -> str:
        return _money(v, lang) if v is not None else ""

    def count(v: Optional[int]) -> str:
        return fmt_int(v, lang) if v is not None else ""

    rows = [
        (t("report.revenue", lang), money(cur.revenue), money(prev.revenue if prev else None),
         pct_change(cur.revenue, prev.revenue if prev else None)),
        (t("report.orders", lang), count(cur.orders), count(prev.orders if prev else None),
         pct_change(cur.orders, prev.orders if prev else None)),
        (t("report.avg_check_full", lang), money(cur.avg_check),
         money(prev.avg_check if prev else None),
         pct_change(cur.avg_check, prev.avg_check if prev else None)),
    ]
    if cur.new_customer_orders is not None and cur.repeat_orders is not None:
        prev_new = prev.new_customer_orders if prev else None
        prev_rep = prev.repeat_orders if prev else None
        rows += [
            (t("report.new_orders", lang), count(cur.new_customer_orders), count(prev_new),
             pct_change(cur.new_customer_orders, prev_new)),
            (t("report.repeat_orders", lang), count(cur.repeat_orders), count(prev_rep),
             pct_change(cur.repeat_orders, prev_rep)),
        ]
    table = [
        "<table bordered compact>",
        f"<tr><th></th><th>{esc(t('report.col_this_week', lang))}</th>"
        f"<th>{esc(t('report.col_last_week', lang))}</th>"
        f"<th>{esc(t('report.col_change', lang))}</th></tr>",
    ]
    for label, now, before, pct in rows:
        table.append(
            f"<tr><td>{esc(label)}</td>{_td_right(f'<b>{now}</b>')}"
            f"{_td_right(before)}{_td_right(_delta(pct, lang) if before else '')}</tr>"
        )
    table.append("</table>")
    return table


def _rich_why(report: WeeklyReport, lang: str, figure: Optional[str] = None) -> List[str]:
    """The move in hryvnia, split into the two things that can move it.

    With a waterfall chart the two-row table is not drawn: the chart carries
    the same four numbers as labels, in the order they happened."""
    cur, prev = report.current, report.previous
    if prev is None:
        return []
    esc = html.escape

    delta = cur.revenue - prev.revenue
    orders_effect, check_effect = decompose(cur, prev)

    def row(effect: float, down_key: str, up_key: str) -> str:
        label = t(down_key if effect < 0 else up_key, lang)
        share = share_of(effect, delta)
        return (f"<tr><td>{esc(label)}</td>{_td_right(_signed_money(effect, lang))}"
                f"{_td_right(f'{share:.0f}%' if share is not None else '')}</tr>")

    out = [f"<h3>{esc(t('report.why_title', lang).upper())}</h3>"]
    if figure:
        out.append(_figure(figure, esc(t("report.fig_why", lang,
                                          delta=_signed_money(delta, lang)))))
    else:
        out += [
            f"<p>{esc(t('report.why_delta', lang, delta=_signed_money(delta, lang)))}</p>",
            "<table compact>",
            row(orders_effect, "report.effect_orders_down", "report.effect_orders_up"),
            row(check_effect, "report.effect_check_down", "report.effect_check_up"),
            "</table>",
        ]

    if cur.new_customer_orders is None or prev.new_customer_orders is None:
        return out
    order_delta = cur.orders - prev.orders
    new_delta = cur.new_customer_orders - prev.new_customer_orders
    new_share = share_of(new_delta, order_delta)
    if new_share is not None and abs(order_delta) >= 5:
        key = ("report.new_share_gain_plain" if order_delta > 0
               else "report.new_share_drop_plain")
        out.append(f"<p><i>{esc(t(key, lang, share=f'{new_share:.0f}'))}</i></p>")
    return out


def _rich_days(report: WeeklyReport, lang: str, figure: Optional[str] = None) -> List[str]:
    """Seven rows, each against the same weekday the week before.

    Same weekday, not the same position: a Saturday is compared with a
    Saturday, which is the only comparison that does not hand back the
    day-of-week effect as if it were news. The best day is bold.

    With the chart, the table folds into a collapsed block and the sparkline
    goes: the picture is the sparkline, at a size that can be read.
    """
    if not report.days:
        return []
    esc = html.escape
    prev = {d.day.weekday(): d for d in report.previous_days}
    best = max(report.days, key=lambda d: d.revenue)
    first, last = report.days[0].day.weekday(), report.days[-1].day.weekday()
    out = [f"<h3>{esc(t('report.by_day', lang).upper())}</h3>"]
    if figure:
        out.append(_figure(figure, esc(t("report.fig_days", lang))))
        out.append(f"<details><summary>{esc(t('report.table_days', lang))}</summary>")
    else:
        # Monospace so the seven glyphs sit at equal widths, one per day,
        # and the two labels say which end is Monday.
        out.append(
            f"<p><code>{sparkline([d.revenue for d in report.days])}</code> "
            f"{esc(t(f'weekday.{first}', lang))} – {esc(t(f'weekday.{last}', lang))}</p>")
    out += [
        "<table bordered striped compact>",
        f"<tr><th></th><th>{esc(t('report.col_revenue', lang))}</th>"
        f"<th>{esc(t('report.col_orders', lang))}</th>"
        f"<th>{esc(t('report.col_week_ago', lang))}</th></tr>",
    ]
    for d in report.days:
        before = prev.get(d.day.weekday())
        label = esc(t(f"weekday.{d.day.weekday()}", lang))
        revenue = _money(d.revenue, lang)
        if d is best and d.revenue:
            label, revenue = f"<b>{label}</b>", f"<b>{revenue}</b>"
        out.append(
            f"<tr><td>{label}</td>{_td_right(revenue)}{_td_right(fmt_int(d.orders, lang))}"
            f"{_td_right(_money(before.revenue, lang) if before else '')}</tr>"
        )
    out.append("</table>")
    if figure:
        out.append("</details>")
    return out


def _rich_channels(report: WeeklyReport, lang: str, figure: Optional[str] = None) -> List[str]:
    """Share and change per channel.

    With the chart, the table folds into a collapsed block: Telegram sets
    table text in the reader's theme colour, so a share drawn in glyphs
    there is monochrome by construction — the picture is where colour lives.
    """
    if not report.channels:
        return []
    esc = html.escape
    total = sum(c.revenue for c in report.channels)
    out = [f"<h3>{esc(t('report.by_channel', lang).upper())}</h3>"]
    if figure:
        out.append(_figure(figure, esc(t("report.fig_channels", lang))))
        out.append(f"<details><summary>{esc(t('report.table_channels', lang))}</summary>")
    out += [
        "<table bordered striped compact>",
        f"<tr><th></th><th>{esc(t('report.col_this_week', lang))}</th>"
        f"<th>{esc(t('report.col_share', lang))}</th>"
        f"<th>{esc(t('report.col_last_week', lang))}</th>"
        f"<th>{esc(t('report.col_change', lang))}</th></tr>",
    ]
    for c in report.channels:
        out.append(
            f"<tr><td>{esc(c.name)}</td>{_td_right(_money(c.revenue, lang))}"
            f"{_td_right(_pct_of(c.revenue, total))}"
            f"{_td_right(_money(c.previous_revenue, lang))}"
            f"{_td_right(_delta(pct_change(c.revenue, c.previous_revenue), lang))}</tr>"
        )
    out.append("</table>")
    if figure:
        out.append("</details>")
    return out


def _rich_top_movers(report: WeeklyReport, lang: str) -> List[str]:
    """Open by default, collapsible: three product names are detail, but
    detail the reader asked for every week so far."""
    if not report.movers:
        return []
    esc = html.escape
    out = [
        f"<details open><summary>{esc(t('report.movers_title_plain', lang))}</summary>",
        "<table compact>",
    ]
    for m in report.movers:
        out.append(f"<tr>{_td_right(_signed_money(m.delta, lang))}<td>{_name(m.name)}</td></tr>")
    out.append("</table>")
    top_share = share_of(sum(m.delta for m in report.movers), report.product_move_total)
    if top_share is not None:
        out.append(
            f"<p><i>{esc(t('report.movers_share_plain', lang, share=f'{top_share:.0f}'))}</i></p>"
        )
    out.append("</details>")
    return out


def _rich_footnotes(report: WeeklyReport, lang: str) -> List[str]:
    """What the verdict link opens, then the collapsed reading guide.

    The guide is two formulas and four lines. `<tg-math-block>` and
    `<tg-math>` take raw LaTeX (Bot API 10.1); the words inside `\text{}` come
    from the translation table, the operators do not.
    """
    esc = html.escape
    out: List[str] = []
    z = report.z
    if z is not None:
        out.append(
            f'<tg-reference name="{_NOTE_Z}">'
            f"{esc(t('report.note_z', lang, weeks=report.baseline_weeks, z=f'{z:+.1f}', sigma=_compact(report.baseline_sd, lang)))}"
            "</tg-reference>"
        )

    def word(key: str) -> str:
        return r"\text{" + t(key, lang) + "}"

    formula_revenue = (
        f"{word('report.revenue')} = {word('report.orders')} "
        rf"\times {word('report.avg_check_full')}"
    )
    formula_z = (
        rf"z = \frac{{{word('report.revenue')} - {word('report.f_mean')}_{{12}}}}"
        r"{\sigma_{12}}"
    )
    rule, formula = "\x00rule\x00", "\x00formula\x00"
    normal = esc(t("report.howto_normal", lang, rule=rule, formula=formula))
    normal = normal.replace(rule, f"<tg-math>{esc('|z| < 1.5')}</tg-math>")
    normal = normal.replace(formula, f"<tg-math>{esc(formula_z)}</tg-math>")
    out.append(
        f"<details><summary>{esc(t('report.howto', lang))}</summary>"
        f"<tg-math-block>{esc(formula_revenue)}</tg-math-block>"
        "<ul>"
        f"<li>{esc(t('report.howto_split', lang))}</li>"
        f"<li>{normal}</li>"
        f"<li>{esc(t('report.howto_days', lang))}</li>"
        f"<li>{esc(t('report.howto_products', lang))}</li>"
        "</ul></details>"
    )
    return out
