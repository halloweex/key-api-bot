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
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional, Tuple

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

_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


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

    return WeeklyReport(
        start=start,
        end=end,
        sales_type=sales_type,
        current=current,
        previous=previous if previous.orders or previous.revenue else None,
        year_ago=year_ago if year_ago.orders or year_ago.revenue else None,
        baseline_mean=mean,
        baseline_sd=sd,
        baseline_weeks=len(series),
        movers=movers[:TOP_MOVERS],
        product_move_total=product_total,
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

def _money(value: float) -> str:
    return f"₴{value:,.0f}"


def _signed_money(value: float) -> str:
    return f"{'+' if value >= 0 else '-'}₴{abs(value):,.0f}"


def _compact(value: float) -> str:
    if abs(value) >= 1_000_000:
        return f"₴{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"₴{value / 1_000:.0f}K"
    return f"₴{value:.0f}"


def _delta(pct: Optional[float]) -> str:
    if pct is None:
        return "—"
    if abs(pct) < FLAT_PCT:
        return "≈ flat"
    return f"{'▲' if pct > 0 else '▼'} {abs(pct):.1f}%"


def _window(start: date, end: date) -> str:
    if start.month == end.month:
        return f"{start.day}–{end.day} {_MONTHS[end.month - 1]} {end.year}"
    return (f"{start.day} {_MONTHS[start.month - 1]} – "
            f"{end.day} {_MONTHS[end.month - 1]} {end.year}")


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


def format_report(report: WeeklyReport, dashboard_url: Optional[str] = None) -> str:
    """Render the report as Telegram HTML."""
    cur, prev, ly = report.current, report.previous, report.year_ago
    lines: List[str] = [
        "📊 <b>Weekly report</b>",
        f"{_window(report.start, report.end)} · {html.escape(report.sales_type)}",
        "",
        f"Revenue    <b>{_money(cur.revenue)}</b>   "
        f"{_delta(pct_change(cur.revenue, prev.revenue if prev else None))}",
        f"Orders     <b>{cur.orders:,}</b>   "
        f"{_delta(pct_change(cur.orders, prev.orders if prev else None))}",
        f"Avg check  <b>{_money(cur.avg_check)}</b>   "
        f"{_delta(pct_change(cur.avg_check, prev.avg_check if prev else None))}",
        "",
    ]

    if report.baseline_mean is not None and report.baseline_weeks >= MIN_BASELINE_WEEKS:
        lines.append(
            f"vs {report.baseline_weeks}-week average   "
            f"{_delta(pct_change(cur.revenue, report.baseline_mean))}"
        )
    if ly:
        ly_year = same_week_last_year(report.start)[0].year
        lines.append(
            f"vs same week {ly_year}   "
            f"{_delta(pct_change(cur.revenue, ly.revenue))}"
        )

    z = report.z
    if z is not None:
        if abs(z) < ANOMALY_Z:
            lines.append(
                f"✅ Inside the normal range · z {z:+.1f} · "
                f"σ {_compact(report.baseline_sd)} over {report.baseline_weeks}w"
            )
        else:
            mark = "🚀" if z > 0 else "⚠️"
            word = "high" if z > 0 else "low"
            lines.append(
                f"{mark} Unusually {word} · z {z:+.1f} · "
                f"σ {_compact(report.baseline_sd)} over {report.baseline_weeks}w"
            )

    lines += _what_moved(report)
    lines += _top_movers(report)

    if dashboard_url:
        lines += ["", f'🔗 <a href="{html.escape(dashboard_url, quote=True)}">'
                      f"Open the dashboard</a>"]

    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)


def _what_moved(report: WeeklyReport) -> List[str]:
    """The chain: revenue → orders or basket → new or repeat."""
    cur, prev = report.current, report.previous
    if prev is None:
        return []

    delta = cur.revenue - prev.revenue
    orders_effect, check_effect = decompose(cur, prev)

    headline = f"Revenue {_signed_money(delta)} week on week"
    # Name the lever only when one of the two clearly carries the move.
    # A 55/45 split is genuinely both, and saying otherwise is a guess.
    orders_share = share_of(orders_effect, delta)
    check_share = share_of(check_effect, delta)
    if orders_share is not None and orders_share >= 60:
        headline += " — order count, not basket size:"
    elif check_share is not None and check_share >= 60:
        headline += " — basket size, not order count:"

    out = ["", "<b>What moved</b>", headline,
           f"• order count  {_signed_money(orders_effect)}",
           f"• avg check    {_signed_money(check_effect)}"]

    if cur.new_customer_orders is None or prev.new_customer_orders is None:
        return out

    out += [
        "",
        f"Orders from new customers   <b>{cur.new_customer_orders:,}</b>   "
        f"{_delta(pct_change(cur.new_customer_orders, prev.new_customer_orders))}",
        f"Repeat orders               <b>{cur.repeat_orders:,}</b>   "
        f"{_delta(pct_change(cur.repeat_orders, prev.repeat_orders))}",
    ]

    order_delta = cur.orders - prev.orders
    new_delta = cur.new_customer_orders - prev.new_customer_orders
    new_share = share_of(new_delta, order_delta)
    if new_share is not None and abs(order_delta) >= 5:
        word = "gain" if order_delta > 0 else "drop"
        out.append(f"New-customer orders are {new_share:.0f}% of that {word}.")
    return out


def _top_movers(report: WeeklyReport) -> List[str]:
    if not report.movers:
        return []
    out = ["", "<b>Top movers</b> vs previous week"]
    for m in report.movers:
        out.append(f"• {_signed_money(m.delta)}  {_name(m.name)}")

    top_sum = sum(m.delta for m in report.movers)
    top_share = share_of(top_sum, report.product_move_total)
    if top_share is not None:
        out.append(
            f"These {len(report.movers)} are {top_share:.0f}% of the week's "
            f"product-revenue move"
        )
    return out
