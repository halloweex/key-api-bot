"""The month badge's "likely" range and its Black Friday line (2026-11-18).

The badge on the revenue chart says what this month will come to: the actual
so far plus the stored LightGBM forecast of the days left. This module says
how sure that is, and what last year's Black Friday would add. Both are the
owner's approved choices after the team's analysis of the month-total
forecast (the artifact "Разбор прогноза выручки"), ported from the
uncertainty engineer's study as it was validated, not redesigned.

THE RANGE

An 80% range for the month total, made in the morning of day T from the days
before it. The days left, T to the month's end, are simulated:

- each past day's one-step ratio, e_d = revenue_d / the mean of d's weekday
  over its 8 most recent open days before d, is drawn from the 365 days
  before T in weekly blocks that start on T's weekday — every day left draws
  a past day of its own weekday, and a week's run of good or bad days stays
  together — centred on the pool's mean;
- times one level shift common to all the days left, lognormal, its sigma
  matched to the spread of every n-day window of the 730 days before T
  (n = days left) after what the blocks already explain. The scale is 1.0,
  chosen on 2025 before 2026 was looked at (0.5 was chosen after, and the
  audit set it aside);
- 4 000 draws with seed 0, so one day's data gives one range; the 10th and
  90th percentiles of the days left's simulated revenue over their 8-week
  weekday forecast are the range's two ratios.

The ratio bounds production's own forecast of the days left:
lo/hi = K + F × [q10, q90], K the month's actual through yesterday, F the
stored forecast from today to the month's end. On 2026 held out, with the
month totals of the LightGBM as it trains since 2026-11-18, that range held
the actual on 88% of mornings, with an interval score of 0.273 against 0.320
for the model's own empirical band (2025: 87%, 0.404 against 0.436). It does
not hold Black Friday: November–December 2025, 51%.

THE BLACK FRIDAY LINE

Promotions are in no calendar, and the range does not hold Black Friday:
27–29 November 2025 ran at ×5.1–5.8 of an ordinary day. So the badge says
separately what last year's would add, and neither the range nor the point
moves with it:

- an event is a run of days at 1.8× their weekday norm or more (the median
  of that weekday over the 8 weeks before, open days only, at least 4 of
  them); spike days with one ordinary day between them are one run;
- its size is its excess over the norm in "normal days" of its month (the
  month's mean norm);
- at T, last year's events of T's month that peaked at 4× or more add the
  size of their days that land, a year on, from T to the month's end, ×
  today's normal day (the median of each day left's weekday over its 8 most
  recent open days, averaged over the days left);
- a year on is Black Friday's own move for November's events (`year_shift`):
  364 days, the same weekday, or 371 when the last Friday of November falls
  a week later in the month, as in 2024, 2029, 2035 and 2040; any other
  month's move 364 days. A fixed 364 landed 2028's run, 23–25 November, on
  22–24 November 2029, a week before that year's Black Friday on the 30th,
  and the line was gone through the week of it (the forecast review,
  2026-11-20);
- only the days still ahead count: a day of the event that lands before T is
  in the month's actual already. The study counted the whole event while
  any of it was ahead, so on the morning of its second day the line added
  the first day a second time; the two differ on those mornings only;
- and only the days that land in T's month: a run that spilt into December,
  27 November–3 December, lands two of its days in December a year on,
  which November's total never holds; they were counted until the forecast
  review of 2026-11-20.

In the 900 days production reads (since June 2024, read on 2026-11-18) only
Black Friday reached 4×. Early 2024 reached 9.5×, but those runs are
artefacts — Sundays reopened in February 2024 against the tiny norms of the
weeks the shop was closed on them — and lie outside the read. The threshold
was set on one November, which is why the line stands beside the range and
never inside it.

A PLANNED PROMOTION'S LINE

A promotion entered in the calendar (revision 0039, `core/promotions.py`)
gets a line of its own on the badge, "if it goes like the past noticeable
ones" (`typical_promotion`):

- the runs above that lasted 2 days or more and started in the 730 days
  before T, Black Friday left out (a November run that peaked at 4× or more:
  its own line covers it);
- each run's net effect: revenue over the norm across the run and the 3 days
  either side of it, in normal days of its month — the days a promotion pulls
  sales forward from, or leaves short after it, count against it;
- the median of those, times today's normal day; nothing with fewer than 5
  runs.

It is inflated, by construction: a run is found only because it raised
revenue, so a promotion that moved nothing never enters the median. The page
says so beside the number, and neither the point nor the range moves with it.
On production's series to 2026-11-17 the 13 multi-day runs since March 2024
have a median of 4.40 normal days (≈ ₴0.46M at ₴105 148 a day); the 730 days
before 2026-11-18 hold 10 of them, with a median of 4.58.

CAUSAL BY CONSTRUCTION

A value for origin T reads the days before T and nothing else: the series
handed in ends the day before the origin, and its index `len(series)` is the
origin. Indices count from the series' first open day, as the study's
counted from its data's, so on the study's series this reproduced its
numbers to the bit, at every origin of 2025-03..2026-10 and at 2026-11-18,
the mornings inside an event aside (above);
`tests/unit/test_forecast_range.py` holds it to a transcription of the
study's code. Production reads `HISTORY_DAYS`, which puts every window it
uses 24 weeks past the series' start, where the weekday tables no longer
depend on where it starts.

Pure: numpy, the standard library and `core.black_friday`'s date, no I/O.
`PredictionService.get_forecast` reads the history and runs `outlook` on its
thread pool: `month_outlook` and `promotion_outlook` from one read, its
weekday tables and norms made once.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple

import numpy as np

from core.black_friday import black_friday_date

COVERAGE = 0.8
QUANTILES = (0.10, 0.90)
DRAWS = 4000
SEED = 0
WEEKS = 8                  # a weekday's table: its 8 most recent open days
WARMUP_DAYS = 56           # windows and blocks start 8 weeks into the series
POOL_DAYS = 365            # the one-step ratios drawn: the year before T
LEVEL_WINDOW_DAYS = 730    # the n-day windows the level term is matched to
LEVEL_SCALE = 1.0
MIN_BLOCK_STARTS = 20      # fewer weekly blocks to draw from: no range
SPIKE_RATIO = 1.8          # a promotion day, against its weekday norm
SPIKE_JOIN_DAYS = 2        # spike days this close are one run (one day between)
NORM_WEEKS = 8             # a day's weekday norm: the 8 calendar weeks before it
NORM_MIN_DAYS = 4          # ... from at least 4 open days
BF_MIN_PEAK = 4.0          # last year's events this strong are counted
SAME_WEEKDAY_YEAR = 364    # an event one year on, on its weekday (November's: `year_shift`)
TYPICAL_WINDOW_DAYS = 730  # a planned promotion's line: runs starting this far back
TYPICAL_MIN_SPAN_DAYS = 2  # ... that lasted this long, the first and last day included
TYPICAL_MARGIN_DAYS = 3    # ... counted with this many days either side
TYPICAL_MIN_RUNS = 5       # ... and no line from fewer of them
# What production reads: the 730-day windows, the 56-day warm-up before the
# first of them, and 114 days to spare, so every table it reads has 24 weeks
# behind it and nothing depends on where the read starts.
HISTORY_DAYS = 900


def month_end(day: date) -> date:
    """The last day of `day`'s month."""
    first_of_next = (day.replace(day=28) + timedelta(days=4)).replace(day=1)
    return first_of_next - timedelta(days=1)


def daily_series(
    rows: Iterable[Tuple[date, float]], first: date, last: date,
) -> np.ndarray:
    """One revenue a day from `first` to `last`, a date with no row at 0.

    Gold has no row for a day without an order, and the shop has been closed
    on a few, so a missing date is a closed day, as the study read it.
    """
    out = np.zeros(max((last - first).days + 1, 0))
    for day, value in rows:
        i = (day - first).days
        if 0 <= i < len(out):
            out[i] += float(value)
    return out


def history_before(
    rows: Iterable[Tuple[date, float]], origin: date, days: int = HISTORY_DAYS,
) -> Tuple[date, np.ndarray]:
    """The series the range is made from: `days` days to the eve of
    `origin`, starting at its first open day — the study's series started at
    its data's first day, and the warm-up counts from there."""
    first = origin - timedelta(days=days)
    series = daily_series(rows, first, origin - timedelta(days=1))
    opened = np.flatnonzero(series > 0)
    if not len(opened):
        return origin, series[:0]
    start = int(opened[0])
    return first + timedelta(days=start), series[start:]


# ── The weekday tables ──────────────────────────────────────────────────────

def weekday_tables(
    revenue: np.ndarray, first_weekday: int, weeks: int = WEEKS,
) -> Tuple[np.ndarray, np.ndarray]:
    """Row i, the morning of day i: per weekday, the mean and the median of
    its `weeks` most recent open days before day i, NaN before its first.

    One row more than the series: the last is the origin's own morning.
    A row changes only in the weekday of the day before it, so each mean is
    taken once, on the same values in the same order the study took it."""
    rows = len(revenue) + 1
    mean = np.full((rows, 7), np.nan)
    median = np.full((rows, 7), np.nan)
    seen: List[List[float]] = [[] for _ in range(7)]
    mean_now = np.full(7, np.nan)
    median_now = np.full(7, np.nan)
    for i in range(rows):
        mean[i] = mean_now
        median[i] = median_now
        if i < len(revenue) and revenue[i] > 0:
            w = (first_weekday + i) % 7
            seen[w].append(revenue[i])
            recent = seen[w][-weeks:]
            mean_now[w] = np.mean(recent)
            median_now[w] = np.median(recent)
    return mean, median


def weekday_norms(
    revenue: np.ndarray, weeks: int = NORM_WEEKS, min_days: int = NORM_MIN_DAYS,
) -> np.ndarray:
    """Each day's norm: the median of its weekday over the `weeks` calendar
    weeks before it, open days only, NaN from fewer than `min_days`."""
    out = np.full(len(revenue), np.nan)
    for i in range(len(revenue)):
        values = [revenue[j] for k in range(1, weeks + 1)
                  if (j := i - 7 * k) >= 0 and revenue[j] > 0]
        if len(values) >= min_days:
            out[i] = np.median(values)
    return out


# ── The range ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Band:
    """The simulated ratio of the days left to their forecast."""

    q_lo: float
    q_hi: float
    sigma_level: float
    days_left: int


def _window_ratios(
    revenue: np.ndarray, mean: np.ndarray, weekdays: np.ndarray,
    n: int, lo: int, hi: int,
) -> np.ndarray:
    """R* of every n-day window starting at lo..hi-1: its revenue over the
    forecast its first morning's table made for it."""
    out = np.full(max(hi - lo, 0), np.nan)
    steps = np.arange(n)
    for j, i0 in enumerate(range(lo, hi)):
        forecast = mean[i0, (weekdays[i0] + steps) % 7]
        if np.isnan(forecast).any():
            continue
        out[j] = revenue[i0:i0 + n].sum() / forecast.sum()
    return out


def simulate_band(
    revenue: np.ndarray, first: date, origin: date, *,
    tables: Optional[Tuple[np.ndarray, np.ndarray]] = None,
    draws: int = DRAWS, seed: int = SEED, pool: int = POOL_DAYS,
    level_window: int = LEVEL_WINDOW_DAYS, level_scale: float = LEVEL_SCALE,
) -> Optional[Band]:
    """The 10th and 90th percentiles of the days left's simulated revenue
    over their 8-week weekday forecast, at the morning of `origin`.

    `revenue` runs from `first` to the eve of `origin`. None when the series
    is too short to draw from: under `MIN_BLOCK_STARTS` weekly blocks, no
    window to match the level to, or a weekday never open."""
    i = len(revenue)
    if (origin - first).days != i:
        raise ValueError("the series must end the day before the origin")
    n = (month_end(origin) - origin).days + 1
    mean, _median = tables if tables is not None else weekday_tables(revenue, first.weekday())
    weekdays = (first.weekday() + np.arange(i + 1)) % 7
    forecast = mean[i, (origin.weekday() + np.arange(n)) % 7]
    if not np.isfinite(forecast).all():
        return None

    with np.errstate(invalid="ignore", divide="ignore"):
        ratios = revenue / mean[np.arange(i), weekdays[:i]]
    lo = max(WARMUP_DAYS, i - pool)
    starts = np.array([s for s in range(lo, i - 6)
                       if weekdays[s] == origin.weekday()
                       and np.isfinite(ratios[s:s + 7]).all()])
    windows = _window_ratios(revenue, mean, weekdays, n,
                             max(WARMUP_DAYS, i - level_window), i - n + 1)
    windows = windows[np.isfinite(windows) & (windows > 0)]
    if len(starts) < MIN_BLOCK_STARTS or len(windows) < 2:
        return None

    # The draws, in the study's order: the blocks, then the level shift.
    rng = np.random.default_rng(seed)
    blocks = -(-n // 7)
    pick = rng.choice(starts, size=(draws, blocks))
    idx = (pick[:, :, None] + np.arange(7)[None, None, :]).reshape(draws, 7 * blocks)[:, :n]
    recent = ratios[lo:i]
    drawn = ratios[idx] / recent[np.isfinite(recent)].mean()
    days = (drawn * forecast[None, :]).sum(1) / forecast.sum()
    # A draw of nothing but closed days is 0, whose log has no variance: the
    # study's backtest (`m_sim`) took the variance over the positive draws,
    # and where none is 0 that is its `sim_at` exactly.
    opened = days[days > 0]
    if len(opened) < 2:
        return None
    spread = np.var(np.log(windows)) - np.var(np.log(opened))
    if not np.isfinite(spread):
        return None
    sigma = float(np.sqrt(max(spread, 0.0)) * level_scale)
    total = days * np.exp(rng.normal(0, sigma, draws))
    q_lo, q_hi = np.quantile(total, QUANTILES)
    return Band(float(q_lo), float(q_hi), sigma, n)


# ── The Black Friday line ───────────────────────────────────────────────────

@dataclass(frozen=True)
class SpikeEvent:
    """A run of promotion days, the ordinary days between them included."""

    start: date
    end: date
    peak: float      # the highest ratio to the weekday norm in the run
    excess: float    # revenue over the norm across the run


def spike_events(
    revenue: np.ndarray, first: date, norms: Optional[np.ndarray] = None,
) -> List[SpikeEvent]:
    """Every run of days at `SPIKE_RATIO` × their weekday norm or more."""
    norms = weekday_norms(revenue) if norms is None else norms
    with np.errstate(invalid="ignore", divide="ignore"):
        ratio = np.where(norms > 0, revenue / norms, np.nan)
    runs: List[List[int]] = []
    for i in np.flatnonzero(np.isfinite(ratio) & (ratio >= SPIKE_RATIO)):
        if runs and i - runs[-1][-1] <= SPIKE_JOIN_DAYS:
            runs[-1].append(int(i))
        else:
            runs.append([int(i)])
    events = []
    for run in runs:
        span = range(run[0], run[-1] + 1)
        events.append(SpikeEvent(
            start=first + timedelta(days=run[0]),
            end=first + timedelta(days=run[-1]),
            peak=float(max(ratio[j] for j in span)),
            excess=float(sum(revenue[j] - norms[j] for j in span)),
        ))
    return events


@dataclass(frozen=True)
class BlackFriday:
    """Last year's big events of this month still ahead, in today's money."""

    extra: float
    size_days: float
    normal_day: float
    last_year_start: date
    last_year_end: date


def year_shift(origin: date) -> timedelta:
    """How far last year's events of `origin`'s month move to land a year on:
    November's as far as Black Friday moved — 364 days, or 371 when the last
    Friday of November falls a week later in the month — and any other
    month's `SAME_WEEKDAY_YEAR`, the same weekday."""
    if origin.month == 11:
        return black_friday_date(origin.year) - black_friday_date(origin.year - 1)
    return timedelta(days=SAME_WEEKDAY_YEAR)


def black_friday(
    revenue: np.ndarray, first: date, origin: date, *,
    tables: Optional[Tuple[np.ndarray, np.ndarray]] = None,
    norms: Optional[np.ndarray] = None,
) -> Optional[BlackFriday]:
    """What last year's events of `origin`'s month at `BF_MIN_PEAK` × or
    more add at today's normal day, counting their days that land, a year on
    (`year_shift`), from `origin` to the month's end; None when none does or
    last year's month is not in the series. `tables` and `norms` are
    `weekday_tables` and `weekday_norms` of `revenue`, made here when not
    given."""
    if (origin - first).days != len(revenue):
        raise ValueError("the series must end the day before the origin")
    year, month = origin.year - 1, origin.month
    if date(year, month, 1) < first:
        return None
    norms = weekday_norms(revenue) if norms is None else norms
    level = month_level(norms, first, year, month)
    shift, last = year_shift(origin), month_end(origin)
    # Each counted event's days that land from the origin to the month's end:
    # none when it has passed or lands in the next month. Before the event
    # and inside the month that is every day of it, summed in the study's
    # order, so the size is the study's to the bit.
    ahead: List[List[int]] = []
    for e in spike_events(revenue, first, norms):
        if (e.start.year, e.start.month) != (year, month) or not e.peak >= BF_MIN_PEAK:
            continue
        days = [j for j in range((e.start - first).days, (e.end - first).days + 1)
                if origin <= first + timedelta(days=j) + shift <= last]
        if days:
            ahead.append(days)
    size = float(sum(sum(revenue[j] - norms[j] for j in days) / level for days in ahead))
    if not ahead or not np.isfinite(size) or size <= 0:
        return None
    normal = normal_day(revenue, first, origin, tables=tables)
    extra = size * normal
    if not np.isfinite(extra) or extra <= 0:
        return None
    counted = [j for days in ahead for j in days]
    return BlackFriday(extra, size, normal,
                       first + timedelta(days=min(counted)), first + timedelta(days=max(counted)))


def month_level(norms: np.ndarray, first: date, year: int, month: int) -> float:
    """A month's normal day: the mean of its days' weekday norms, NaN when
    none is known — the unit an event is sized in."""
    in_month = [norms[i] for i in range(len(norms))
                if (d := first + timedelta(days=i)).year == year and d.month == month
                and norms[i] == norms[i]]
    return float(np.mean(in_month)) if in_month else float("nan")


def normal_day(
    revenue: np.ndarray, first: date, origin: date, *,
    tables: Optional[Tuple[np.ndarray, np.ndarray]] = None,
) -> float:
    """Today's normal day: the median of each day left's weekday over its 8
    most recent open days, averaged over the days left — what the Black
    Friday line and a planned promotion's line are priced at."""
    _mean, median = tables if tables is not None else weekday_tables(revenue, first.weekday())
    n = (month_end(origin) - origin).days + 1
    return float(np.mean(median[len(revenue), (origin.weekday() + np.arange(n)) % 7]))


# ── A planned promotion's line ──────────────────────────────────────────────

@dataclass(frozen=True)
class TypicalPromotion:
    """What a past noticeable promotion of 2 days and more added, typically,
    in today's money."""

    extra: float        # size_days × normal_day
    size_days: float    # the median run's net effect, in normal days of its month
    normal_day: float
    runs: int           # how many runs the median is of


def typical_promotion(
    revenue: np.ndarray, first: date, origin: date, *,
    tables: Optional[Tuple[np.ndarray, np.ndarray]] = None,
    norms: Optional[np.ndarray] = None,
) -> Optional[TypicalPromotion]:
    """The median net effect of the runs of `TYPICAL_MIN_SPAN_DAYS` days and
    more that started in the `TYPICAL_WINDOW_DAYS` before `origin`, Black
    Friday's left out, at today's normal day — or None from fewer than
    `TYPICAL_MIN_RUNS` runs.

    A run's net effect is its revenue over the norm across the run and
    `TYPICAL_MARGIN_DAYS` either side, the days with a norm only, in normal
    days of the month it started in. Inflated by construction: a run is
    found only because it raised revenue, so a promotion that moved nothing
    never enters the median (see "A PLANNED PROMOTION'S LINE" above).
    `tables` and `norms` as `black_friday` takes them."""
    if (origin - first).days != len(revenue):
        raise ValueError("the series must end the day before the origin")
    norms = weekday_norms(revenue) if norms is None else norms
    since = origin - timedelta(days=TYPICAL_WINDOW_DAYS)
    sizes: List[float] = []
    for e in spike_events(revenue, first, norms):
        if e.start < since or (e.end - e.start).days + 1 < TYPICAL_MIN_SPAN_DAYS:
            continue
        if e.start.month == 11 and e.peak >= BF_MIN_PEAK:
            continue     # Black Friday: its own line says what it adds
        lo = max((e.start - first).days - TYPICAL_MARGIN_DAYS, 0)
        hi = min((e.end - first).days + TYPICAL_MARGIN_DAYS, len(revenue) - 1)
        net = sum(revenue[j] - norms[j] for j in range(lo, hi + 1) if np.isfinite(norms[j]))
        sizes.append(float(net / month_level(norms, first, e.start.year, e.start.month)))
    if len(sizes) < TYPICAL_MIN_RUNS:
        return None
    size = float(np.median(sizes))
    normal = normal_day(revenue, first, origin, tables=tables)
    extra = size * normal
    if not np.isfinite(extra) or extra <= 0:
        return None
    return TypicalPromotion(extra, size, normal, len(sizes))


class _Read(NamedTuple):
    """`history_before`'s series and what every line is made of: its weekday
    tables and norms, made once (the forecast review, 2026-11-20: the
    promotion's line made them a second time, after the range)."""

    first: date
    revenue: np.ndarray
    tables: Tuple[np.ndarray, np.ndarray]
    norms: np.ndarray


def _read(rows: Sequence[Tuple[date, float]], origin: date) -> Optional[_Read]:
    """`rows` read for `origin` as every line reads them, or None when they
    hold no open day."""
    first, revenue = history_before(rows, origin)
    if len(revenue) == 0:
        return None
    return _Read(first, revenue, weekday_tables(revenue, first.weekday()),
                 weekday_norms(revenue))


def _typical(read: Optional[_Read], origin: date) -> Optional[TypicalPromotion]:
    if read is None:
        return None
    return typical_promotion(read.revenue, read.first, origin,
                             tables=read.tables, norms=read.norms)


def promotion_outlook(
    rows: Sequence[Tuple[date, float]], origin: date,
) -> Optional[TypicalPromotion]:
    """`typical_promotion` over Gold's (date, revenue) before `origin`, read
    as the range reads it (`history_before`)."""
    return _typical(_read(rows, origin), origin)


# ── What the forecast route answers ─────────────────────────────────────────

def forecast_left(
    predictions: Sequence[Dict[str, Any]], origin: date,
) -> Optional[float]:
    """F: the stored forecast of every day from `origin` to the month's end,
    or None when one of them is missing — a part of the days left is not a
    forecast of the month."""
    last = month_end(origin)
    by_day: Dict[date, float] = {}
    for p in predictions:
        day = date.fromisoformat(str(p["date"])[:10])
        if origin <= day <= last:
            by_day[day] = float(p["predicted_revenue"])
    if len(by_day) != (last - origin).days + 1:
        return None
    return sum(by_day[origin + timedelta(days=k)] for k in range(len(by_day)))


def month_outlook(
    rows: Sequence[Tuple[date, float]], origin: date, predicted_left: Optional[float],
) -> Dict[str, Optional[Dict[str, Any]]]:
    """`month_range` and `black_friday` of `/api/revenue/forecast`.

    `rows` are Gold's (date, revenue) before `origin`; `predicted_left` is
    `forecast_left` of the stored forecast. Either key is None when it cannot
    be said: no complete forecast of the days left, too short a history, or
    (for Black Friday) nothing big last year still ahead."""
    if not _a_forecast(predicted_left):
        return {"month_range": None, "black_friday": None}
    return _month_keys(rows, origin, predicted_left, _read(rows, origin))


def outlook(
    rows: Sequence[Tuple[date, float]], origin: date, predicted_left: Optional[float],
) -> Tuple[Dict[str, Optional[Dict[str, Any]]], Optional[TypicalPromotion]]:
    """`month_outlook` and `promotion_outlook` from one read, its weekday
    tables and norms made once: what the forecast route computes on the ML
    thread (`PredictionService._range_and_lines`)."""
    read = _read(rows, origin)
    return _month_keys(rows, origin, predicted_left, read), _typical(read, origin)


def _a_forecast(predicted_left: Optional[float]) -> bool:
    """Whether `predicted_left` is a forecast of the days left to bound."""
    return predicted_left is not None and bool(np.isfinite(predicted_left)) and predicted_left > 0


def _month_keys(
    rows: Sequence[Tuple[date, float]], origin: date, predicted_left: Optional[float],
    read: Optional[_Read],
) -> Dict[str, Optional[Dict[str, Any]]]:
    """`month_outlook`'s answer from a read already made."""
    if predicted_left is None or not _a_forecast(predicted_left) or read is None:
        return {"month_range": None, "black_friday": None}
    first, revenue = read.first, read.revenue
    # K: the month's actual through yesterday, from the same read.
    known = float(daily_series(rows, origin.replace(day=1),
                               origin - timedelta(days=1)).sum())
    band = simulate_band(revenue, first, origin, tables=read.tables)
    bf = black_friday(revenue, first, origin, tables=read.tables, norms=read.norms)
    return {
        "month_range": None if band is None else {
            "lo": round(known + predicted_left * band.q_lo, 2),
            "hi": round(known + predicted_left * band.q_hi, 2),
            "coverage": COVERAGE,
            "days_left": band.days_left,
        },
        "black_friday": None if bf is None else {
            "extra": round(bf.extra, 2),
            "total": round(known + predicted_left + bf.extra, 2),
            "size_days": round(bf.size_days, 2),
            "normal_day": round(bf.normal_day, 2),
            "last_year_start": bf.last_year_start.isoformat(),
            "last_year_end": bf.last_year_end.isoformat(),
            # Counted events start in the origin's month: November's is
            # Black Friday, any other a big promotion.
            "kind": "black_friday" if origin.month == 11 else "big_promotion",
        },
    }
