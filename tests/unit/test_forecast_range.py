"""The month badge's 80% range and Black Friday line (`core/forecast_range.py`).

The method is the uncertainty engineer's, validated on the team's harness
before it was ported: on production's own series the port reproduced the
study's `sim_at(T, lvl_scale=1.0)` band and its `allowance_bf` to the bit at
all 610 origins of 2025-03-01..2026-10-31 and at 2026-11-18. That series is
production revenue and stays out of this public repository, so the same
promise is kept here on a synthetic one: `_study` below is the study's code
transcribed — dates, dictionaries and loops as it wrote them — and the module
must answer what it answers, draw for draw.
"""
from __future__ import annotations

import functools
from datetime import date

import numpy as np
import pytest

from core import forecast_range as fr
from tests.helpers.forecast_range_series import D, FIRST, LAST, REVENUE


def _rows(before: date):
    return [(d, v) for d, v in sorted(REVENUE.items()) if d < before]


@functools.lru_cache(maxsize=None)
def _study(origin: date, B: int = 4000, seed: int = 0, lvl_scale: float = 1.0,
           pool: int = 365, lwin: int = 730, positive: bool = False,
           ahead_only: bool = False):
    """The study's `sim_at` and `allowance_bf` at `origin`, transcribed
    (`fc/ml/uncertainty/{core,ivlib,events,regime,today}.py`). `positive`
    takes the level term's variance over the positive draws, as its
    backtest (`simlib.m_sim`) did; `ahead_only` sizes an event by its days
    still ahead, the port's one change to the line."""
    DATES = [FIRST + D(days=i) for i in range((origin - FIRST).days)]
    IDX = {d: i for i, d in enumerate(DATES)}
    R = np.array([REVENUE.get(d, 0.0) for d in DATES])
    NDAY = len(DATES)
    T = origin

    def _w8(stat):
        out = np.full((NDAY, 7), np.nan)
        hist = {w: [] for w in range(7)}
        for i, d in enumerate(DATES):
            for w in range(7):
                v = hist[w][-8:]
                if v:
                    out[i, w] = np.mean(v) if stat == "mean" else np.median(v)
            if R[i] > 0:
                hist[d.weekday()].append(R[i])
        return out

    W8 = _w8("mean")
    WD = np.array([d.weekday() for d in DATES])
    E1 = R / W8[np.arange(NDAY), WD]

    def base_vec(days, stat="mean"):
        out = []
        for d in days:
            v, k = [], 1
            while len(v) < 8 and k < 60:
                x = d - D(days=7 * k)
                k += 1
                if x < T and x >= DATES[0] and R[IDX[x]] > 0:
                    v.append(R[IDX[x]])
            out.append(np.nan if not v else (np.mean(v) if stat == "mean" else np.median(v)))
        return np.array(out)

    def rstar(n):
        a = np.full(NDAY, np.nan)
        for i0 in range(56, NDAY - n + 1):
            f = W8[i0, (WD[i0] + np.arange(n)) % 7]
            if np.isnan(f).any():
                continue
            a[i0] = R[i0:i0 + n].sum() / f.sum()
        return a

    # sim_at
    rng = np.random.default_rng(seed)
    me = fr.month_end(T)
    days = [T + D(days=k) for k in range((me - T).days + 1)]
    n = len(days)
    f = base_vec(days)
    i = NDAY
    lo = max(56, i - pool)
    starts = np.array([s for s in range(lo, i - 6)
                       if WD[s] == T.weekday() and np.isfinite(E1[s:s + 7]).all()])
    k = -(-n // 7)
    pick = rng.choice(starts, size=(B, k))
    idx = (pick[:, :, None] + np.arange(7)[None, None, :]).reshape(B, 7 * k)[:, :n]
    e = E1[idx] / E1[lo:i][np.isfinite(E1[lo:i])].mean()
    Qs = (e * f[None, :]).sum(1) / f.sum()
    a = rstar(n)[max(56, i - lwin):i - n + 1]
    a = a[np.isfinite(a) & (a > 0)]
    sig = np.sqrt(max(np.var(np.log(a)) - np.var(np.log(Qs[Qs > 0] if positive else Qs)),
                      0.0)) * lvl_scale
    tot = Qs * np.exp(rng.normal(0, sig, B))
    lo_, hi_ = np.quantile(tot, [0.1, 0.9])

    # events.py, regime.py
    def norm_before(d):
        v = [R[IDX[x]] for kk in range(1, 9)
             if (x := d - D(days=7 * kk)) >= DATES[0] and R[IDX[x]] > 0]
        return float(np.median(v)) if len(v) >= 4 else np.nan

    NORM = {d: norm_before(d) for d in DATES}
    RATIO = {d: (R[IDX[d]] / NORM[d] if NORM[d] and NORM[d] > 0 else np.nan) for d in DATES}
    ev = []
    for d in [d for d in DATES if RATIO[d] == RATIO[d] and RATIO[d] >= 1.8]:
        if ev and (d - ev[-1][-1]).days <= 2:
            ev[-1].append(d)
        else:
            ev.append([d])
    EV = []
    for run in ev:
        s, t = run[0], run[-1]
        span = [s + D(days=j) for j in range((t - s).days + 1)]
        EV.append({"start": s, "end": t, "peak": max(RATIO[d] for d in span),
                   "excess": sum(R[IDX[d]] - NORM[d] for d in span)})
    y, m = T.year - 1, T.month
    lvl = np.mean([NORM[d] for d in DATES if d.year == y and d.month == m
                   and NORM[d] == NORM[d]] or [np.nan])

    def excess(e_):
        if not ahead_only:
            return e_["excess"]
        span = [e_["start"] + D(days=j) for j in range((e_["end"] - e_["start"]).days + 1)]
        return sum(R[IDX[d]] - NORM[d] for d in span if d + D(days=364) >= T)

    u = sum(excess(e_) / lvl for e_ in EV
            if e_["start"].year == y and e_["start"].month == m
            and e_["peak"] >= 4.0 and e_["end"] + D(days=364) >= T)
    normal = np.mean(base_vec(days, stat="median"))
    return float(lo_), float(hi_), float(u * normal), float(sig), float(f.sum())


ORIGINS = [date(2025, 3, 1), date(2025, 3, 5), date(2025, 3, 15), date(2025, 7, 1),
           date(2025, 11, 3), date(2025, 11, 20), date(2025, 11, 30), date(2025, 12, 30)]


class TestTheStudysProcedure:
    @pytest.mark.parametrize("origin", ORIGINS, ids=str)
    def test_the_band_and_the_allowance_are_the_studys_to_the_bit(self, origin):
        series = fr.daily_series(_rows(origin), FIRST, origin - D(days=1))
        band = fr.simulate_band(series, FIRST, origin)
        bf = fr.black_friday(series, FIRST, origin)
        lo, hi, allowance, sigma, _f = _study(origin, ahead_only=True)

        assert band is not None
        assert (band.q_lo, band.q_hi, band.sigma_level) == (lo, hi, sigma)
        assert (0.0 if bf is None else bf.extra) == allowance
        assert band.days_left == (fr.month_end(origin) - origin).days + 1

    @pytest.mark.parametrize("origin", [o for o in ORIGINS if o != date(2025, 11, 30)], ids=str)
    def test_outside_an_event_its_days_ahead_are_the_whole_of_it(self, origin):
        """The port's one change to the line: on a morning outside an event,
        the study's own allowance."""
        assert _study(origin)[2] == _study(origin, ahead_only=True)[2]

    def test_a_draw_of_closed_days_takes_the_backtests_variance(self):
        """On 31 December one day is left, a Wednesday, and 2025-01-01 was
        closed: some draws are that day alone, 0, and the study's `sim_at`
        has no level term there (the log of 0) — its range is NaN. The port
        takes the variance over the positive draws, as the backtest did."""
        origin = date(2025, 12, 31)
        with np.errstate(divide="ignore", invalid="ignore"):
            assert np.isnan(_study(origin)[0])
        series = fr.daily_series(_rows(origin), FIRST, origin - D(days=1))
        band = fr.simulate_band(series, FIRST, origin)
        lo, hi, _allowance, sigma, _f = _study(origin, positive=True)
        assert band is not None
        assert (band.q_lo, band.q_hi, band.sigma_level) == (lo, hi, sigma)

    @pytest.mark.parametrize("origin", [date(2025, 11, 20), date(2025, 7, 1)], ids=str)
    def test_the_production_read_answers_what_the_whole_series_does(self, origin):
        """`HISTORY_DAYS` before the origin, not the series from its first
        day: every window the range reads lies 24 weeks inside the read."""
        whole = fr.daily_series(_rows(origin), FIRST, origin - D(days=1))
        first, read = fr.history_before(_rows(origin), origin)
        assert first == origin - D(days=fr.HISTORY_DAYS)
        assert fr.simulate_band(read, first, origin) == fr.simulate_band(whole, FIRST, origin)
        assert fr.black_friday(read, first, origin) == fr.black_friday(whole, FIRST, origin)

    def test_one_days_data_is_one_range(self):
        series = fr.daily_series(_rows(date(2025, 11, 20)), FIRST, date(2025, 11, 19))
        assert (fr.simulate_band(series, FIRST, date(2025, 11, 20))
                == fr.simulate_band(series.copy(), FIRST, date(2025, 11, 20)))

    def test_the_range_brackets_the_forecast(self):
        series = fr.daily_series(_rows(date(2025, 7, 1)), FIRST, date(2025, 6, 30))
        band = fr.simulate_band(series, FIRST, date(2025, 7, 1))
        assert band is not None and band.q_lo < 1.0 < band.q_hi

    def test_a_series_that_does_not_end_the_day_before_is_refused(self):
        series = fr.daily_series(_rows(date(2025, 7, 1)), FIRST, date(2025, 6, 29))
        with pytest.raises(ValueError):
            fr.simulate_band(series, FIRST, date(2025, 7, 1))
        with pytest.raises(ValueError):
            fr.black_friday(series, FIRST, date(2025, 7, 1))

    def test_too_short_a_history_has_no_range(self):
        origin = FIRST + D(days=80)
        series = fr.daily_series(_rows(origin), FIRST, origin - D(days=1))
        assert fr.simulate_band(series, FIRST, origin) is None


class TestTheBlackFridayLine:
    def _at(self, origin):
        series = fr.daily_series(_rows(origin), FIRST, origin - D(days=1))
        return fr.black_friday(series, FIRST, origin)

    def test_last_years_black_friday_ahead_is_counted(self):
        bf = self._at(date(2025, 11, 20))
        assert bf is not None
        assert (bf.last_year_start, bf.last_year_end) == (date(2024, 11, 29), date(2024, 12, 1))
        assert bf.size_days > 8                       # three days at ×5 are ~12 extra days
        assert bf.extra == pytest.approx(bf.size_days * bf.normal_day)

    def test_it_counts_until_its_weekday_a_year_on(self):
        """2024-12-01 + 364 days is 2025-11-30, a Sunday as it was."""
        assert self._at(date(2025, 11, 30)) is not None
        # The run started in November: December's line does not count it.
        assert self._at(date(2025, 12, 1)) is None

    def test_inside_the_event_only_its_days_ahead_count(self):
        """2024-11-29..12-01 is 2025-11-28..30 a year on. On the morning of
        the 29th the 28th is in the month's actual already: the line counts
        the two days left, and on the 30th the last. The study counted all
        three on each of those mornings."""
        first_day = self._at(date(2025, 11, 28))
        second = self._at(date(2025, 11, 29))
        third = self._at(date(2025, 11, 30))
        assert first_day is not None and second is not None and third is not None
        assert (first_day.last_year_start, first_day.last_year_end) == (date(2024, 11, 29), date(2024, 12, 1))
        assert (second.last_year_start, second.last_year_end) == (date(2024, 11, 30), date(2024, 12, 1))
        assert (third.last_year_start, third.last_year_end) == (date(2024, 12, 1), date(2024, 12, 1))

        # Each day's excess over its norm, in normal days of November 2024.
        series = fr.daily_series(_rows(date(2025, 11, 30)), FIRST, date(2025, 11, 29))
        norms = fr.weekday_norms(series)
        november = [norms[(date(2024, 11, 1) - FIRST).days + k] for k in range(30)]
        level = np.mean([v for v in november if np.isfinite(v)])
        i = (date(2024, 11, 29) - FIRST).days
        days = [(series[i + k] - norms[i + k]) / level for k in range(3)]
        assert first_day.size_days == pytest.approx(sum(days))
        assert second.size_days == pytest.approx(days[1] + days[2])
        assert third.size_days == pytest.approx(days[2])
        assert _study(date(2025, 11, 29))[2] > second.extra

    def test_a_big_promotion_that_has_passed_is_not(self):
        """The ×5 of 2024-03-08 is ahead on 2025-03-05 and gone on the 15th."""
        assert self._at(date(2025, 3, 5)) is not None
        assert self._at(date(2025, 3, 15)) is None

    def test_ordinary_promotions_are_not_counted(self):
        """×2 in August 2024: a spike, under the 4× of the line."""
        assert self._at(date(2025, 8, 1)) is None

    def test_no_line_without_last_years_month(self):
        origin = date(2024, 1, 10)            # January 2023 starts before the series
        series = fr.daily_series(_rows(origin), date(2023, 2, 1), origin - D(days=1))
        assert fr.black_friday(series, date(2023, 2, 1), origin) is None

    def test_events_are_runs_of_spike_days(self):
        series = fr.daily_series(_rows(LAST + D(days=1)), FIRST, LAST)
        events = fr.spike_events(series, FIRST)
        bf = [e for e in events if e.start == date(2024, 11, 29)]
        assert len(bf) == 1 and bf[0].end == date(2024, 12, 1) and bf[0].peak >= 4.0


class TestWhatTheRouteAnswers:
    TODAY = date(2025, 11, 20)

    def _predictions(self, first: date, last: date, value: float = 125_000.0):
        return [{"date": (first + D(days=i)).isoformat(), "predicted_revenue": value}
                for i in range((last - first).days + 1)]

    def test_the_forecast_left_is_every_day_to_the_month_end(self):
        preds = self._predictions(self.TODAY - D(days=2), self.TODAY + D(days=40), 10.0)
        assert fr.forecast_left(preds, self.TODAY) == 110.0      # 20..30 November

    def test_a_missing_day_is_no_forecast_of_the_month(self):
        preds = [p for p in self._predictions(self.TODAY, date(2025, 11, 30))
                 if p["date"] != "2025-11-25"]
        assert fr.forecast_left(preds, self.TODAY) is None

    def test_the_range_is_k_plus_f_times_the_ratios(self):
        rows = _rows(self.TODAY)
        left = fr.forecast_left(self._predictions(self.TODAY, date(2025, 11, 30)), self.TODAY)
        assert left is not None
        out = fr.month_outlook(rows, self.TODAY, left)

        known = sum(v for d, v in rows if d >= date(2025, 11, 1))
        series = fr.daily_series(rows, FIRST, self.TODAY - D(days=1))
        band = fr.simulate_band(series, FIRST, self.TODAY)
        assert band is not None
        month_range = out["month_range"]
        assert month_range == {
            "lo": round(known + left * band.q_lo, 2),
            "hi": round(known + left * band.q_hi, 2),
            "coverage": 0.8, "days_left": 11,
        }
        assert month_range["lo"] < known + left < month_range["hi"]

    def test_black_friday_is_beside_the_point_and_not_in_it(self):
        rows = _rows(self.TODAY)
        left = 11 * 125_000.0
        out = fr.month_outlook(rows, self.TODAY, left)
        known = sum(v for d, v in rows if d >= date(2025, 11, 1))
        bf = out["black_friday"]
        assert bf is not None
        assert bf["total"] == pytest.approx(known + left + bf["extra"], abs=0.02)
        assert (bf["last_year_start"], bf["last_year_end"]) == ("2024-11-29", "2024-12-01")
        assert bf["kind"] == "black_friday"
        # The range is the same with or without the line.
        assert out["month_range"]["hi"] < known + left + bf["extra"]

    def test_a_big_promotion_of_another_month_is_named_so(self):
        """The ×5 of 2024-03-08, still ahead on 2025-03-05."""
        origin = date(2025, 3, 5)
        out = fr.month_outlook(_rows(origin), origin, 27 * 125_000.0)
        assert out["black_friday"] is not None
        assert out["black_friday"]["kind"] == "big_promotion"
        assert out["black_friday"]["last_year_start"] == "2024-03-08"

    def test_nothing_on_or_after_the_origin_is_read(self):
        rows = _rows(self.TODAY)
        later = rows + [(self.TODAY, 9e9), (self.TODAY + D(days=3), 9e9)]
        assert fr.month_outlook(later, self.TODAY, 1e6) == fr.month_outlook(rows, self.TODAY, 1e6)

    @pytest.mark.parametrize("left", [None, 0.0, float("nan")])
    def test_without_a_forecast_of_the_days_left_nothing_is_said(self, left):
        assert fr.month_outlook(_rows(self.TODAY), self.TODAY, left) == {
            "month_range": None, "black_friday": None}

    def test_no_history_says_nothing(self):
        assert fr.month_outlook([], self.TODAY, 1e6) == {
            "month_range": None, "black_friday": None}


class TestTheSeries:
    def test_a_day_without_a_row_is_closed(self):
        rows = [(date(2025, 1, 1), 5.0), (date(2025, 1, 3), 7.0), (date(2024, 12, 1), 99.0)]
        assert fr.daily_series(rows, date(2025, 1, 1), date(2025, 1, 4)).tolist() == [5.0, 0.0, 7.0, 0.0]

    def test_the_read_starts_at_its_first_open_day(self):
        rows = [(date(2025, 1, 5), 5.0), (date(2025, 1, 7), 7.0)]
        first, series = fr.history_before(rows, date(2025, 1, 9), days=10)
        assert first == date(2025, 1, 5) and series.tolist() == [5.0, 0.0, 7.0, 0.0]
