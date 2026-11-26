"""A planned promotion's line: what a past noticeable one added (revision 0039).

`core.forecast_range.typical_promotion` is the method the promotions
calendar's spec states, measured first on production's series in the
scratchpad's `fc/promo/net.py`: the net effect of each run of spike days,
over the run and three days either side, in normal days of its month. That
series is production revenue and stays out of this public repository, so the
promise is kept here as `tests/unit/test_forecast_range.py` keeps the range's:
`_study` below is that code transcribed — its loops and its order — with the
spec's three rules beside it (runs of 2 days and more, started in the 730
days before the origin, Black Friday left out), and the module must answer
what it answers, to the bit. `net.py` rounded each net effect to two places
for its print; the method does not, and neither does the transcription.
"""
from __future__ import annotations

import functools
import statistics as st
from datetime import date, timedelta

import numpy as np
import pytest

from core import forecast_range as fr
from tests.helpers.typical_promotion_series import D, FIRST, REVENUE


def _rows(before: date):
    return [(d, v) for d, v in sorted(REVENUE.items()) if d < before]


def _series(origin: date) -> np.ndarray:
    return fr.daily_series(_rows(origin), FIRST, origin - D(days=1))


@functools.lru_cache(maxsize=None)
def _study(origin: date, window: int = 730, min_days: int = 2, min_runs: int = 5,
           margin: int = 3):
    """`fc/promo/net.py` at `origin`, transcribed — the events, norms and
    series as `fc/promo/detect.py` made them (the port's own functions, as
    the study called them) — with the spec's rules: a run of `min_days` and
    more, started `window` days before the origin or later, not a November
    run that peaked at 4× (net.py's `bf`), and none from fewer than
    `min_runs`. The normal day is `tests/unit/test_forecast_range.py`'s
    transcription of the study's `base_vec(days, stat="median")`."""
    DATES = [FIRST + D(days=i) for i in range((origin - FIRST).days)]
    IDX = {d: i for i, d in enumerate(DATES)}
    s = np.array([REVENUE.get(d, 0.0) for d in DATES])
    first = DATES[0]
    n = fr.weekday_norms(s)
    ev = fr.spike_events(s, first, n)
    nets = []
    for e in ev:
        a, b = e.start, e.end
        days = (b - first).days - (a - first).days + 1
        if a < origin - D(days=window) or days < min_days:
            continue
        if a.month == 11 and e.peak >= 4:
            continue
        i0, i1 = (a - first).days - margin, (b - first).days + margin
        win = [j for j in range(max(i0, 0), min(i1, len(s) - 1) + 1) if np.isfinite(n[j])]
        net = sum(s[j] - n[j] for j in win)
        month = [n[j] for j in range(len(s))
                 if (first + timedelta(days=j)).month == a.month
                 and (first + timedelta(days=j)).year == a.year and np.isfinite(n[j])]
        nets.append(net / np.mean(month))
    if len(nets) < min_runs:
        return None

    def base_vec(days_, stat="median"):
        out = []
        for d in days_:
            v, k = [], 1
            while len(v) < 8 and k < 60:
                x = d - D(days=7 * k)
                k += 1
                if x < origin and x >= DATES[0] and s[IDX[x]] > 0:
                    v.append(s[IDX[x]])
            out.append(np.nan if not v else (np.mean(v) if stat == "mean" else np.median(v)))
        return np.array(out)

    me = fr.month_end(origin)
    left = [origin + D(days=k) for k in range((me - origin).days + 1)]
    normal = np.mean(base_vec(left, stat="median"))
    size = st.median(nets)
    return float(size), float(normal), float(size * normal), len(nets)


ORIGINS = [date(2025, 10, 6), date(2025, 11, 20), date(2025, 12, 1),
           date(2025, 12, 31), date(2026, 2, 1)]


class TestTheStudysMethod:
    @pytest.mark.parametrize("origin", ORIGINS, ids=str)
    def test_the_line_is_the_studys_to_the_bit(self, origin):
        typical = fr.typical_promotion(_series(origin), FIRST, origin)
        size, normal, extra, runs = _study(origin)

        assert typical is not None
        assert (typical.size_days, typical.normal_day, typical.extra, typical.runs) == (
            size, normal, extra, runs)

    def test_the_normal_day_is_the_black_friday_lines(self):
        """One price for both lines. Kills: a normal day computed another
        way for one of them."""
        origin = date(2025, 11, 20)
        series = _series(origin)
        bf = fr.black_friday(series, FIRST, origin)
        typical = fr.typical_promotion(series, FIRST, origin)
        assert bf is not None and typical is not None
        assert typical.normal_day == bf.normal_day == fr.normal_day(series, FIRST, origin)

    def test_the_production_read_answers_what_the_whole_series_does(self):
        """`HISTORY_DAYS` before the origin, as the range reads it: every run
        the line counts starts 730 days back, so its month and its norms lie
        inside the read."""
        origin = date(2026, 2, 1)
        whole = fr.typical_promotion(_series(origin), FIRST, origin)
        read = fr.promotion_outlook(_rows(origin), origin)
        assert read is not None and read == whole

    def test_a_series_that_does_not_end_the_day_before_is_refused(self):
        series = fr.daily_series(_rows(date(2025, 7, 1)), FIRST, date(2025, 6, 29))
        with pytest.raises(ValueError):
            fr.typical_promotion(series, FIRST, date(2025, 7, 1))


class TestWhichRunsCount:
    ORIGIN = date(2025, 12, 31)

    def _events(self):
        series = _series(self.ORIGIN)
        return series, fr.spike_events(series, FIRST)

    def test_black_friday_is_left_out_and_a_quieter_november_is_not(self):
        """Kills: the Black Friday filter dropped (its 13 normal days would be
        counted as a typical promotion) or widened to every November run."""
        with_bf = _study(self.ORIGIN)
        typical = fr.typical_promotion(_series(self.ORIGIN), FIRST, self.ORIGIN)
        assert typical is not None and with_bf is not None
        series, events = self._events()
        november = [e for e in events if e.start.month == 11]
        assert {(e.start, e.peak >= 4) for e in november} >= {
            (date(2024, 11, 8), False), (date(2024, 11, 29), True), (date(2025, 11, 28), True)}
        counted = [e for e in events
                   if e.start >= self.ORIGIN - D(days=730)
                   and (e.end - e.start).days >= 1
                   and not (e.start.month == 11 and e.peak >= 4)]
        assert typical.runs == len(counted)
        assert date(2024, 11, 8) in {e.start for e in counted}

    def test_one_day_is_not_a_promotion_here(self):
        """The ×3.1 of 2025-03-18 stands alone: at 1.8× a single day is mostly
        the noise of a shop's revenue. Kills: the length filter dropped."""
        _series_, events = self._events()
        single = [e for e in events if e.start == date(2025, 3, 18)]
        assert single and single[0].start == single[0].end
        assert _study(self.ORIGIN, min_days=1)[3] > fr.typical_promotion(
            _series(self.ORIGIN), FIRST, self.ORIGIN).runs

    def test_an_ordinary_day_inside_does_not_split_a_run(self):
        _series_, events = self._events()
        may = [e for e in events if e.start == date(2025, 5, 6)]
        assert may and may[0].end == date(2025, 5, 8)

    def test_the_window_is_730_days(self):
        """A run that started 731 days before the origin is out, one 730 days
        before is in. Kills: the window dropped, which would count every run
        the read holds, or measured from somewhere else."""
        start = date(2024, 2, 13)
        inside = start + D(days=730)
        outside = start + D(days=731)
        assert _study(inside)[3] == _study(outside)[3] + 1
        for origin in (inside, outside):
            typical = fr.typical_promotion(_series(origin), FIRST, origin)
            assert typical is not None and typical.runs == _study(origin)[3]

    def test_the_three_days_either_side_are_counted(self, monkeypatch):
        """A run's net effect takes in the three days either side — the
        slumps after the runs here lower it — and the module follows its
        constant. Kills: the margin dropped from the window."""
        series = _series(self.ORIGIN)
        with_margin = fr.typical_promotion(series, FIRST, self.ORIGIN)
        monkeypatch.setattr(fr, "TYPICAL_MARGIN_DAYS", 0)
        without = fr.typical_promotion(series, FIRST, self.ORIGIN)
        assert with_margin is not None and without is not None
        assert without.size_days == _study(self.ORIGIN, margin=0)[0]
        assert with_margin.size_days == _study(self.ORIGIN)[0]
        assert with_margin.size_days != without.size_days

    def test_fewer_than_five_runs_say_nothing(self):
        """On 1 August 2024 the 730 days hold four runs of 2 days and more:
        no line. Kills: the floor dropped."""
        origin = date(2024, 8, 1)
        assert _study(origin, min_runs=1)[3] < 5
        assert fr.typical_promotion(_series(origin), FIRST, origin) is None
        assert fr.promotion_outlook(_rows(origin), origin) is None

    def test_no_history_says_nothing(self):
        assert fr.promotion_outlook([], date(2025, 12, 1)) is None


class TestItIsMoney:
    def test_extra_is_size_times_the_normal_day(self):
        origin = date(2025, 12, 1)
        typical = fr.typical_promotion(_series(origin), FIRST, origin)
        assert typical is not None
        assert typical.extra == typical.size_days * typical.normal_day
        # Runs of ×2-3 over two or three days, slumps and margins counted:
        # a few normal days, never a Black Friday's dozen.
        assert 1.0 < typical.size_days < 6.0
