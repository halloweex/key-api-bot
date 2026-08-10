"""The weekly report card: a picture that must never cost the report.

It is decoration wrapped around numbers that are not, so the contract is narrow
and absolute — render something valid, or return None and let the text go out
alone. There is no third outcome, including on a host with no fonts, a week
with no history, or a value that breaks the arithmetic.
"""
from __future__ import annotations

import io
from datetime import date

import pytest

from core.weekly_report import ProductMove, WeekTotals, WeeklyReport
from core.weekly_report_image import (
    BAR,
    CALM,
    CHART_BOTTOM,
    CHART_TOP,
    DOWN,
    PAD,
    UP,
    H,
    W,
    _tone,
    render_weekly_card,
)


def _report(**overrides) -> WeeklyReport:
    base = dict(
        start=date(2026, 5, 18),
        end=date(2026, 5, 24),
        sales_type="retail",
        current=WeekTotals(revenue=968_638, orders=355,
                           new_customer_orders=170, repeat_orders=185),
        previous=WeekTotals(revenue=1_305_788, orders=478,
                            new_customer_orders=273, repeat_orders=205),
        year_ago=WeekTotals(revenue=603_194, orders=264),
        baseline_mean=1_100_920.0,
        baseline_sd=276_497.0,
        baseline_weeks=12,
        movers=[ProductMove(name="Differ & Deeper Cream",
                            current=19_966, previous=101_885)],
        product_move_total=-339_041.0,
        baseline_series=tuple(
            float(v) for v in (
                1_153_731, 1_582_423, 700_256, 948_058, 826_118, 908_980,
                1_004_110, 1_126_971, 875_484, 1_631_477, 1_147_640, 1_305_788,
            )
        ),
    )
    base.update(overrides)
    return WeeklyReport(**base)


def _open(data: bytes):
    from PIL import Image
    return Image.open(io.BytesIO(data)).convert("RGB")


def _uniform(region) -> bool:
    """Is this crop a single flat colour — i.e. nothing drawn there?"""
    return all(low == high for low, high in region.getextrema())


class TestRendering:
    def test_it_produces_one_still_image_at_card_size(self):
        img = _open(render_weekly_card(_report()))
        assert img.size == (W, H)
        assert getattr(img, "n_frames", 1) == 1

    def test_it_is_light(self):
        """A dark card is a hole burnt in a chat that is mostly white."""
        img = _open(render_weekly_card(_report()))
        assert img.getpixel((5, 5)) == (255, 255, 255)

    def test_it_stays_small_enough_to_send_every_week(self):
        assert len(render_weekly_card(_report())) < 400_000

    def test_every_band_of_the_card_is_used(self):
        """Headline, chart and stats each occupy their own third.

        A layout regression that drops a block leaves its band flat white, and
        nothing else in the suite would notice.
        """
        img = _open(render_weekly_card(_report()))
        for name, box in (
            ("headline", (0, PAD, W, CHART_TOP - 40)),
            ("chart", (0, CHART_TOP, W, CHART_BOTTOM)),
            ("stats", (0, CHART_BOTTOM + 100, W, H - 20)),
        ):
            assert not _uniform(img.crop(box)), f"{name} band is empty"


class TestChart:
    def _last_bar_colour(self, report) -> tuple:
        """The colour of the newest bar, sampled just above its baseline."""
        img = _open(render_weekly_card(report))
        return img.getpixel((W - PAD - 20, CHART_BOTTOM - 10))

    def test_the_newest_week_is_the_one_picked_out(self):
        """The whole point of the chart is where *this* week sits."""
        assert self._last_bar_colour(_report()) == CALM
        # ...and it is the only bar in the accent colour.
        img = _open(render_weekly_card(_report()))
        assert img.getpixel((PAD + 20, CHART_BOTTOM - 10)) == BAR

    def test_an_unusually_low_week_is_flagged_on_the_chart_itself(self):
        low = _report(current=WeekTotals(revenue=100_000, orders=40))
        assert self._last_bar_colour(low) == DOWN


class TestTone:
    def test_an_ordinary_week_is_painted_calm(self):
        """A 25% fall inside its own normal range is not an emergency.

        Colouring every dip red is the visual form of crying wolf, and the chip
        under the headline already carries the direction.
        """
        assert _tone(_report()) == CALM

    def test_an_unusually_low_week_is_painted_low(self):
        assert _tone(_report(current=WeekTotals(revenue=100_000, orders=40))) == DOWN

    def test_an_unusually_high_week_is_painted_high(self):
        assert _tone(_report(current=WeekTotals(revenue=2_400_000, orders=800))) == UP

    def test_no_history_means_no_verdict_to_colour(self):
        assert _tone(_report(baseline_sd=None, baseline_mean=None,
                             baseline_weeks=0)) == CALM


class TestDegradation:
    def test_a_host_without_dejavu_gets_no_card_and_no_crash(self, monkeypatch):
        """python:3.14-slim ships no fonts at all.

        The default bitmap face is not a fallback worth having: no Cyrillic and
        no ₴ would render this catalogue as a row of boxes.
        """
        monkeypatch.setattr("core.weekly_report_image._font_file",
                            lambda *a, **k: None)
        assert render_weekly_card(_report()) is None

    def test_a_drawing_failure_costs_the_card_and_nothing_else(self, monkeypatch):
        def _explode(*args, **kwargs):
            raise RuntimeError("no pixels today")

        monkeypatch.setattr("core.weekly_report_image._draw_chart", _explode)
        assert render_weekly_card(_report()) is None

    @pytest.mark.parametrize("name,overrides", [
        ("first week ever", dict(previous=None, year_ago=None, baseline_mean=None,
                                 baseline_sd=None, baseline_weeks=0,
                                 baseline_series=())),
        ("one prior week", dict(baseline_weeks=1, baseline_series=(500_000.0,),
                                baseline_sd=None)),
        ("a dead week", dict(current=WeekTotals(revenue=0, orders=0,
                                                new_customer_orders=0,
                                                repeat_orders=0))),
        ("no customer split", dict(current=WeekTotals(revenue=500_000, orders=100))),
        ("a flat history", dict(baseline_sd=0.0)),
    ])
    def test_thin_data_still_renders_or_declines_cleanly(self, name, overrides):
        data = render_weekly_card(_report(**overrides))
        if data is not None:
            assert _open(data).size == (W, H)
