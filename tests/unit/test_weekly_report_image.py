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
    DOWN,
    MUTED,
    PAD,
    STATS_TOP,
    UP,
    H,
    W,
    _delta,
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

    def test_both_halves_of_the_card_are_used(self):
        """The headline and the three stats each occupy their own half.

        A layout regression that drops a block leaves its half flat white, and
        nothing else in the suite would notice.
        """
        img = _open(render_weekly_card(_report()))
        for name, box in (
            ("headline", (0, PAD, W, STATS_TOP - 60)),
            ("stats", (0, STATS_TOP, W, H - 40)),
        ):
            assert not _uniform(img.crop(box)), f"{name} half is empty"

class TestDelta:
    def test_a_fall_is_red_and_a_rise_is_green(self):
        assert _delta(-25.8) == ("▼ 25.8%", DOWN)
        assert _delta(12.3) == ("▲ 12.3%", UP)

    def test_a_tenth_of_a_percent_is_not_a_movement(self):
        """₴2,729 against ₴2,732 is not something to colour in."""
        text, colour = _delta(-0.1)
        assert text == "≈ flat"
        assert colour == MUTED

    def test_no_base_is_marked_rather_than_faked(self):
        assert _delta(None) == ("—", MUTED)


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

        monkeypatch.setattr("core.weekly_report_image._draw_stats", _explode)
        assert render_weekly_card(_report()) is None

    @pytest.mark.parametrize("name,overrides", [
        ("first week ever", dict(previous=None, year_ago=None, baseline_mean=None,
                                 baseline_sd=None, baseline_weeks=0)),
        ("a dead week", dict(current=WeekTotals(revenue=0, orders=0,
                                                new_customer_orders=0,
                                                repeat_orders=0))),
        ("no customer split", dict(current=WeekTotals(revenue=500_000, orders=100))),
        ("a week that tripled", dict(current=WeekTotals(
            revenue=9_999_999, orders=4_000, new_customer_orders=2_000,
            repeat_orders=2_000))),
    ])
    def test_thin_or_extreme_data_still_renders(self, name, overrides):
        data = render_weekly_card(_report(**overrides))
        assert data is not None, name
        assert _open(data).size == (W, H)
