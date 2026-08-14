"""Tests for the post-compact floor check.

Fixtures are the real Done: lines from /var/log/keycrm-compact.log. The twelve
weeks below are what the detector was built from, and the last of them is the
regression it exists to catch.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "compact_floor_check",
    Path(__file__).resolve().parents[2] / "deploy" / "compact_floor_check.py",
)
cfc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cfc)


# Verbatim from the production log, 31.05 → 09.08.
REAL_LOG = """
[2026-05-24T02:01:27+00:00] Done: 8.5G → 1.9G | disk: 27% → 29% used
[2026-05-31T02:01:26+00:00] Done: 3.3G → 66M | disk: 31% → 20% used
[2026-06-07T02:01:25+00:00] Done: 3.4G → 67M | disk: 25% → 20% used
[2026-06-14T02:01:25+00:00] Done: 4.0G → 69M | disk: 27% → 22% used
[2026-06-21T02:01:26+00:00] Done: 4.0G → 70M | disk: 28% → 23% used
[2026-06-28T02:01:25+00:00] Done: 4.0G → 71M | disk: 29% → 23% used
[2026-07-05T02:01:26+00:00] Done: 3.9G → 72M | disk: 29% → 24% used
[2026-07-12T02:01:25+00:00] Done: 3.8G → 74M | disk: 29% → 24% used
[2026-07-19T02:01:25+00:00] Done: 4.1G → 73M | disk: 30% → 25% used
[2026-07-26T02:01:26+00:00] Done: 4.0G → 74M | disk: 31% → 25% used
[2026-08-02T02:01:26+00:00] Done: 4.1G → 76M | disk: 31% → 26% used
"""

REGRESSION_WEEK = "[2026-08-09T02:01:25+00:00] Done: 4.4G → 81M | disk: 64% → 58% used\n"


class TestParsing:
    def test_reads_both_floors_from_the_real_log(self):
        floors = cfc.parse_floors(REAL_LOG)
        assert len(floors) == 11
        assert floors[0] == (pytest.approx(1945.6), 29.0)   # 1.9G in MB
        assert floors[-1] == (pytest.approx(76.0), 26.0)

    def test_units_are_normalised(self):
        floors = cfc.parse_floors(
            "Done: 9.9G → 2.0G | disk: 40% → 30% used\n"
            "Done: 9.9G → 500K | disk: 40% → 30% used\n"
        )
        assert floors[0][0] == pytest.approx(2048.0)
        assert floors[1][0] == pytest.approx(0.488, abs=0.01)

    def test_lines_that_are_not_summaries_are_ignored(self):
        assert cfc.parse_floors("=== WEEKLY COMPACT START ===\nStopping services...\n") == []


class TestEvaluation:
    def test_twelve_healthy_weeks_are_quiet(self):
        """Nothing in the real record before 09.08 should have said anything."""
        severity, msg = cfc.evaluate_floors(cfc.parse_floors(REAL_LOG))
        assert severity is None
        assert "steady" in msg

    def test_the_2026_08_09_regression_is_caught(self):
        """The disk floor moved 26% → 58% in one week and nothing noticed for
        five days. This is the whole reason the check exists."""
        severity, msg = cfc.evaluate_floors(cfc.parse_floors(REAL_LOG + REGRESSION_WEEK))
        assert severity == "CRITICAL"
        assert "26% → 58%" in msg
        assert "retained" in msg

    def test_the_db_floor_alone_stayed_innocent_that_week(self):
        """66 → 81 MB over twelve weeks is drift, not a step. The detector must
        not claim the database did it — that was the original error."""
        db_only = cfc.parse_floors(REAL_LOG + REGRESSION_WEEK)
        severity, msg = cfc.evaluate_floors([(db, 26.0) for db, _ in db_only])
        assert severity is None

    def test_a_retained_database_trips_the_ratio(self):
        history = cfc.parse_floors(REAL_LOG) + [(140.0, 26.0)]
        severity, msg = cfc.evaluate_floors(history)
        assert severity == "CRITICAL"
        assert "median" in msg

    def test_a_mild_rise_warns_before_it_is_critical(self):
        history = cfc.parse_floors(REAL_LOG) + [(92.0, 26.0)]
        severity, _ = cfc.evaluate_floors(history)
        assert severity == "WARN"

    def test_the_absolute_backstop_catches_slow_inflation(self):
        """A ratio alone cannot see creep that drags the median along with it."""
        crept = [(300.0, 25.0)] * 8 + [(310.0, 25.0)]
        severity, msg = cfc.evaluate_floors(crept)
        assert severity == "CRITICAL"
        assert "line" in msg

    def test_two_points_do_not_produce_a_verdict(self):
        """Inventing normality from two samples is how a threshold ends up
        calibrated against noise."""
        severity, msg = cfc.evaluate_floors([(70.0, 22.0), (71.0, 23.0)])
        assert severity is None
        assert "not enough history" in msg

    def test_a_falling_floor_is_never_an_alert(self):
        history = cfc.parse_floors(REAL_LOG) + [(40.0, 18.0)]
        severity, _ = cfc.evaluate_floors(history)
        assert severity is None
