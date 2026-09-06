"""The control group's draw, which is the one thing a campaign cannot re-run.

A campaign is measured by comparing the people who got the message against the
people who deliberately did not. If the draw moves between the preview and the
freeze, or between two engines, or between two deploys, the comparison is
against a group that was never actually withheld — and the campaign has no
measurable lift at all, which is the only reason to withhold anyone.

So the properties tested here are not arithmetic, they are promises:
deterministic, process-independent, re-drawn per campaign, and uniform.
"""
from __future__ import annotations

import subprocess
import sys

import pytest

from core.sms_holdout import HOLDOUT, TARGET, assign_arm, holdout_bucket


class TestItIsStable:
    def test_the_same_customer_and_campaign_always_draw_the_same(self):
        first = [assign_arm(b, "aug-sale", 10) for b in range(500)]
        second = [assign_arm(b, "aug-sale", 10) for b in range(500)]
        assert first == second

    def test_it_survives_a_fresh_interpreter(self):
        """Python's own `hash()` is salted per process, so a split built on it
        would move every restart — a preview and the freeze that follows could
        disagree inside one sitting. SHA-1 is why this passes."""
        code = (
            "from core.sms_holdout import holdout_bucket;"
            "print([holdout_bucket(b, 'aug-sale') for b in range(20)])"
        )
        out = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True,
        ).stdout.strip()
        assert out == str([holdout_bucket(b, "aug-sale") for b in range(20)])

    def test_it_does_not_depend_on_the_engine_that_asked(self):
        """The whole reason this left SQL: DuckDB's `hash()` has no PostgreSQL
        equivalent, so the arm has to be decided before either store is
        involved. Nothing here can reach a database — pinned by import."""
        import core.sms_holdout as module

        source = module.__file__
        with open(source, encoding="utf-8") as handle:
            text = handle.read()
        for forbidden in ("duckdb", "asyncpg", "SELECT", "core.pg"):
            assert forbidden not in text


class TestItIsRedrawnPerCampaign:
    def test_the_same_person_is_not_always_the_one_withheld(self):
        """Otherwise the same unlucky customers never hear from us again."""
        withheld_aug = {b for b in range(2000) if assign_arm(b, "aug", 10) == HOLDOUT}
        withheld_sep = {b for b in range(2000) if assign_arm(b, "sep", 10) == HOLDOUT}

        assert withheld_aug and withheld_sep
        overlap = len(withheld_aug & withheld_sep) / len(withheld_aug)
        # Independent draws of ~10% each overlap ~10% of the time.
        assert overlap < 0.30, f"the two campaigns withheld the same people ({overlap:.0%})"


class TestTheProportionIsHonest:
    @pytest.mark.parametrize("pct", (5, 10, 25, 50))
    def test_about_the_requested_share_is_withheld(self, pct):
        n = 5000
        withheld = sum(assign_arm(b, "aug", pct) == HOLDOUT for b in range(n))
        share = 100 * withheld / n
        assert abs(share - pct) < 2.5, f"asked {pct}%, withheld {share:.1f}%"

    def test_zero_percent_withholds_nobody(self):
        """A real choice on the page, not a degenerate input."""
        assert all(assign_arm(b, "aug", 0) == TARGET for b in range(500))

    def test_a_negative_share_cannot_withhold_anyone(self):
        assert assign_arm(1, "aug", -5) == TARGET

    def test_buckets_stay_in_range(self):
        assert all(0 <= holdout_bucket(b, "aug") < 100 for b in range(1000))
