"""The copy that carries the money must not be able to die quietly.

Before this, a broken `bronze.orders` mirror waited for the 07:30 comparison —
a full day in which the main copy could be dead with nobody told. Rule 3 of the
alerts charter: silence is not health, and a missing block is a failure.
"""
from datetime import datetime, timedelta, timezone

import pytest

from bot import canary
from bot.canary import check_mirror_freshness


def _payload(**tables):
    return {"mirrors": tables}


def _ok(age_seconds=600):
    return {
        "last_ok_at": "2026-08-29T06:32:12+00:00",
        "age_seconds": age_seconds,
        "failures_since_ok": 0,
        "failing": False,
    }


class TestTheVerdict:
    def test_a_fresh_mirror_is_quiet(self):
        failures, ages = check_mirror_freshness(_payload(**{"bronze.orders": _ok()}))
        assert failures == []
        assert ages == {"bronze.orders": 600}

    def test_missing_block_is_a_failure(self):
        """The endpoint publishes null rather than {} when it cannot read the
        watermarks, precisely so this cannot read green."""
        failures, ages = check_mirror_freshness({"status": "healthy"})
        assert [k for k, _ in failures] == ["mirror_block_missing"]
        assert ages == {}

    def test_null_payload_is_a_failure(self):
        failures, _ = check_mirror_freshness(None)
        assert [k for k, _ in failures] == ["mirror_block_missing"]

    def test_a_watched_table_absent_from_the_block_is_a_failure(self):
        failures, ages = check_mirror_freshness(_payload())
        assert [k for k, _ in failures] == ["mirror_missing:bronze.orders"]
        assert ages["bronze.orders"] is None

    def test_never_shipped_is_not_a_large_age(self):
        entry = _ok(age_seconds=None) | {"last_ok_at": None}
        failures, _ = check_mirror_freshness(_payload(**{"bronze.orders": entry}))
        assert [k for k, _ in failures] == ["mirror_never:bronze.orders"]

    def test_stale_beyond_the_limit(self):
        entry = _ok(age_seconds=9 * 3600)
        failures, _ = check_mirror_freshness(_payload(**{"bronze.orders": entry}))
        assert [k for k, _ in failures] == ["mirror_stale:bronze.orders"]
        assert "9h" in failures[0][1]

    def test_a_quiet_night_is_not_a_failure(self):
        """Measured on production: the orders watermark legitimately stands
        still from ~01:00 Kyiv until the 05:15 status refresh. A threshold
        tight enough to catch a dead mirror by age alone would page nightly."""
        entry = _ok(age_seconds=5 * 3600)
        failures, _ = check_mirror_freshness(_payload(**{"bronze.orders": entry}))
        assert failures == []

    def test_a_failing_mirror_alerts_while_still_inside_its_age_limit(self):
        """This is the whole reason the age limit can afford to be generous:
        an actively failing mirror is caught on the next sync tick."""
        entry = _ok(age_seconds=60) | {"failures_since_ok": 4, "failing": True}
        failures, _ = check_mirror_freshness(_payload(**{"bronze.orders": entry}))
        assert [k for k, _ in failures] == ["mirror_failing:bronze.orders"]
        assert "4×" in failures[0][1]

    def test_failing_and_stale_are_two_separate_verdicts(self):
        entry = _ok(age_seconds=30 * 3600) | {"failures_since_ok": 2, "failing": True}
        failures, _ = check_mirror_freshness(_payload(**{"bronze.orders": entry}))
        assert sorted(k for k, _ in failures) == [
            "mirror_failing:bronze.orders", "mirror_stale:bronze.orders",
        ]

    def test_honours_custom_thresholds(self):
        entry = _ok(age_seconds=100)
        failures, _ = check_mirror_freshness(
            _payload(**{"bronze.orders": entry}), max_age_s={"bronze.orders": 50},
        )
        assert [k for k, _ in failures] == ["mirror_stale:bronze.orders"]

    def test_clickhouse_is_deliberately_not_watched(self):
        """The optional store's daily window of silence was accepted and
        written down when step 4 shipped; paging on it would reopen a settled
        decision by accident."""
        assert not any(t.startswith("clickhouse.") for t in canary.MIRROR_MAX_AGE_S)


class TestTheThresholdIsMeasured:
    def test_it_clears_the_measured_night(self):
        """01:00 Kyiv to the 05:15 status refresh is ~4h15m of legitimate
        silence, ~6h if that job is ever skipped."""
        assert canary.MIRROR_MAX_AGE_S["bronze.orders"] >= 6 * 3600

    def test_it_is_still_far_below_the_daily_comparison(self):
        """The point is to close a 24-hour hole, not to move it."""
        assert canary.MIRROR_MAX_AGE_S["bronze.orders"] < 24 * 3600


class TestRunCanary:
    @staticmethod
    def _healthy():
        import httpx  # noqa: F401  (kept local; the module fixture builds one)
        from tests.unit.test_canary import _healthy_payload

        payload = _healthy_payload()
        payload["mirrors"] = {"bronze.orders": _ok()}
        return payload

    @pytest.mark.asyncio
    async def test_a_stale_mirror_makes_the_cycle_warn(self):
        import httpx
        from unittest.mock import patch

        payload = self._healthy()
        payload["mirrors"]["bronze.orders"]["age_seconds"] = 30 * 3600

        def handler(request):
            return httpx.Response(200, json=payload)

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary("https://x.test", client=c)

        assert result.severity == "warn"
        assert "mirror_stale:bronze.orders" in result.failure_keys
        assert result.mirror_ages["bronze.orders"] == 30 * 3600

    @pytest.mark.asyncio
    async def test_a_health_payload_without_mirrors_is_not_green(self):
        import httpx
        from unittest.mock import patch

        payload = self._healthy()
        del payload["mirrors"]

        def handler(request):
            return httpx.Response(200, json=payload)

        future = datetime.now(timezone.utc) + timedelta(days=60)
        cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
            with patch.object(canary, "_fetch_peer_cert", return_value=cert):
                result = await canary.run_canary("https://x.test", client=c)

        assert result.ok is False
        assert "mirror_block_missing" in result.failure_keys

    @pytest.mark.asyncio
    async def test_an_unreachable_dashboard_adds_no_mirror_noise(self):
        """Same rule the dq block already follows: one cause, one page."""
        import httpx
        from unittest.mock import patch

        def handler(request):
            raise httpx.ConnectError("down")

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
            with patch.object(canary, "_fetch_peer_cert",
                              side_effect=OSError("no tls")):
                result = await canary.run_canary("https://x.test", client=c)

        assert not any(k.startswith("mirror_") for k in result.failure_keys)


class TestTheAlertNamesALever:
    def test_a_mirror_failure_says_what_to_read(self):
        result = canary.CanaryResult(
            ok=False, severity="warn",
            failures=["mirror: bronze.orders has failed 4× since its last success"],
            failure_keys=["mirror_failing:bronze.orders"],
            mirror_ages={"bronze.orders": 60},
        )
        msg = canary.format_alert(result, "https://x.test")
        assert "meta.mirror_state" in msg
        assert "bronze.orders_mirror=" in msg

    def test_an_unreachable_dashboard_is_not_told_to_restart_first(self):
        result = canary.CanaryResult(
            ok=False, severity="critical",
            failures=["health request failed: timeout"],
            failure_keys=["health_unreachable"],
        )
        msg = canary.format_alert(result, "https://x.test")
        assert "curl /api/health from the VPS" in msg

    def test_a_green_result_needs_no_lever(self):
        result = canary.CanaryResult(ok=True, severity="ok")
        assert canary._what_to_do(result) is None
