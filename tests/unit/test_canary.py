"""Tests for bot/canary.py — health probe, cert expiry, state machine."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from bot import canary
from bot.canary import CanaryState, run_canary


DASHBOARD = "https://ksanalytics.duckdns.org"


def _mock_transport(handler):
    """Build an httpx.AsyncClient backed by a MockTransport for handler."""
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _healthy_payload():
    return {
        "status": "healthy",
        "version": "1.2.3",
        "uptime_seconds": 100,
        "duckdb": {"status": "connected", "latency_ms": 5},
        "sync": {"status": "active", "seconds_since_sync": 30},
        "data_quality": {
            "integrity": {"last_success_at": "2026-08-08T19:00:00+03:00", "age_seconds": 1800},
            "reconciliation": {"last_success_at": "2026-08-08T05:00:00+03:00", "age_seconds": 52200},
        },
    }


# ─── check_health ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_check_health_returns_payload_on_200():
    def handler(request):
        return httpx.Response(200, json=_healthy_payload())

    async with _mock_transport(handler) as client:
        code, payload, err = await canary.check_health(
            f"{DASHBOARD}/api/health", client=client
        )
    assert code == 200
    assert payload["status"] == "healthy"
    assert err is None


@pytest.mark.asyncio
async def test_check_health_reports_timeout():
    def handler(request):
        raise httpx.ConnectTimeout("simulated timeout")

    async with _mock_transport(handler) as client:
        code, payload, err = await canary.check_health(
            f"{DASHBOARD}/api/health", client=client
        )
    assert code is None
    assert payload is None
    assert err and "timeout" in err.lower()


@pytest.mark.asyncio
async def test_check_health_reports_5xx_with_payload_none():
    def handler(request):
        return httpx.Response(503, content=b"<html>oops</html>")

    async with _mock_transport(handler) as client:
        code, payload, err = await canary.check_health(
            f"{DASHBOARD}/api/health", client=client
        )
    assert code == 503
    assert payload is None
    assert err is None


# ─── check_cert_expiry ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_check_cert_expiry_happy_path():
    future = datetime.now(timezone.utc) + timedelta(days=60)
    fake_cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}

    with patch.object(canary, "_fetch_peer_cert", return_value=fake_cert):
        days, err = await canary.check_cert_expiry("example.com")
    assert err is None
    assert 58 <= days <= 60


@pytest.mark.asyncio
async def test_check_cert_expiry_negative_when_expired():
    past = datetime.now(timezone.utc) - timedelta(days=2)
    fake_cert = {"notAfter": past.strftime("%b %d %H:%M:%S %Y GMT")}

    with patch.object(canary, "_fetch_peer_cert", return_value=fake_cert):
        days, err = await canary.check_cert_expiry("example.com")
    assert err is None
    assert days < 0


@pytest.mark.asyncio
async def test_check_cert_expiry_handles_unparseable_notafter():
    fake_cert = {"notAfter": "not a real date"}
    with patch.object(canary, "_fetch_peer_cert", return_value=fake_cert):
        days, err = await canary.check_cert_expiry("example.com")
    assert days is None
    assert err and "unparseable" in err


@pytest.mark.asyncio
async def test_check_cert_expiry_handles_socket_error():
    with patch.object(canary, "_fetch_peer_cert", side_effect=OSError("no route")):
        days, err = await canary.check_cert_expiry("example.com")
    assert days is None
    assert err and "OSError" in err


# ─── check_dq_freshness ─────────────────────────────────────────────────────

def _dq_payload(**layers):
    return {"data_quality": layers}


def test_dq_freshness_passes_when_both_layers_recent():
    failures, ages = canary.check_dq_freshness(_healthy_payload())
    assert failures == []
    assert ages == {"integrity": 1800, "reconciliation": 52200}


def test_dq_freshness_flags_stale_reconciliation():
    payload = _healthy_payload()
    payload["data_quality"]["reconciliation"]["age_seconds"] = 3 * 86400
    failures, ages = canary.check_dq_freshness(payload)
    keys = [k for k, _ in failures]
    assert keys == ["dq_stale:reconciliation"]
    assert "3d" in failures[0][1]
    assert ages["reconciliation"] == 3 * 86400


def test_dq_freshness_missing_block_is_a_failure():
    """The whole point: absence must not read green."""
    failures, ages = canary.check_dq_freshness({"status": "healthy"})
    assert [k for k, _ in failures] == ["dq_block_missing"]
    assert ages == {}


def test_dq_freshness_null_payload_is_a_failure():
    failures, _ = canary.check_dq_freshness(None)
    assert [k for k, _ in failures] == ["dq_block_missing"]


def test_dq_freshness_missing_layer_is_a_failure():
    payload = _dq_payload(integrity={"age_seconds": 60})
    failures, ages = canary.check_dq_freshness(payload)
    assert [k for k, _ in failures] == ["dq_missing:reconciliation"]
    assert ages["reconciliation"] is None


def test_dq_freshness_never_succeeded_is_a_failure():
    """A layer whose only runs all errored reports a null age, not a big one."""
    payload = _dq_payload(
        integrity={"last_success_at": None, "age_seconds": None},
        reconciliation={"last_success_at": None, "age_seconds": None},
    )
    failures, _ = canary.check_dq_freshness(payload)
    assert sorted(k for k, _ in failures) == [
        "dq_never:integrity", "dq_never:reconciliation",
    ]


def test_dq_freshness_honours_custom_thresholds():
    payload = _dq_payload(integrity={"age_seconds": 100})
    failures, _ = canary.check_dq_freshness(payload, max_age_s={"integrity": 50})
    assert [k for k, _ in failures] == ["dq_stale:integrity"]


# ─── run_canary integration ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_run_canary_all_green():
    def handler(request):
        return httpx.Response(200, json=_healthy_payload())

    future = datetime.now(timezone.utc) + timedelta(days=60)
    fake_cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}

    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=fake_cert):
            result = await run_canary(DASHBOARD, client=client)
    assert result.ok is True
    assert result.severity == "ok"
    assert result.failures == []
    assert result.health_status == "healthy"
    assert result.http_code == 200
    assert result.cert_days_remaining and result.cert_days_remaining > 50


@pytest.mark.asyncio
async def test_run_canary_flags_degraded_status_as_critical():
    payload = _healthy_payload()
    payload["status"] = "degraded"

    def handler(request):
        return httpx.Response(200, json=payload)

    future = datetime.now(timezone.utc) + timedelta(days=60)
    fake_cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}

    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=fake_cert):
            result = await run_canary(DASHBOARD, client=client)
    assert result.ok is False
    assert result.severity == "critical"
    assert any("degraded" in f for f in result.failures)


@pytest.mark.asyncio
async def test_run_canary_flags_non_200_as_critical():
    def handler(request):
        return httpx.Response(502)

    future = datetime.now(timezone.utc) + timedelta(days=60)
    fake_cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}

    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=fake_cert):
            result = await run_canary(DASHBOARD, client=client)
    assert result.severity == "critical"
    assert any("502" in f for f in result.failures)


@pytest.mark.asyncio
async def test_run_canary_flags_short_cert_as_critical():
    def handler(request):
        return httpx.Response(200, json=_healthy_payload())

    near = datetime.now(timezone.utc) + timedelta(days=5)
    fake_cert = {"notAfter": near.strftime("%b %d %H:%M:%S %Y GMT")}

    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=fake_cert):
            result = await run_canary(DASHBOARD, client=client)
    assert result.severity == "critical"
    assert any("expires in" in f for f in result.failures)


@pytest.mark.asyncio
async def test_run_canary_cert_failure_alone_is_warn():
    def handler(request):
        return httpx.Response(200, json=_healthy_payload())

    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", side_effect=OSError("no route")):
            result = await run_canary(DASHBOARD, client=client)
    assert result.severity == "warn"
    assert any("cert check failed" in f for f in result.failures)


@pytest.mark.asyncio
async def test_run_canary_flags_stale_reconciliation_as_warn():
    payload = _healthy_payload()
    payload["data_quality"]["reconciliation"]["age_seconds"] = 4 * 86400

    def handler(request):
        return httpx.Response(200, json=payload)

    future = datetime.now(timezone.utc) + timedelta(days=60)
    fake_cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}

    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=fake_cert):
            result = await run_canary(DASHBOARD, client=client)
    assert result.ok is False
    assert result.severity == "warn"
    assert result.failure_keys == ["dq_stale:reconciliation"]
    assert result.dq_ages["reconciliation"] == 4 * 86400


@pytest.mark.asyncio
async def test_run_canary_flags_health_payload_without_dq_block():
    payload = _healthy_payload()
    del payload["data_quality"]

    def handler(request):
        return httpx.Response(200, json=payload)

    future = datetime.now(timezone.utc) + timedelta(days=60)
    fake_cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}

    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=fake_cert):
            result = await run_canary(DASHBOARD, client=client)
    assert result.ok is False
    assert result.failure_keys == ["dq_block_missing"]


@pytest.mark.asyncio
async def test_run_canary_unreachable_does_not_add_dq_noise():
    """A dead dashboard reports one problem, not two."""
    def handler(request):
        raise httpx.ConnectTimeout("down")

    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", side_effect=OSError("no route")):
            result = await run_canary(DASHBOARD, client=client)
    assert result.severity == "critical"
    assert not any(k.startswith("dq_") for k in result.failure_keys)


# ─── State machine: dedup + recovery ────────────────────────────────────────

def _failing_result():
    return canary.CanaryResult(
        ok=False, severity="critical", failures=["health returned HTTP 503"],
        failure_keys=["health_http"], http_code=503,
    )


def _ok_result():
    return canary.CanaryResult(
        ok=True, severity="ok", failures=[], health_status="healthy",
        http_code=200, cert_days_remaining=90, sync_seconds_since=30,
    )


def test_state_first_failure_alerts():
    state = CanaryState(cooldown_s=3600)
    assert state.decide(_failing_result(), now=0) == "alert"


def test_state_repeat_failure_within_cooldown_silent():
    state = CanaryState(cooldown_s=3600)
    assert state.decide(_failing_result(), now=0) == "alert"
    assert state.decide(_failing_result(), now=60) is None
    assert state.decide(_failing_result(), now=3599) is None


def test_state_repeat_failure_after_cooldown_alerts_again():
    state = CanaryState(cooldown_s=3600)
    state.decide(_failing_result(), now=0)
    assert state.decide(_failing_result(), now=3600) == "alert"


def test_state_recovery_emits_recovery_then_silent():
    state = CanaryState(cooldown_s=3600)
    state.decide(_failing_result(), now=0)
    assert state.decide(_ok_result(), now=120) == "recovery"
    # Subsequent OKs are silent.
    assert state.decide(_ok_result(), now=180) is None


def test_state_no_alert_when_starting_healthy():
    state = CanaryState(cooldown_s=3600)
    assert state.decide(_ok_result(), now=0) is None


def test_state_alerts_again_after_recovery_then_failure():
    state = CanaryState(cooldown_s=3600)
    state.decide(_failing_result(), now=0)
    state.decide(_ok_result(), now=120)
    # New failure after recovery should alert immediately, ignoring old cooldown.
    assert state.decide(_failing_result(), now=200) == "alert"


# ─── State machine: per-key throttling ──────────────────────────────────────

def _keyed_result(*keys):
    return canary.CanaryResult(
        ok=False, severity="warn",
        failures=[f"problem {k}" for k in keys], failure_keys=list(keys),
    )


def test_state_new_problem_alerts_despite_another_cooldown():
    """A second, different problem must not be swallowed by the first's cooldown."""
    state = CanaryState(cooldown_s=3600)
    assert state.decide(_keyed_result("health_http"), now=0) == "alert"
    assert state.decide(_keyed_result("health_http"), now=60) is None
    assert state.decide(
        _keyed_result("health_http", "dq_stale:reconciliation"), now=120
    ) == "alert"


def test_state_same_key_suppressed_regardless_of_message_text():
    """Throttling keys on the problem, not on text that moves every cycle."""
    state = CanaryState(cooldown_s=3600)
    first = canary.CanaryResult(
        ok=False, severity="warn",
        failures=["last successful reconciliation run was 30h ago"],
        failure_keys=["dq_stale:reconciliation"],
    )
    later = canary.CanaryResult(
        ok=False, severity="warn",
        failures=["last successful reconciliation run was 31h ago"],
        failure_keys=["dq_stale:reconciliation"],
    )
    assert state.decide(first, now=0) == "alert"
    assert state.decide(later, now=900) is None


def test_state_resolved_key_alerts_again_without_full_recovery():
    """One problem clearing while another persists must not mute its return."""
    state = CanaryState(cooldown_s=3600)
    state.decide(_keyed_result("health_http", "dq_stale:reconciliation"), now=0)
    # reconciliation recovers; health still broken and still in cooldown
    assert state.decide(_keyed_result("health_http"), now=600) is None
    # reconciliation goes stale again well inside the original cooldown
    assert state.decide(
        _keyed_result("health_http", "dq_stale:reconciliation"), now=1200
    ) == "alert"


def test_state_unkeyed_failure_still_alerts():
    result = canary.CanaryResult(ok=False, severity="critical", failures=["boom"])
    state = CanaryState(cooldown_s=3600)
    assert state.decide(result, now=0) == "alert"
    assert state.decide(result, now=60) is None


# ─── Formatters ─────────────────────────────────────────────────────────────

def test_format_alert_includes_failures_and_extras():
    result = canary.CanaryResult(
        ok=False, severity="critical",
        failures=["status=degraded", "cert expires in 5d (<14)"],
        http_code=200, health_status="degraded", cert_days_remaining=5,
        sync_seconds_since=120,
    )
    msg = canary.format_alert(result, DASHBOARD)
    assert "Dashboard CRITICAL" in msg
    assert DASHBOARD in msg
    assert "status=degraded" in msg
    assert "cert expires in 5d" in msg
    assert "cert_days=5" in msg
    assert "sync_age=120s" in msg


def test_format_alert_includes_dq_ages():
    result = canary.CanaryResult(
        ok=False, severity="warn",
        failures=["data quality: last successful reconciliation run was 2d 6h ago (>30h)"],
        failure_keys=["dq_stale:reconciliation"],
        http_code=200, health_status="healthy",
        dq_ages={"integrity": 3600, "reconciliation": 194400},
    )
    msg = canary.format_alert(result, DASHBOARD)
    assert "Dashboard Warning" in msg
    assert "reconciliation run was 2d 6h ago" in msg
    assert "reconciliation_age=2d 6h" in msg
    assert "integrity_age=1h" in msg


def test_format_recovery_mentions_cert_and_sync():
    msg = canary.format_recovery(_ok_result(), DASHBOARD)
    assert "recovered" in msg
    assert "cert 90d" in msg
    assert "sync 30s" in msg
