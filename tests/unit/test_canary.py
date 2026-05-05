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


# ─── State machine: dedup + recovery ────────────────────────────────────────

def _failing_result():
    return canary.CanaryResult(
        ok=False, severity="critical", failures=["health returned HTTP 503"],
        http_code=503,
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


def test_format_recovery_mentions_cert_and_sync():
    msg = canary.format_recovery(_ok_result(), DASHBOARD)
    assert "recovered" in msg
    assert "cert 90d" in msg
    assert "sync 30s" in msg
