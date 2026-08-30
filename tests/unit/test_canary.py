"""Tests for bot/canary.py — health probe, cert expiry, state machine."""
from __future__ import annotations

import pathlib
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from bot import canary
from bot.canary import run_canary


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
            "mirror_landing": {"last_success_at": "2026-08-08T07:30:00+03:00", "age_seconds": 43200},
        },
        # The Postgres copy of landing. Absent, `run_canary` reports
        # `mirror_block_missing` — that is the point of the block, and it is
        # exercised in tests/unit/test_mirror_freshness.py.
        "mirrors": {
            "bronze.orders": {
                "last_ok_at": "2026-08-08T19:25:00+03:00",
                "age_seconds": 300,
                "failures_since_ok": 0,
                "failing": False,
            },
        },
        # The alerting watching itself (step 03). Absent → failure, rule 3.
        "alerting": {
            "consecutive_transport_failures": 0,
            "last_delivery_at": 1723900000.0,
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


def test_dq_freshness_passes_when_all_layers_recent():
    failures, ages = canary.check_dq_freshness(_healthy_payload())
    assert failures == []
    assert ages == {"integrity": 1800, "reconciliation": 52200,
                    "mirror_landing": 43200}


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
    assert sorted(k for k, _ in failures) == [
        "dq_missing:mirror_landing", "dq_missing:reconciliation",
    ]
    assert ages["reconciliation"] is None


def test_dq_freshness_never_succeeded_is_a_failure():
    """A layer whose only runs all errored reports a null age, not a big one."""
    payload = _dq_payload(
        integrity={"last_success_at": None, "age_seconds": None},
        reconciliation={"last_success_at": None, "age_seconds": None},
        mirror_landing={"last_success_at": None, "age_seconds": None},
    )
    failures, _ = canary.check_dq_freshness(payload)
    assert sorted(k for k, _ in failures) == [
        "dq_never:integrity", "dq_never:mirror_landing",
        "dq_never:reconciliation",
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
    assert any("сертификат истекает" in f for f in result.failures)


@pytest.mark.asyncio
async def test_run_canary_cert_failure_alone_is_warn():
    def handler(request):
        return httpx.Response(200, json=_healthy_payload())

    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", side_effect=OSError("no route")):
            result = await run_canary(DASHBOARD, client=client)
    assert result.severity == "warn"
    assert any("TLS не проверился" in f for f in result.failures)


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


# ─── The state machine is gone (step 07) ───────────────────────────────────
#
# CanaryState — per-key cooldowns, recovery transitions — was the sixth and
# last private throttle. Its properties live on in the shared machinery and
# are tested there: "a new problem is never masked" is the Gate's
# per-bucket state (a changed failure set is a new compound bucket, which
# fires immediately); the standing-condition decay is the Gate's escalating
# cooldown (tests/unit/test_alert_gate.py); and recovery is resolve_group's
# per-key "✅ Resolved" with its delivered-first gate
# (tests/unit/test_alert_resolved.py) — which announces partial recoveries,
# something the all-or-nothing transition never could.


def test_the_state_machine_is_really_gone():
    """The consolidation pin: six throttles became one Gate."""
    import bot.canary as canary_module
    import bot.main as bot_main_module

    assert not hasattr(canary_module, "CanaryState")
    assert not hasattr(canary_module, "format_recovery")
    # bot.main no longer imports either — the job speaks to the Gate.
    assert not hasattr(bot_main_module, "CanaryState")
    assert not hasattr(bot_main_module, "format_recovery")


def test_format_alert_includes_failures_and_extras():
    result = canary.CanaryResult(
        ok=False, severity="critical",
        failures=["status=degraded", "cert expires in 5d (<14)"],
        http_code=200, health_status="degraded", cert_days_remaining=5,
        sync_seconds_since=120,
    )
    msg = canary.format_alert(result, DASHBOARD)
    assert "Дашборд лежит" in msg
    # Ссылку из тела убрали правкой владельца 30.08 («коротко»): у обоих
    # админов дашборд в закладках, а URL в каждом алерте — шум.
    assert "status=degraded" in msg
    assert "cert expires in 5d" in msg
    assert "cert 5д" in msg
    # sync-возраст из тела убран тем же коротким форматом — он в /api/health


def test_format_alert_includes_dq_ages():
    from bot.canary import CanaryResult, format_alert
    """Перевёрнут правкой владельца 30.08: детальные возрасты слоёв — шум в
    странице и живут в /api/health; тело несёт максимум пару ключевых цифр."""
    result = CanaryResult(
        ok=False, severity="warn",
        failures=["reconciliation: молчит 2d 6h"],
        failure_keys=["dq_stale:reconciliation"],
        dq_ages={"reconciliation": 2 * 86400 + 6 * 3600, "integrity": 1800},
    )
    msg = format_alert(result, DASHBOARD)
    assert "reconciliation: молчит 2d 6h" in msg     # сам провал — да
    assert "integrity" not in msg                     # приборная панель — нет
