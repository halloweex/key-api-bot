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
            # The same 05:30 job's Postgres half (DN-21).
            "reconciliation_pg": {"last_success_at": "2026-08-08T05:30:00+03:00", "age_seconds": 50400},
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
                    "mirror_landing": 43200, "reconciliation_pg": 50400}


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
        "dq_missing:reconciliation_pg",
    ]
    assert ages["reconciliation"] is None


def test_dq_freshness_never_succeeded_is_a_failure():
    """A layer whose only runs all errored reports a null age, not a big one."""
    payload = _dq_payload(
        integrity={"last_success_at": None, "age_seconds": None},
        reconciliation={"last_success_at": None, "age_seconds": None},
        mirror_landing={"last_success_at": None, "age_seconds": None},
        reconciliation_pg={"last_success_at": None, "age_seconds": None},
    )
    failures, _ = canary.check_dq_freshness(payload)
    assert sorted(k for k, _ in failures) == [
        "dq_never:integrity", "dq_never:mirror_landing",
        "dq_never:reconciliation", "dq_never:reconciliation_pg",
    ]


def test_dq_freshness_honours_custom_thresholds():
    payload = _dq_payload(integrity={"age_seconds": 100})
    failures, _ = canary.check_dq_freshness(payload, max_age_s={"integrity": 50})
    assert [k for k, _ in failures] == ["dq_stale:integrity"]


# ─── The Postgres half of the reconciliation is watched (DN-21) ─────────────
#
# `reconciliation_pg` is its own layer so that a Postgres comparison which
# stops cannot hide behind a fresh DuckDB one. These go through the default
# thresholds on purpose: the watch is the entry in DQ_MAX_AGE_S, and a test
# that passed its own `max_age_s` would stay green with the entry gone.

def _pg_layer_aged(age_seconds):
    """The healthy payload with only `reconciliation_pg`'s age changed."""
    payload = _healthy_payload()
    payload["data_quality"]["reconciliation_pg"]["age_seconds"] = age_seconds
    return payload


def test_reconciliation_pg_silent_31h_fails():
    failures, ages = canary.check_dq_freshness(_pg_layer_aged(31 * 3600))
    assert [k for k, _ in failures] == ["dq_stale:reconciliation_pg"]
    assert "31h" in failures[0][1]
    assert ages["reconciliation_pg"] == 31 * 3600


def test_reconciliation_pg_29h_passes():
    """One missed 05:30 is past 24h; the six hours of grace are what keep a
    late run from paging."""
    failures, _ = canary.check_dq_freshness(_pg_layer_aged(29 * 3600))
    assert failures == []


def test_reconciliation_pg_absent_from_the_block_fails():
    """Web stopped publishing the layer, the rest still there."""
    payload = _healthy_payload()
    del payload["data_quality"]["reconciliation_pg"]
    failures, ages = canary.check_dq_freshness(payload)
    assert [k for k, _ in failures] == ["dq_missing:reconciliation_pg"]
    assert ages["reconciliation_pg"] is None


def test_reconciliation_pg_never_succeeded_fails():
    """Every run errored — or, on a web with no Postgres, none was written."""
    failures, _ = canary.check_dq_freshness(_pg_layer_aged(None))
    assert [k for k, _ in failures] == ["dq_never:reconciliation_pg"]


@pytest.mark.asyncio
async def test_reconciliation_pg_pages_through_run_canary():
    """The same judgement reaches the result the Gate throttles on, as a warn
    and under its own key — never folded into the DuckDB half's."""
    payload = _pg_layer_aged(31 * 3600)

    def handler(request):
        return httpx.Response(200, json=payload)

    future = datetime.now(timezone.utc) + timedelta(days=60)
    fake_cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}

    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=fake_cert):
            result = await run_canary(DASHBOARD, client=client)
    assert result.severity == "warn"
    assert result.failure_keys == ["dq_stale:reconciliation_pg"]
    assert result.dq_ages["reconciliation_pg"] == 31 * 3600


def test_reconciliation_pg_keys_are_registered_conditions():
    """Alert keys are exact-match: an unregistered one would be delivered as
    an inert EVENT, with no lifecycle and no resolve."""
    from core.alerting import is_condition

    for key in ("dq_missing:reconciliation_pg", "dq_never:reconciliation_pg",
                "dq_stale:reconciliation_pg"):
        assert is_condition(key), key


def test_every_watched_layer_is_one_web_publishes():
    """A layer the canary watches but /api/health never reports would read as
    `dq_missing` on every probe, forever."""
    from core.data_quality import WATCHED_LAYERS

    assert set(canary.DQ_MAX_AGE_S) <= set(WATCHED_LAYERS)


# ─── A declared age limit has a ceiling (DN-05c) ────────────────────────────

def _declared(max_age_s: int, age: int) -> list:
    """The mirror keys filed for a derived table web declared `max_age_s` for,
    seen at `age` seconds."""
    payload = _healthy_payload()
    payload["mirrors"]["silver.orders"] = {
        "last_ok_at": "2026-09-18T03:00:00+03:00", "age_seconds": age,
        "failures_since_ok": 0, "failing": False, "max_age_s": max_age_s,
    }
    failures, _ = canary.check_mirror_freshness(payload)
    return [k for k, _ in failures]


def test_a_declaration_above_the_ceiling_is_judged_at_the_ceiling():
    """One wrong constant in web must not switch the derived-table page off:
    36,000 s declared pages at 10,800 s."""
    assert canary.DECLARED_MAX_AGE_CEILING_S == 10_800
    assert _declared(36_000, 10_801) == ["mirror_stale:silver.orders"]
    assert _declared(36_000, 10_800) == []


def test_a_declaration_under_the_ceiling_is_unchanged():
    assert _declared(5_400, 5_401) == ["mirror_stale:silver.orders"]
    assert _declared(5_400, 5_400) == []


def test_the_ceiling_does_not_tighten_what_web_declares_today():
    """Written out in bot/canary.py because nothing under bot/ may import
    core.pg*; this keeps the ceiling from quietly becoming the limit."""
    from core.pg_derivation import DERIVED_MAX_AGE_S

    assert DERIVED_MAX_AGE_S < canary.DECLARED_MAX_AGE_CEILING_S


def test_the_canarys_own_limits_are_not_capped():
    """The ceiling is for what web declares; bronze.orders keeps its measured
    eight hours even if a web build ever declared one for it."""
    payload = _healthy_payload()
    payload["mirrors"]["bronze.orders"].update(age_seconds=6 * 3600, max_age_s=36_000)
    failures, _ = canary.check_mirror_freshness(payload)
    assert failures == []


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
    assert any("cert expires in" in f for f in result.failures)


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
    assert "Dashboard DOWN" in msg
    # Ссылку из тела убрали правкой владельца 30.08 («коротко»): у обоих
    # админов дашборд в закладках, а URL в каждом алерте — шум.
    assert "status=degraded" in msg
    assert "cert expires in 5d" in msg
    assert "cert 5d" in msg
    # sync-возраст из тела убран тем же коротким форматом — он в /api/health


def test_format_alert_includes_dq_ages():
    from bot.canary import CanaryResult, format_alert
    """Перевёрнут правкой владельца 30.08: детальные возрасты слоёв — шум в
    странице и живут в /api/health; тело несёт максимум пару ключевых цифр."""
    result = CanaryResult(
        ok=False, severity="warn",
        failures=["reconciliation: молчит 2д 6ч"],
        failure_keys=["dq_stale:reconciliation"],
        dq_ages={"reconciliation": 2 * 86400 + 6 * 3600, "integrity": 1800},
    )
    msg = format_alert(result, DASHBOARD)
    assert "reconciliation: молчит 2д 6ч" in msg     # сам провал — да
    assert "integrity" not in msg                     # приборная панель — нет


@pytest.mark.asyncio
async def test_run_canary_warns_on_a_derivation_mode_web_did_not_understand():
    """The wiring, not only the judge: a KS_PG_DERIVE typo reaches a failure
    key and a warn, and the site is otherwise healthy."""
    payload = _healthy_payload()
    payload["derivation"] = {"mode": "piggyback",
                             "error": "KS_PG_DERIVE='owned' is not one of ('piggyback', 'own')"}

    def handler(request):
        return httpx.Response(200, json=payload)

    future = datetime.now(timezone.utc) + timedelta(days=60)
    fake_cert = {"notAfter": future.strftime("%b %d %H:%M:%S %Y GMT")}
    async with _mock_transport(handler) as client:
        with patch.object(canary, "_fetch_peer_cert", return_value=fake_cert):
            result = await run_canary(DASHBOARD, client=client)
    assert result.severity == "warn"
    assert result.failure_keys == ["derivation_mode_invalid"]
