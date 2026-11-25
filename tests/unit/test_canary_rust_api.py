"""The watchdog on the Rust pilot (bot/canary.py, check_rust_api).

Two questions: is `ks-rust-api` there, and how much of its window did it hand
to the Python app for a reason of its own. A hand-over by design — a route it
does not port, a filter it leaves to Python — is never counted against it."""
from __future__ import annotations

import asyncio

import httpx

from bot import canary
from core.alerting import REGISTRY

READY = (200, {"status": "ok"}, None)


def _stats(requests: int, handed: dict, window_s: int = 900):
    return (200, {"uptime_s": 4000, "window_s": window_s,
                  "window": {"requests": requests, "handed_over": handed},
                  "since_start": {"requests": requests, "handed_over": handed}}, None)


def test_every_key_it_emits_is_registered():
    for key in ("rust_api_unreachable", "rust_api_not_ready", "rust_api_handover_share"):
        assert key in REGISTRY, key


def test_a_container_that_does_not_answer():
    found, judged = canary.check_rust_api((None, None, "ConnectError: refused"), _stats(0, {}))
    assert [k for k, _ in found] == ["rust_api_unreachable"]
    assert not judged


def test_not_ready_names_the_reason():
    found, judged = canary.check_rust_api(
        (503, {"status": "unavailable", "reason": "revision"}, None), _stats(0, {}))
    assert found == [("rust_api_not_ready",
                      "ks-rust-api not ready (revision): it hands every request to Python")]
    assert not judged


def test_hand_overs_by_design_are_not_counted():
    found, judged = canary.check_rust_api(READY, _stats(
        100, {"route": 40, "line_level_filter": 30, "promocode_filter": 9, "input": 5,
              "source_without_gold_channel": 3}))
    assert found == [] and judged


def test_the_share_of_its_own_reasons():
    found, judged = canary.check_rust_api(READY, _stats(
        100, {"pool": 4, "timeout": 3, "line_level_filter": 50}, window_s=600))
    assert judged
    assert found == [("rust_api_handover_share",
                      "ks-rust-api handed 7 of 100 requests to Python in 10 min "
                      "(pool 4, timeout 3)")]
    # At the limit itself it is quiet: over 5 %, not 5 %.
    assert canary.check_rust_api(READY, _stats(100, {"pool": 5}))[0] == []


def test_too_few_requests_to_judge_a_share():
    found, judged = canary.check_rust_api(READY, _stats(
        canary.RUST_HANDOVER_MIN_REQUESTS - 1, {"database": 10}))
    assert found == [] and judged


def test_a_window_it_cannot_read_is_not_judged():
    for stats in ((None, None, "timeout after 10s"), (501, {"branch": "route"}, None),
                  (200, {"window": "?"}, None)):
        assert canary.check_rust_api(READY, stats) == ([], False)


def _probe(handler, monkeypatch, url: str | None):
    if url is None:
        monkeypatch.delenv(canary.RUST_API_URL_ENV, raising=False)
    else:
        monkeypatch.setenv(canary.RUST_API_URL_ENV, url)
    monkeypatch.setattr(canary, "_delivered_canary_keys", lambda: [])

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await canary.run_canary("http://dash", client=client)
    return asyncio.run(run())


def test_unset_it_asks_nothing(monkeypatch):
    asked = []

    def handler(request):
        asked.append(request.url.path)
        return httpx.Response(200, json={"status": "healthy"})

    result = _probe(handler, monkeypatch, None)
    assert asked == ["/api/health"]
    assert not any(k.startswith("rust_api_") for k in result.failure_keys)


def test_set_it_probes_both_paths_and_warns(monkeypatch):
    asked = []

    def handler(request):
        asked.append(request.url.path)
        if request.url.host == "dash":
            return httpx.Response(200, json={"status": "healthy"})
        if request.url.path == "/readyz":
            return httpx.Response(503, json={"status": "unavailable", "reason": "database"})
        return httpx.Response(200, json=_stats(0, {})[1])

    result = _probe(handler, monkeypatch, "http://ks-rust-api:8090/")
    assert sorted(asked) == ["/api/health", "/readyz", "/stats"]
    assert "rust_api_not_ready" in result.failure_keys
    assert result.severity in ("warn", "critical")
