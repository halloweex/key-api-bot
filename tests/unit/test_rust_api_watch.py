"""The watch on the Rust pilot (core/rust_api_watch.py): a line in the 09:00
digest, never a page — the owner's decision of 2026-11-25.

Is `ks-rust-api` there, and how much of its last day did it hand to the
Python app for a reason of its own. A hand-over by design — a route it does
not port, a filter it leaves to Python — is never counted against it."""
from __future__ import annotations

import ast
import asyncio
from pathlib import Path

import httpx

from core import data_quality, rust_api_watch as watch
from core.alerting import REGISTRY, Kind
from core.data_quality import Severity

REPO = Path(__file__).resolve().parents[2]
URL = "http://ks-rust-api:8090"
READY = watch.Answer(200, {"status": "ok"})


def _stats(requests: int, handed: dict, window_s: int = 86_400) -> watch.Answer:
    return watch.Answer(200, {"uptime_s": 90_000, "window_s": window_s,
                              "window": {"requests": requests, "handed_over": handed},
                              "since_start": {"requests": requests, "handed_over": handed}})


def _judge(readyz: watch.Answer, stats: watch.Answer = watch.Answer()):
    return watch.judge(watch.Facts(url=URL, readyz=readyz, stats=stats))


class TestTheNames:
    def test_each_is_a_registered_condition_with_words_and_a_lever(self):
        for name in watch.CONDITIONS:
            assert REGISTRY[name].kind is Kind.CONDITION, name
            assert data_quality.HUMAN_CHECK_NAMES[name], name
            assert any(name.startswith(prefix) for prefix, _ in data_quality.REMEDIATION), name

    def test_they_are_new_names(self):
        """The canary's keys never left the pull request; they are not reused."""
        assert not any(name.startswith("rust_api_") for name in watch.CONDITIONS)


class TestTheJudgement:
    def test_unset_it_files_nothing(self):
        assert watch.judge(watch.Facts()) == []

    def test_every_finding_is_a_warn_for_the_digest(self):
        cases = [_judge(watch.Answer(error="ConnectError: refused")),
                 _judge(watch.Answer(503, {"reason": "revision"})),
                 _judge(READY, watch.Answer(501, {"branch": "route"})),
                 _judge(READY, _stats(100, {"pool": 9}))]
        names = [found[0].check_name for found in cases]
        assert names == [watch.UNREACHABLE, watch.NOT_READY, watch.UNWATCHED,
                         watch.HANDOVER_SHARE]
        for found in cases:
            assert len(found) == 1 and found[0].severity is Severity.WARN
            assert watch.is_watch_finding(found[0])

    def test_not_ready_names_the_reason(self):
        [found] = _judge(watch.Answer(503, {"status": "unavailable", "reason": "database"}))
        assert "(database)" in found.description

    def test_hand_overs_by_design_are_not_counted(self):
        assert _judge(READY, _stats(100, {"route": 40, "line_level_filter": 30,
                                          "promocode_filter": 9, "input": 5,
                                          "source_without_gold_channel": 3})) == []

    def test_the_share_of_its_own_reasons(self):
        [found] = _judge(READY, _stats(100, {"pool": 4, "timeout": 3,
                                             "line_level_filter": 50}, window_s=7_200))
        assert found.count == 7
        assert found.description == (
            "ks-rust-api handed 7 of 100 requests to Python in the last 2 h "
            "for reasons of its own (pool 4, timeout 3)")
        # At the limit itself it is quiet: over 5 %, not 5 %.
        assert _judge(READY, _stats(100, {"pool": 5})) == []

    def test_too_few_requests_to_judge_a_share(self):
        assert _judge(READY, _stats(watch.MIN_REQUESTS - 1, {"database": 10})) == []


def _read(handler, monkeypatch, url):
    if url is None:
        monkeypatch.delenv(watch.URL_ENV, raising=False)
    else:
        monkeypatch.setenv(watch.URL_ENV, url)

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await watch.read_facts(client)
    return asyncio.run(run())


class TestTheRead:
    def test_unset_it_asks_nothing(self, monkeypatch):
        asked = []
        facts = _read(lambda r: asked.append(r) or httpx.Response(200), monkeypatch, None)
        assert asked == [] and facts == watch.Facts()

    def test_set_it_asks_readiness_then_counts(self, monkeypatch):
        asked = []

        def handler(request):
            asked.append(request.url.path)
            if request.url.path == "/readyz":
                return httpx.Response(200, json={"status": "ok"})
            return httpx.Response(200, json=_stats(0, {}).payload)

        facts = _read(handler, monkeypatch, URL + "/")
        assert asked == ["/readyz", "/stats"]
        assert facts.url == URL and facts.readyz.status == 200
        assert watch.judge(facts) == []

    def test_a_pilot_that_does_not_answer_is_a_fact_not_a_raise(self, monkeypatch):
        def handler(request):
            raise httpx.ConnectError("refused", request=request)

        facts = _read(handler, monkeypatch, URL)
        assert facts.readyz.error and facts.stats == watch.Answer()
        assert [i.check_name for i in watch.judge(facts)] == [watch.UNREACHABLE]


def test_the_integrity_job_files_it_and_never_pages_with_it():
    """Asked where the twins and the captures are judged, and left out of the
    list the job pages with — read from the code, not from a comment."""
    tree = ast.parse((REPO / "core" / "scheduler.py").read_text())
    called = {node.func.attr for node in ast.walk(tree)
              if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
              and isinstance(node.func.value, ast.Name)
              and node.func.value.id == "rust_api_watch"}
    assert {"read_facts", "judge", "is_watch_finding"} <= called
    paged = [node for node in ast.walk(tree)
             if isinstance(node, ast.Assign) and any(
                 isinstance(t, ast.Name) and t.id == "paged" for t in node.targets)]
    assert paged and all("is_watch_finding" in ast.unparse(node) for node in paged)
