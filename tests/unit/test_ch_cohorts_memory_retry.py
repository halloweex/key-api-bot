"""A cohort read that ClickHouse refused for want of memory is asked again.

The server is shared, and its total memory limit is spent by other projects'
queries too: on 2026-10-06 the OvercommitTracker stopped a cohort read with
code 241, the tab fell back to DuckDB, and the clean week the read-fallback
switch waits for started again. Only 241 is retried, and only a bounded
number of times; everything else reaches the caller's fallback at once.
"""
import asyncio

import pytest

from core import ch_cohorts

REFUSED = RuntimeError(
    "ClickHouse HTTP 500: Code: 241. DB::Exception: Memory limit (total) "
    "exceeded: would use 1.93 GiB, maximum: 1.60 GiB. (MEMORY_LIMIT_EXCEEDED)")
OTHER = RuntimeError("ClickHouse HTTP 500: Code: 60. DB::Exception: Unknown table")


@pytest.fixture
def ch(monkeypatch):
    """`execute` answers from a script; `sleep` only records."""
    calls, slept = [], []
    script = []

    async def execute(sql, **_):
        calls.append(sql)
        step = script.pop(0)
        if isinstance(step, BaseException):
            raise step
        return step

    async def sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr("core.ch_common.execute", execute)
    monkeypatch.setattr(ch_cohorts.asyncio, "sleep", sleep)
    return script, calls, slept


def run(coro):
    return asyncio.run(coro)


def test_a_refusal_then_an_answer_answers(ch):
    script, calls, slept = ch
    script += [REFUSED, "2026-01\t3\n"]
    rows = run(ch_cohorts.fetch("SELECT ?", [1], (ch_cohorts.TEXT, ch_cohorts.INT)))
    assert rows == [("2026-01", 3)]
    assert len(calls) == 2 and slept == [1.0]


def test_every_attempt_refused_reaches_the_caller(ch):
    """Mutation: drop the `delay is None` exit — the loop would end with no
    answer and no raise, and the caller would read nothing as an answer."""
    script, calls, slept = ch
    script += [REFUSED] * 3
    with pytest.raises(RuntimeError, match="Code: 241"):
        run(ch_cohorts.fetch("SELECT 1", []))
    assert len(calls) == 1 + len(ch_cohorts.MEMORY_RETRY_DELAYS_S)
    assert slept == list(ch_cohorts.MEMORY_RETRY_DELAYS_S)


def test_any_other_clickhouse_error_is_not_retried(ch):
    script, calls, slept = ch
    script += [OTHER, "never reached"]
    with pytest.raises(RuntimeError, match="Code: 60"):
        run(ch_cohorts.fetch("SELECT 1", []))
    assert len(calls) == 1 and slept == []


def test_a_transport_failure_is_not_retried(ch):
    script, calls, slept = ch
    script += [TimeoutError("read timed out"), "never reached"]
    with pytest.raises(TimeoutError):
        run(ch_cohorts.fetch("SELECT 1", []))
    assert len(calls) == 1 and slept == []


def test_the_wait_is_bounded():
    """Two retries, four seconds of waiting. A refusal comes back in
    milliseconds (114 ms on 2026-10-06) and nothing slower is retried, so the
    read still answers well inside the middleware's 30 s."""
    assert ch_cohorts.MEMORY_RETRY_DELAYS_S == (1.0, 3.0)
