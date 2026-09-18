"""A CRITICAL from the Postgres and ClickHouse arms reaches a human.

Both arms passed `run_id=run_id` to `evidence_for_agent` without ever assigning
`run_id`. Every existing test ran them with nothing to report, so the alert
branch never executed: the first real CRITICAL would have raised NameError
before the page went out, skipped `_resolve_dq_layer`, and — because
`_run_dq_reconciliation` calls the Postgres arm without a guard — cost the
ClickHouse verdict behind it.

So these tests drive the branch that matters, with a finding in it.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.data_quality import IntegrityIssue, Severity
from core.scheduler import BackgroundScheduler

AS_OF = datetime(2026, 9, 18, 2, 30, tzinfo=timezone.utc)
START, END = date(2026, 6, 20), date(2026, 9, 18)

ARMS = [
    ("_persist_postgres_reconciliation", "reconciliation_pg"),
    ("_persist_ch_reconciliation", "reconciliation_ch"),
]


class _Conn:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *a):
        return False


class _Store:
    def connection(self):
        return _Conn()


def _critical():
    return {
        "issues": [IntegrityIssue(
            check_name="order_missing_in_store", table_name="orders",
            severity=Severity.CRITICAL, count=3, sample_ids=(11, 12, 13),
        )],
        "discrepancies": [],
        "error": None,
    }


async def _drive(method, result, *, persist):
    sched = BackgroundScheduler()
    sched._send_dq_alert_throttled = AsyncMock()
    sched._resolve_dq_layer = AsyncMock()
    with patch("core.data_quality.persist_run", new=persist), \
         patch("core.data_quality.machine_attempts_note", return_value=None), \
         patch("core.duckdb_store.get_store",
               new=AsyncMock(return_value=_Store())):
        await getattr(sched, method)(
            result, started_at=AS_OF, as_of=AS_OF,
            window_start=START, window_end=END,
        )
    return sched


@pytest.mark.parametrize("method,layer", ARMS)
@pytest.mark.asyncio
async def test_a_critical_is_sent_with_the_run_it_came_from(method, layer):
    sched = await _drive(method, _critical(), persist=lambda conn, **kw: 4242)

    sched._send_dq_alert_throttled.assert_awaited_once()
    args, kwargs = sched._send_dq_alert_throttled.await_args
    assert args[0] == layer
    assert kwargs["conditions"] == ["order_missing_in_store"]
    # The pointer the diagnostic agent follows back to the findings.
    assert kwargs["evidence"]["run_id"] == 4242
    assert kwargs["evidence"]["layer"] == layer
    sched._resolve_dq_layer.assert_awaited_once()


@pytest.mark.parametrize("method,layer", ARMS)
@pytest.mark.asyncio
async def test_a_failed_persist_still_pages(method, layer):
    """The ledger is the less important half: a DuckDB write that throws
    costs the pointer, never the alert."""
    def _boom(conn, **kw):
        raise RuntimeError("duckdb is locked")

    sched = await _drive(method, _critical(), persist=_boom)

    sched._send_dq_alert_throttled.assert_awaited_once()
    evidence = sched._send_dq_alert_throttled.await_args.kwargs["evidence"]
    assert "run_id" not in evidence
    sched._resolve_dq_layer.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_critical_on_postgres_does_not_cost_the_clickhouse_verdict():
    """`_run_dq_reconciliation` calls the arms one after the other; an
    exception out of the first is the second never running."""
    sched = BackgroundScheduler()
    sched._send_dq_alert_throttled = AsyncMock()
    sched._resolve_dq_layer = AsyncMock()
    layers = []

    def _persist(conn, **kw):
        layers.append(kw["layer"])
        return len(layers)

    with patch("core.data_quality.persist_run", new=_persist), \
         patch("core.data_quality.machine_attempts_note", return_value=None), \
         patch("core.duckdb_store.get_store",
               new=AsyncMock(return_value=_Store())):
        for method, _ in ARMS:
            await getattr(sched, method)(
                _critical(), started_at=AS_OF, as_of=AS_OF,
                window_start=START, window_end=END,
            )

    assert layers == ["reconciliation_pg", "reconciliation_ch"]
    sent = [c.args[0] for c in sched._send_dq_alert_throttled.await_args_list]
    assert sent == ["reconciliation_pg", "reconciliation_ch"]
