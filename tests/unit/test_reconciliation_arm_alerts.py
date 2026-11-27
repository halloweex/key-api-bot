"""A CRITICAL from the Postgres and ClickHouse arms reaches a human.

Both arms passed `run_id=run_id` to `evidence_for_agent` without ever assigning
`run_id`. Every existing test ran them with nothing to report, so the alert
branch never executed: the first real CRITICAL would have raised NameError
before the page went out, skipped `_resolve_dq_layer`, and — because
`_run_dq_reconciliation` calls the Postgres arm without a guard — cost the
ClickHouse verdict behind it.

So these tests drive the branch that matters, with a finding in it, and read
what reached `core.alerting.raise_alert` and `resolve_group`, beside the run
the journal wrote on the test Postgres.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.data_quality import IntegrityIssue, Severity
from core.scheduler import BackgroundScheduler
from tests.helpers.dq_journal_pg import pool  # noqa: F401
from tests.helpers.recon_chains_alerts import alert_boundary
from tests.integration.pg_tab_seed import needs_postgres


START, END = date(2026, 6, 20), date(2026, 9, 18)

ARMS = [
    ("_persist_postgres_reconciliation", "reconciliation_pg"),
    ("_persist_ch_reconciliation", "reconciliation_ch"),
]


def _critical():
    return {
        "issues": [IntegrityIssue(
            check_name="order_missing_in_store", table_name="orders",
            severity=Severity.CRITICAL, count=3, sample_ids=(11, 12, 13),
        )],
        "discrepancies": [],
        "error": None,
    }


async def _persist(method, result):
    """The arm's run, started now: the journal stores its duration."""
    scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
    now = datetime.now(timezone.utc)
    await getattr(scheduler, method)(
        result, started_at=now, as_of=now, window_start=START, window_end=END,
    )


@needs_postgres
@pytest.mark.parametrize("method,layer", ARMS)
@pytest.mark.asyncio
async def test_a_critical_is_sent_with_the_run_it_came_from(pool, method, layer):  # noqa: F811
    """The page names the finding and points at the run the journal holds,
    and the layer's resolution keeps the finding firing."""
    with alert_boundary() as alerts:
        await _persist(method, _critical())

    (run_id,) = [r["run_id"] for r in await pool.fetch(
        "SELECT run_id FROM app.data_quality_runs WHERE layer = $1", layer)]
    (page,) = alerts.pages
    assert page.group == f"dq:{layer}"
    assert page.conditions == ["order_missing_in_store"]
    # The pointer the diagnostic agent follows back to the findings.
    assert page.evidence["run_id"] == run_id
    assert page.evidence["layer"] == layer
    assert alerts.held(f"dq:{layer}") == {"order_missing_in_store"}


@pytest.mark.parametrize("method,layer", ARMS)
@pytest.mark.asyncio
async def test_a_failed_persist_still_pages(method, layer):
    """The ledger is the less important half: a journal Postgres refuses
    costs the pointer, never the alert."""
    with patch("core.pg.get_pool", new=AsyncMock(side_effect=OSError("connection refused"))), \
         alert_boundary() as alerts:
        await _persist(method, _critical())

    (page,) = alerts.pages
    assert page.conditions == ["order_missing_in_store"]
    assert page.evidence["layer"] == layer
    assert "run_id" not in page.evidence
    assert alerts.held(f"dq:{layer}") == {"order_missing_in_store"}


@needs_postgres
@pytest.mark.asyncio
async def test_a_critical_on_postgres_does_not_cost_the_clickhouse_verdict(pool):  # noqa: F811
    """`_run_dq_reconciliation` calls the arms one after the other; an
    exception out of the first is the second never running."""
    with alert_boundary() as alerts:
        for method, _ in ARMS:
            await _persist(method, _critical())

    journalled = [r["layer"] for r in await pool.fetch(
        "SELECT layer FROM app.data_quality_runs ORDER BY run_id")]
    assert journalled == ["reconciliation_pg", "reconciliation_ch"]
    assert [page.group for page in alerts.pages] == [
        "dq:reconciliation_pg", "dq:reconciliation_ch"]
