"""A Postgres arm gated on a missing backfill is a failed run (L4-05).

With `meta.mirror_state.backfilled_at` NULL for `bronze.orders` the arm
compares nothing with KeyCRM, and it returned a WARN with `error` None: the run
was journalled as a success and reset `reconciliation_pg`'s age (from 40 h to
0 s in the reproduction), so the canary never paged a Postgres reconciled
against nothing. It now carries `error_message`, like the ClickHouse arm's
stale gate. Production has `backfilled_at` set, so nothing moves there.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.scheduler import BackgroundScheduler


@pytest.mark.asyncio
async def test_the_gated_run_is_failed_and_resolves_nothing():
    """Mutation: return `"error": None` from the gate again."""
    scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
    with patch("core.mirror_reconciliation.configured", return_value=True), \
         patch("core.pg.get_pool", new=AsyncMock()), \
         patch("core.pg.require_revision", new=AsyncMock()), \
         patch("core.mirror_reconciliation.fetch_watermarks", new=AsyncMock(
             return_value={"bronze.orders": {"backfilled_at": None}})):
        now = datetime.now(timezone.utc)
        result = await scheduler._reconcile_postgres(
            {}, {}, window_start=date(2026, 8, 7), window_end=date(2026, 11, 5),
            as_of=now, inflight_ids=set())

    assert [i.check_name for i in result["issues"]] == ["mirror_backfill_pending"]
    assert result["error"].startswith("mirror_backfill_pending: ")

    journal = AsyncMock(return_value=3)
    resolve = AsyncMock()
    with patch("core.dq_journal.journal_run", new=journal), \
         patch("core.alerting.resolve_group", new=resolve):
        await scheduler._persist_postgres_reconciliation(
            result, started_at=now, as_of=now,
            window_start=date(2026, 8, 7), window_end=date(2026, 11, 5))
    assert journal.await_args.kwargs["error_message"].startswith("mirror_backfill_pending")
    resolve.assert_not_awaited()
