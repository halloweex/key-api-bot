"""A 05:30 run whose KeyCRM fetch raised leaves a failed run on each layer (L4-09).

The fetch is shared by both arms and runs before either; when it raised
(`KeyCRMAPIError('429')`) no arm had a result, so nothing was journalled on
`reconciliation_pg` or `reconciliation_ch` — the morning's check left no row
saying it had failed, only the canary's age a day later. Each layer this
process writes now gets a FAILED run carrying the error, which moves no age.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from core.exceptions import KeyCRMAPIError
from core.scheduler import BackgroundScheduler


async def _run(*, ch: bool):
    runs = []

    async def journal_run(**kw):
        runs.append(kw)
        return len(runs)

    scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
    with patch("core.reconciliation_io.keycrm_orders_in_window",
               new=AsyncMock(side_effect=KeyCRMAPIError("429 Too Many Attempts"))), \
         patch("core.dq_journal.journal_run", new=journal_run), \
         patch("core.mirror_reconciliation.configured", return_value=True), \
         patch("core.ch_common.configured", return_value=ch):
        out = await scheduler._run_dq_reconciliation()
    return out, runs


class TestTheFailedFetch:
    @pytest.mark.asyncio
    async def test_both_layers_are_journalled_failed(self):
        """Mutation: drop step 1b — no run on either layer."""
        out, runs = await _run(ch=True)
        assert "KeyCRMAPIError" in out["error"] and out["repair"] is None
        assert [r["layer"] for r in runs] == ["reconciliation_pg", "reconciliation_ch"]
        for r in runs:
            assert "429" in r["error_message"]
            assert r["issues"] == [] and r["discrepancies"] == []
            assert r["api_calls_used"] == 0

    @pytest.mark.asyncio
    async def test_only_the_layers_this_process_writes(self):
        """A host without ClickHouse never writes its layer, and a failed run
        there would read as an arm that exists."""
        _out, runs = await _run(ch=False)
        assert [r["layer"] for r in runs] == ["reconciliation_pg"]
