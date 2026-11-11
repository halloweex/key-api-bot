"""A reconciliation page made of discrepancies carries its conditions (L4-01).

The 05:30 job's arms paged on CRITICAL with `conditions` built from the
issues alone, and a CRITICAL made of discrepancies has no issue: the page went
out with `conditions=[]`, the Gate journalled no series and kept no delivered
entry, and the clean run after it announced nothing resolved. Each CRITICAL
class now raises `recon_pg:<class>` / `recon_ch:<class>`, registered, and
`_resolve_dq_layer` holds the classes still found.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.alerting import is_condition
from core.data_quality import classify_order_discrepancies
from core.scheduler import BackgroundScheduler, discrepancy_conditions

D = date(2026, 10, 3)
PG = {101: dict(status_id=12, source_id=4, manager_id=None, buyer_id=1,
                grand_total=100.0, order_date=D, n_lines=1, qty=1, line_amount=100.0)}
KC = {101: dict(PG[101], status_id=19), 102: dict(PG[101])}


def _drift():
    return classify_order_discrepancies(PG, KC)


async def _persist(layer, discrepancies):
    scheduler = BackgroundScheduler.__new__(BackgroundScheduler)
    persist = {"reconciliation_pg": scheduler._persist_postgres_reconciliation,
               "reconciliation_ch": scheduler._persist_ch_reconciliation}[layer]
    raise_alert = AsyncMock(return_value=1)
    resolve = AsyncMock()
    with patch("core.dq_journal.journal_run", new=AsyncMock(return_value=7)), \
         patch("core.alerting.raise_alert", new=raise_alert), \
         patch("core.alerting.resolve_group", new=resolve), \
         patch("core.data_quality.machine_attempts_note", return_value=None):
        now = datetime.now(timezone.utc)
        await persist({"issues": [], "discrepancies": discrepancies, "error": None},
                      started_at=now, as_of=now, window_start=D, window_end=D)
    return raise_alert, resolve


class TestTheNames:
    def test_one_per_critical_class(self):
        assert discrepancy_conditions("reconciliation_pg", _drift()) == [
            "recon_pg:missing_in_dk", "recon_pg:status_drift"]
        assert discrepancy_conditions("reconciliation_ch", _drift()) == [
            "recon_ch:missing_in_dk", "recon_ch:status_drift"]

    def test_every_name_is_a_registered_condition(self):
        for layer in ("reconciliation_pg", "reconciliation_ch"):
            for name in discrepancy_conditions(layer, _drift()):
                assert is_condition(name), name

    def test_other_layers_raise_none(self):
        assert discrepancy_conditions("integrity", _drift()) == []


class TestThePageAndItsResolution:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("layer,prefix", [("reconciliation_pg", "recon_pg"),
                                              ("reconciliation_ch", "recon_ch")])
    async def test_the_page_names_its_classes_and_holds_them(self, layer, prefix):
        """Mutation: build `conditions` from the issues alone again — []."""
        raise_alert, resolve = await _persist(layer, _drift())
        assert raise_alert.await_args.kwargs["conditions"] == [
            f"{prefix}:missing_in_dk", f"{prefix}:status_drift"]
        assert resolve.await_args.kwargs["still_firing"] == [
            f"{prefix}:missing_in_dk", f"{prefix}:status_drift"]

    @pytest.mark.asyncio
    async def test_a_clean_run_resolves_them(self):
        raise_alert, resolve = await _persist("reconciliation_pg", [])
        raise_alert.assert_not_awaited()
        resolve.assert_awaited_once_with("dq:reconciliation_pg", still_firing=[])
