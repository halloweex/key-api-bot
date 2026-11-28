"""A run in which a check could not look does not announce its recovery.

`_resolve_dq_layer` resolves every delivered page condition this run did not
find again. A check that raised — or said it is not watching — did not find its
conditions because it did not look, and "✅ Resolved" for a live
`silver_missing_rows` page after `silver_arc` crashed is a recovery nobody
verified. Those conditions are held as still firing; everything the run did
verify resolves as before. Found by the chain-2 review; the gap predates #208,
which only made the crash visible.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import textwrap
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from core import data_quality as dq
from core.data_quality import IntegrityIssue, Severity
from core.store import AnalyticsStore


@pytest_asyncio.fixture
async def store(tmp_path):
    s = AnalyticsStore()
    yield s


def _issue(name, severity=Severity.WARN):
    return IntegrityIssue(check_name=name, table_name="t", severity=severity, count=1)


class TestWhatTheScanCouldNotSee:
    def test_its_conditions_are_unverified(self):
        assert dq.unverified_conditions(["inventory_snapshot_continuity"], []) == [
            "inventory_snapshot_gaps"]

    def test_an_unwatched_inventory_calendar_leaves_the_gaps_unverified(self):
        assert dq.unverified_conditions([], [_issue("inventory_continuity_unwatched")]) == [
            "inventory_snapshot_gaps"]

    def test_unwatched_watermarks_leave_the_judged_freshness_unverified(self):
        """Every chain's watermarks have moved since step 14's S14-00 (a
        constant), so every one of them with a freshness threshold is held;
        the search index's cursor has none and is not."""
        assert dq.unverified_conditions([], [_issue("sync_watermarks_unwatched")]) == [
            "freshness_buyers", "freshness_categories", "freshness_expense_types",
            "freshness_managers", "freshness_offers", "freshness_orders",
            "freshness_products", "freshness_stocks"]

    def test_a_clean_run_has_nothing_unverified(self):
        assert dq.unverified_conditions([], [_issue("pg_headline_vs_line_items")]) == []


class TestTheResolution:
    def _still_firing(self, issues, unverified=()):
        from core.scheduler import BackgroundScheduler

        resolve = AsyncMock(return_value=0)
        with patch("core.alerting.resolve_group", resolve):
            asyncio.run(BackgroundScheduler()._resolve_dq_layer(
                "integrity", issues, None, unverified=unverified))
        return set(resolve.await_args.kwargs["still_firing"])

    def test_unverified_conditions_are_held_firing(self):
        still = self._still_firing([_issue("integrity_check_raised")],
                                   unverified=["pg_silver_missing_rows"])
        assert still == {"pg_silver_missing_rows"}

    def test_verified_criticals_still_hold_and_the_rest_resolve(self):
        still = self._still_firing([_issue("fk_orphan_order_products_order_id", Severity.CRITICAL)])
        assert still == {"fk_orphan_order_products_order_id"}


class TestTheMapCannotDrift:
    def test_every_guard_name_is_mapped_and_nothing_else(self):
        tree = ast.parse(textwrap.dedent(inspect.getsource(dq.check_internal_integrity)))
        guards = {n.args[0].value for n in ast.walk(tree)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                  and n.func.id == "guarded" and n.args and isinstance(n.args[0], ast.Constant)}
        assert guards and guards == set(dq.GUARDED_CHECK_CONDITIONS)

    @pytest.mark.parametrize("guard", sorted(dq.GUARDED_CHECK_CONDITIONS))
    def test_each_tuple_is_what_the_check_emits(self, guard):
        """Derived from the check itself, minus its own `*_unwatched` finding,
        which is a blindness report rather than a condition it verifies."""
        tree = ast.parse(textwrap.dedent(inspect.getsource(dq.check_internal_integrity)))
        call = next(n for n in ast.walk(tree)
                    if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                    and n.func.id == "guarded" and n.args[0].value == guard)
        check_name = next(n.func.id for n in ast.walk(call.args[1])
                          if isinstance(n, ast.Call) and isinstance(n.func, ast.Name))
        fn_tree = ast.parse(textwrap.dedent(inspect.getsource(getattr(dq, check_name))))
        emitted = set()
        for node in ast.walk(fn_tree):
            if isinstance(node, ast.keyword) and node.arg == "check_name" \
                    and isinstance(node.value, ast.Constant):
                emitted.add(node.value.value)
        emitted = {e for e in emitted if not e.endswith("_unwatched")}
        assert emitted and emitted == set(dq.GUARDED_CHECK_CONDITIONS[guard])


# ─── Through the job ──────────────────────────────────────────────────────────


@pytest.fixture
def integrity_job(monkeypatch):
    """The integrity job with the real Gate: its journal a list
    (`tests/journal_fake.py`), the snapshot calendar whole through yesterday,
    the watermarks and their copy refused, the Postgres twins and the chain
    invariants clean, and the admins' Telegram a list of what was sent."""
    from core import pg_chain_invariants as inv
    from core import pg_warehouse_dq as twins
    from core.alerting import reset_gate
    from core.scheduler import BackgroundScheduler
    from tests.helpers.warehouse_dq_facts import _facts
    from tests.journal_fake import install_journal

    # No archive to record the alerts in (it stands down without a DSN), no
    # agent task spooled into the tree, no Rust pilot to ask.
    monkeypatch.delenv("KS_PG_DSN", raising=False)
    monkeypatch.delenv("KS_ALERT_AGENT", raising=False)
    monkeypatch.delenv("KS_RUST_API_URL", raising=False)
    install_journal(monkeypatch)
    today = datetime.now().astimezone().date()
    first = today - timedelta(days=60)
    calendar = (first, frozenset(first + timedelta(days=n) for n in range(60)))
    refused = AsyncMock(side_effect=RuntimeError("no store in this test"))
    sent = []

    async def send(text, *_a, **_k):
        sent.append(text)
        return 1

    reset_gate()
    with patch("core.pg_inventory_write.read_snapshot_calendar",
               AsyncMock(return_value=calendar)), \
         patch("core.pg_chain_watermarks.read_values_and_stamps", refused), \
         patch("core.pg_frozen_metadata.frozen_sync_rows", refused), \
         patch.object(twins, "read_facts", AsyncMock(return_value=_facts())), \
         patch.object(inv, "read_facts", AsyncMock(return_value=inv.Facts())), \
         patch("bot.main.send_admin_message", new=AsyncMock(side_effect=send)):
        yield BackgroundScheduler(), sent
    reset_gate()


def _a_standing_page(condition):
    """`condition` delivered on the integrity layer and not yet resolved."""
    from core.alerting import raise_alert

    assert asyncio.run(raise_alert(
        f"page: {condition}", conditions=[condition],
        bucket=f"integrity:{condition}", group="dq:integrity")) == 1


def _raises(*_a, **_k):
    raise RuntimeError('relation "app.inventory_sku_history" does not exist')


class TestTheJobAnnouncesOnlyWhatItSaw:
    def test_a_check_that_raised_announces_no_recovery(self, integrity_job, monkeypatch):
        """Mutation: resolve without the scan's `raised_out`, or without the
        conditions it names."""
        from core.alerting import delivered_conditions

        scheduler, sent = integrity_job
        _a_standing_page("inventory_snapshot_gaps")
        monkeypatch.setattr(dq, "_inventory_snapshot_continuity_check", _raises)
        asyncio.run(scheduler._run_dq_integrity())
        assert len(sent) == 1, sent            # the page; no resolution
        assert delivered_conditions().get("inventory_snapshot_gaps") == "dq:integrity"

    def test_the_same_check_looking_and_finding_nothing_resolves_it(self, integrity_job):
        from core.alerting import delivered_conditions

        scheduler, sent = integrity_job
        _a_standing_page("inventory_snapshot_gaps")
        asyncio.run(scheduler._run_dq_integrity())
        assert len(sent) == 2, sent            # the page, then its resolution
        assert "inventory_snapshot_gaps" in sent[1]
        assert "inventory_snapshot_gaps" not in delivered_conditions()
