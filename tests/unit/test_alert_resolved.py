"""Step 04: a condition that clears says so — once, and only if its firing
was ever heard.

Before this, exactly one condition in the system announced its own recovery
(the canary). Everything else went silent on clearing, and silence after an
alert is indistinguishable from the throttle holding it. The gate: a
"✅ resolved" may only follow a fired notice that reached someone — otherwise
the first thing a human hears about a condition is that it went away.
"""
from contextlib import ExitStack
from datetime import datetime, timezone
from typing import Any, Callable, NamedTuple
from unittest.mock import AsyncMock, patch

import pytest

from core.alerting import (
    AlertGate, delivered_conditions, is_condition, raise_alert, reset_gate, resolve_group,
)
from core.data_quality import Discrepancy, DiscrepancyClass, IntegrityIssue, Severity
from tests.helpers.alerts_journal_pg import alert_ledger, settle
from tests.helpers.alerts_telegram import ADMINS, telegram_at_the_transport
from tests.helpers.warehouse_dq_facts import _facts as clean_twin_facts
from tests.integration.pg_tab_seed import needs_postgres
from tests.journal_fake import install_journal


@pytest.fixture(autouse=True)
def _clean():
    reset_gate()
    yield
    reset_gate()


def _sent(n=2):
    return patch("bot.main.send_admin_message", new=AsyncMock(return_value=n))


def _archived():
    return patch("core.alert_archive.write_resolved_now",
                 new=AsyncMock(return_value=True))


class TestTheDeliveryGate:
    @pytest.mark.asyncio
    async def test_an_unheard_condition_resolves_silently(self):
        """Fired but kill-switched/undelivered → clearing sends nothing."""
        with patch("bot.main.send_admin_message", new=AsyncMock(return_value=0)):
            await raise_alert("x", conditions=["disk:WARN"],
                              bucket="disk:WARN", group="disk")
        with _sent() as send, _archived():
            assert await resolve_group("disk") == 0
        send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_heard_condition_resolves_once(self):
        with _sent():
            await raise_alert("боль", conditions=["disk:WARN"],
                              bucket="disk:WARN", group="disk")
        with _sent() as send, _archived():
            assert await resolve_group("disk") == 2
            text = send.await_args.args[0]
            assert "✅ Resolved:" in text and "disk:WARN" in text
            assert "stood" in text
        # The pop is the idempotence: the clean run repeats every cycle,
        # the notice must not.
        with _sent() as send2, _archived():
            assert await resolve_group("disk") == 0
        send2.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_events_never_resolve(self):
        """An OOM kill or a failed backup cannot 'go away'."""
        with _sent():
            await raise_alert(
                "x", conditions=["warehouse:backup_failed"],
                bucket="warehouse:backup_failed", group="warehouse",
            )
        with _sent() as send, _archived():
            assert await resolve_group("warehouse") == 0
        send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_still_firing_keys_stay_armed(self):
        """The DQ shape: three findings announced, next run still has one.
        (Three of `compare_table`'s conditions were the fixture until they
        were retired with it on 2026-11-19; unregistered, a key never
        resolves.)"""
        with _sent():
            await raise_alert(
                "x",
                conditions=["order_versions_missing", "order_versions_flooding",
                            "order_versions_stalled"],
                bucket="dq:fp1", group="dq:mirror_landing",
            )
        with _sent() as send, _archived() as rec:
            await resolve_group("dq:mirror_landing",
                                still_firing=["order_versions_stalled"])
            text = send.await_args.args[0]
            assert "order_versions_missing" in text
            assert "order_versions_flooding" in text
            assert "order_versions_stalled" not in text
            assert sorted(rec.call_args.args[0]) == [
                "order_versions_flooding", "order_versions_missing"]
        # The survivor clears on a later clean run.
        with _sent() as send2, _archived():
            await resolve_group("dq:mirror_landing")
            assert "order_versions_stalled" in send2.await_args.args[0]

    @pytest.mark.asyncio
    async def test_groups_do_not_cross(self):
        with _sent():
            await raise_alert("x", conditions=["disk:WARN"],
                              bucket="disk:WARN", group="disk")
        with _sent() as send, _archived():
            assert await resolve_group("memory") == 0
        send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_archive_closes_even_if_the_notice_reaches_nobody(self):
        """Delivery of the resolution is best-effort; the series closing is
        not — otherwise a kill-switched dev instance leaves firing ghosts."""
        with _sent():
            await raise_alert("x", conditions=["disk:WARN"],
                              bucket="disk:WARN", group="disk")
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=0)), _archived() as rec:
            assert await resolve_group("disk") == 0
        rec.assert_awaited_once()
        assert rec.await_args.args[0] == ["disk:WARN"]

    @pytest.mark.asyncio
    async def test_a_ledger_that_cannot_record_holds_the_notice_and_keeps_the_key(self):
        """Fire-and-forget left the series firing for good when the write
        missed its budget: the key was gone from the map and nothing retried.
        Now nothing is announced that the ledger does not hold, and the next
        healthy pass tries again."""
        with _sent():
            await raise_alert("x", conditions=["disk:WARN"],
                              bucket="disk:WARN", group="disk")
        with _sent() as send, patch("core.alert_archive.write_resolved_now",
                                    new=AsyncMock(return_value=False)):
            assert await resolve_group("disk") == 0
        send.assert_not_awaited()
        # The ledger is back: the same resolution is recorded and announced.
        with _sent(2) as send, _archived() as rec:
            assert await resolve_group("disk") == 2
        rec.assert_awaited_once()
        # And only once.
        with _sent(2) as send, _archived():
            assert await resolve_group("disk") == 0


class TestDeliveredMapDurability:
    def test_the_map_survives_a_restart(self, tmp_path):
        """A condition fired before a deploy must still resolve after it."""
        path = tmp_path / "gate.json"
        g1 = AlertGate(state_path=path)
        g1.note_delivered_conditions(["disk:WARN"], "disk", now=1000.0)

        g2 = AlertGate(state_path=path)
        taken = g2.take_resolved("disk", now=2000.0)
        assert taken == {"disk:WARN": 1000.0}

    def test_refiring_keeps_the_original_first_delivered(self):
        """'stood 26h' must measure from the first heard firing, not the
        latest reminder."""
        g = AlertGate()
        g.note_delivered_conditions(["disk:WARN"], "disk", now=1000.0)
        g.note_delivered_conditions(["disk:WARN"], "disk", now=90000.0)
        assert g.take_resolved("disk")["disk:WARN"] == 1000.0


class TestEmitterWiring:
    @pytest.mark.asyncio
    async def test_a_clean_disk_run_resolves_the_breach(self, tmp_path):
        """End-to-end through the scheduler job: breach → clean → notice."""
        from datetime import datetime, timezone
        from core.store import AnalyticsStore
        from core.scheduler import BackgroundScheduler

        store = AnalyticsStore()
        scheduler = BackgroundScheduler()

        def sample(pct, free):
            return {"sampled_at": datetime.now(timezone.utc),
                    "db_size_mb": 2480, "disk_pct_used": pct,
                    "disk_free_gb": free}

        # `_archived()` like every other test here that resolves for
        # real. Without it this reaches the actual ledger, and
        # `write_resolved_now` gives a pool it must *create* a one-second
        # budget: alone on a quiet host that is enough, but inside the
        # full suite on the deploy gate's box it is not, the write fails,
        # and the resolution is held back exactly as designed — so the
        # test failed on behaviour that was working.
        with patch("core.store.get_store",
                   AsyncMock(return_value=store)), \
             _archived(), \
             patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=2)) as send:
            with patch("core.disk_monitor.sample_disk_state",
                       return_value=sample(92.0, 6.0)):
                await scheduler._run_disk_watchdog()
            assert send.await_count == 1  # the breach
            with patch("core.disk_monitor.sample_disk_state",
                       return_value=sample(40.0, 60.0)):
                await scheduler._run_disk_watchdog()
            assert send.await_count == 2  # the recovery
            assert "✅ Resolved:" in send.await_args.args[0]
            assert "disk:CRITICAL" in send.await_args.args[0]

    @pytest.mark.asyncio
    async def test_a_disk_step_down_resolves_the_critical(self, tmp_path):
        """CRITICAL → WARN through the real job. The key carries its severity,
        so without this the CRITICAL series stands firing for as long as the
        WARN holds — after 2026-09-17 that was a page about bytes gone by
        morning, sitting in every digest tail."""
        from datetime import datetime, timezone
        from core.store import AnalyticsStore
        from core.scheduler import BackgroundScheduler

        store = AnalyticsStore()
        scheduler = BackgroundScheduler()

        def sample(pct, free):
            return {"sampled_at": datetime.now(timezone.utc),
                    "db_size_mb": 2480, "disk_pct_used": pct,
                    "disk_free_gb": free}

        with patch("core.store.get_store",
                   AsyncMock(return_value=store)), \
             _archived(), \
             patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=2)) as send:
            with patch("core.disk_monitor.sample_disk_state",
                       return_value=sample(92.0, 6.0)):
                await scheduler._run_disk_watchdog()
            # The page: the message carries the icon, not the key.
            assert any("🚨" in str(c.args[0]) for c in send.await_args_list)

            sent_before = send.await_count
            # Between the capacity lines: still WARN, no longer CRITICAL.
            with patch("core.disk_monitor.sample_disk_state",
                       return_value=sample(80.0, 20.0)):
                await scheduler._run_disk_watchdog()

            after = [str(c.args[0]) for c in send.await_args_list[sent_before:]]
            resolved = [m for m in after if "✅ Resolved:" in m]
            assert resolved, f"no resolve notice on the step down: {after!r}"
            assert "disk:CRITICAL" in resolved[0]
            assert "disk:WARN" not in resolved[0]

    @pytest.mark.asyncio
    async def test_a_disk_step_up_does_not_call_the_warn_resolved(self, tmp_path):
        """WARN → CRITICAL must not say the WARN cleared. It would be true of
        the key and false of the disk, at the worst possible moment."""
        from datetime import datetime, timezone
        from core.store import AnalyticsStore
        from core.scheduler import BackgroundScheduler

        store = AnalyticsStore()
        scheduler = BackgroundScheduler()

        def sample(pct, free):
            return {"sampled_at": datetime.now(timezone.utc),
                    "db_size_mb": 2480, "disk_pct_used": pct,
                    "disk_free_gb": free}

        with patch("core.store.get_store",
                   AsyncMock(return_value=store)), \
             _archived(), \
             patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=2)) as send:
            with patch("core.disk_monitor.sample_disk_state",
                       return_value=sample(80.0, 20.0)):
                await scheduler._run_disk_watchdog()
            sent_before = send.await_count
            with patch("core.disk_monitor.sample_disk_state",
                       return_value=sample(92.0, 6.0)):
                await scheduler._run_disk_watchdog()

            after = [str(c.args[0]) for c in send.await_args_list[sent_before:]]
            assert not any("✅ Resolved:" in m for m in after), (
                f"a step up announced something resolved: {after!r}"
            )

    @needs_postgres
    @pytest.mark.asyncio
    async def test_a_failed_dq_run_resolves_nothing(self, monkeypatch):
        """A run that produced no verdict proves nothing: the condition is
        unknown, not cleared. The Postgres arm pages a status drift, and the
        next arm fails before it compares anything: no notice goes out, the
        series stays firing and the page stays heard. Mutation: resolve the
        layer on a run that carries an error."""
        install_journal(monkeypatch)
        telegram = telegram_at_the_transport(monkeypatch)
        async with alert_ledger(monkeypatch) as ledger:
            await _reconciliation("reconciliation_pg", ([], [_discrepancy(_DRIFT, _CRIT)]))
            await settle()
            paged = list(telegram.messages)
            await _reconciliation("reconciliation_pg", ([], []), error="KeyCRM fetch failed")
            await settle()
            events = await ledger.events()
            series = await ledger.series()

        assert sorted(int(p["chat_id"]) for p in paged) == sorted(ADMINS)
        assert not any("recon_pg:status_drift" in p["text"]
                       for p in telegram.messages[len(paged):])
        assert events == [("recon_pg:status_drift", "fired", len(ADMINS))]
        assert series == [("recon_pg:status_drift", "firing", 1)]
        assert delivered_conditions() == {"recon_pg:status_drift": "dq:reconciliation_pg"}

    @needs_postgres
    @pytest.mark.asyncio
    async def test_a_warn_downgrade_clears_the_page_condition(self, monkeypatch):
        """CRITICAL → WARN: the page-condition is announced cleared, by its
        name, to each admin, though the finding stands and the journal (the
        digest's lane) still carries it — the charter's lane split.
        Mutation: hold every finding a run still has, whatever its severity."""
        journal = install_journal(monkeypatch)
        telegram = telegram_at_the_transport(monkeypatch)
        async with alert_ledger(monkeypatch) as ledger:
            await _mirror_landing("mirror_landing", [_issue("order_versions_stalled", _CRIT)])
            await settle()
            paged = len(telegram.messages)
            await _mirror_landing("mirror_landing", [_issue("order_versions_stalled", _WARN)])
            await settle()
            events = [(key, kind) for key, kind, _ in await ledger.events()]

        notices = telegram.messages[paged:]
        assert paged == len(ADMINS)
        assert sorted(int(p["chat_id"]) for p in notices) == sorted(ADMINS)
        assert all("order_versions_stalled" in p["text"] for p in notices)
        assert events == [("order_versions_stalled", "fired"),
                          ("order_versions_stalled", "resolved")]
        assert delivered_conditions() == {}
        assert [(i["check_name"], i["severity"])
                for i in journal.issues(journal.rows[-1].run_id)] == [
            ("order_versions_stalled", "WARN")]


# ─── Every layer that pages, run with a CRITICAL beside a WARN ──────────────


def _issue(name, severity):
    return IntegrityIssue(check_name=name, table_name="t", severity=severity,
                          count=1, sample_ids=[], description="d")


def _discrepancy(diff_class, severity):
    return Discrepancy(month="2026-11", source_id=4, diff_class=diff_class,
                       field="orders", dk_value=1.0, kc_value=2.0,
                       severity=severity, order_ids=(101,))


async def _integrity_job(*, scan, twins=(), chains=None):
    """The integrity job. Its scan answers `scan`, or raises it; the Postgres
    twins file `twins`; the chain invariants file `chains` over a read of
    every chain, or, with None, could not read; the capture stamps are unread."""
    from core import analytics_captures, pg_chain_invariants, pg_warehouse_dq
    from core.scheduler import BackgroundScheduler

    watched = tuple(sorted(pg_chain_invariants.watched_chains()))
    unread = AsyncMock(side_effect=RuntimeError("not read in this test"))
    with ExitStack() as stack:
        enter = stack.enter_context
        if isinstance(scan, BaseException):
            enter(patch("core.data_quality.check_internal_integrity", side_effect=scan))
        else:
            enter(patch("core.data_quality.check_internal_integrity", return_value=list(scan)))
        enter(patch.object(pg_warehouse_dq, "read_facts",
                           AsyncMock(return_value=clean_twin_facts())))
        enter(patch.object(pg_warehouse_dq, "check_pg_warehouse", return_value=list(twins)))
        if chains is None:
            enter(patch.object(pg_chain_invariants, "read_facts", AsyncMock(
                return_value=pg_chain_invariants.Facts.blind(watched, "not read in this test"))))
        else:
            enter(patch.object(pg_chain_invariants, "read_facts", AsyncMock(
                return_value=pg_chain_invariants.Facts(watched=watched))))
            enter(patch.object(pg_chain_invariants, "judge", return_value=list(chains)))
        enter(patch.object(analytics_captures, "read_facts", AsyncMock(
            return_value=analytics_captures.Facts(unwatched="not read in this test"))))
        for read in ("core.pg_inventory_write.read_snapshot_calendar",
                     "core.pg_chain_watermarks.read_values_and_stamps",
                     "core.pg_frozen_metadata.frozen_sync_rows"):
            enter(patch(read, unread))
        await BackgroundScheduler()._run_dq_integrity()


async def _integrity(layer, found):
    await _integrity_job(scan=found)


async def _mirror_landing(layer, found):
    """The mirror_landing job, the order archive's check finding `found` and
    every other check clean."""
    from core.scheduler import BackgroundScheduler

    clean = AsyncMock(return_value=[])
    with patch("core.mirror_reconciliation.reconcile_order_utm_completeness", clean), \
         patch("core.mirror_reconciliation.pg_gold_internal_check", clean), \
         patch("core.mirror_reconciliation.reconcile_order_versions",
               AsyncMock(return_value=list(found))), \
         patch("core.mirror_reconciliation.reconcile_buyer_completeness", clean), \
         patch("core.ch_silver.reconcile_clickhouse", clean), \
         patch("core.ch_history.reconcile_ch_history", clean):
        await BackgroundScheduler()._run_dq_mirror_landing()


async def _reconciliation(layer, found, *, error=None):
    """An arm of the 05:30 job persisting its verdict: `found` is (issues,
    discrepancies), and `error` what made the arm fail, if it did."""
    from core.scheduler import BackgroundScheduler

    scheduler = BackgroundScheduler()
    persist = {"reconciliation_pg": scheduler._persist_postgres_reconciliation,
               "reconciliation_ch": scheduler._persist_ch_reconciliation}[layer]
    issues, discrepancies = found
    now = datetime.now(timezone.utc)
    await persist({"issues": issues, "discrepancies": discrepancies, "error": error},
                  started_at=now, as_of=now, window_start=now.date(), window_end=now.date())


class _Layer(NamedTuple):
    run: Callable
    found: Any
    page: list      # the conditions the page is about: its CRITICALs
    riders: list    # what rode along in the same run, each a registered condition


_CRIT, _WARN = Severity.CRITICAL, Severity.WARN
_DRIFT, _MISMATCH = DiscrepancyClass.STATUS_DRIFT, DiscrepancyClass.VALUE_MISMATCH

PAGING_LAYERS = {
    "integrity": _Layer(
        _integrity,
        [_issue("freshness_orders", _CRIT), _issue("inventory_snapshot_gaps", _WARN)],
        ["freshness_orders"], ["inventory_snapshot_gaps"]),
    "mirror_landing": _Layer(
        _mirror_landing,
        [_issue("order_versions_stalled", _CRIT), _issue("buyers_without_verdict", _WARN)],
        ["order_versions_stalled"], ["buyers_without_verdict"]),
    "reconciliation_pg": _Layer(
        _reconciliation,
        ([_issue("mirror_backfill_pending", _WARN)],
         [_discrepancy(_DRIFT, _CRIT), _discrepancy(_MISMATCH, _WARN)]),
        ["recon_pg:status_drift"], ["mirror_backfill_pending", "recon_pg:value_mismatch"]),
    "reconciliation_ch": _Layer(
        _reconciliation,
        ([_issue("ch_reconcile_pending", _WARN)],
         [_discrepancy(_DRIFT, _CRIT), _discrepancy(_MISMATCH, _WARN)]),
        ["recon_ch:status_drift"], ["ch_reconcile_pending", "recon_ch:value_mismatch"]),
}


class TestPageConditionsAreCriticalOnly:
    """The first real night found this: a CRITICAL alert's conditions list
    carried the WARN/INFO findings riding in the same run, and the resolve
    that followed — still_firing being CRITICAL-only — instantly announced
    '✅ mirror_retired_rows — stood 0m' about a standing INFO that had not
    gone anywhere. The page is about what pages; the digest owns the rest."""

    @needs_postgres
    @pytest.mark.asyncio
    @pytest.mark.parametrize("layer", list(PAGING_LAYERS))
    async def test_a_page_is_about_its_criticals_alone(self, layer, monkeypatch):
        """Every layer that pages, run once with a CRITICAL beside a WARN
        (each a registered condition, so a WARN that rode along would be held
        as heard and announced). The page reaches each admin once, the ledger
        journals the CRITICAL alone, and nothing is announced resolved.
        Mutation: build any layer's `conditions` from every finding again."""
        spec = PAGING_LAYERS[layer]
        assert all(is_condition(name) for name in spec.page + spec.riders)

        install_journal(monkeypatch)
        telegram = telegram_at_the_transport(monkeypatch)
        async with alert_ledger(monkeypatch) as ledger:
            await spec.run(layer, spec.found)
            await settle()
            events = await ledger.events()

        assert telegram.chats() == sorted(ADMINS)
        assert events == [(name, "fired", len(ADMINS)) for name in spec.page]
        assert delivered_conditions() == {name: f"dq:{layer}" for name in spec.page}

    @needs_postgres
    @pytest.mark.asyncio
    async def test_a_failed_scan_still_clears_what_dropped_to_warn(self, monkeypatch):
        """When the integrity scan raises, the Postgres twins and the chain
        invariants did look, so a page of theirs that dropped to WARN is
        announced cleared on that run, by its prefix: their still-firing
        lists are CRITICAL-only too. Mutation: hold every twin or chain
        finding there, whatever its severity."""
        twin, chain = "pg_silver_missing_rows", "chain_sequence_behind"
        install_journal(monkeypatch)
        telegram = telegram_at_the_transport(monkeypatch)
        async with alert_ledger(monkeypatch) as ledger:
            await _integrity_job(scan=[], twins=[_issue(twin, _CRIT)],
                                 chains=[_issue(chain, _CRIT)])
            heard = delivered_conditions()
            await _integrity_job(scan=RuntimeError("the scan raised"),
                                 twins=[_issue(twin, _WARN)], chains=[_issue(chain, _WARN)])
            await settle()
            events = sorted((key, kind) for key, kind, _ in await ledger.events())

        assert heard == {twin: "dq:integrity", chain: "dq:integrity"}
        assert delivered_conditions() == {}
        assert events == [(chain, "fired"), (chain, "resolved"),
                          (twin, "fired"), (twin, "resolved")]
        # The page, then one notice for each family.
        assert telegram.chats() == sorted(ADMINS * 3)
