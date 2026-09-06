"""Step 04: a condition that clears says so — once, and only if its firing
was ever heard.

Before this, exactly one condition in the system announced its own recovery
(the canary). Everything else went silent on clearing, and silence after an
alert is indistinguishable from the throttle holding it. The gate: a
"✅ resolved" may only follow a fired notice that reached someone — otherwise
the first thing a human hears about a condition is that it went away.
"""
from unittest.mock import AsyncMock, patch

import pytest

from core.alerting import AlertGate, raise_alert, reset_gate, resolve_group


@pytest.fixture(autouse=True)
def _clean():
    reset_gate()
    yield
    reset_gate()


def _sent(n=2):
    return patch("bot.main.send_admin_message", new=AsyncMock(return_value=n))


def _archived():
    return patch("core.alert_archive.record_resolved")


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
        """The DQ shape: three findings announced, next run still has one."""
        with _sent():
            await raise_alert(
                "x",
                conditions=["mirror_missing_rows", "mirror_orphan_rows",
                            "mirror_failing"],
                bucket="dq:fp1", group="dq:mirror_landing",
            )
        with _sent() as send, _archived() as rec:
            await resolve_group("dq:mirror_landing",
                                still_firing=["mirror_failing"])
            text = send.await_args.args[0]
            assert "mirror_missing_rows" in text
            assert "mirror_orphan_rows" in text
            assert "mirror_failing" not in text
            assert sorted(rec.call_args.args[0]) == [
                "mirror_missing_rows", "mirror_orphan_rows"]
        # The survivor clears on a later clean run.
        with _sent() as send2, _archived():
            await resolve_group("dq:mirror_landing")
            assert "mirror_failing" in send2.await_args.args[0]

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
        rec.assert_called_once()
        assert rec.call_args.kwargs["delivered"] == 0


class TestDeliveredMapDurability:
    def test_the_map_survives_a_restart(self, tmp_path):
        """A condition fired before a deploy must still resolve after it."""
        path = tmp_path / "gate.json"
        g1 = AlertGate(state_path=path)
        g1.note_delivered_conditions(["disk:WARN"], "disk", now=1000.0)

        g2 = AlertGate(state_path=path)
        taken = g2.take_resolved("disk", now=2000.0)
        assert taken == {"disk:WARN": 1000.0}

    def test_the_step03_file_format_still_loads(self, tmp_path):
        path = tmp_path / "gate.json"
        path.write_text(
            '{"b": {"first_seen": 1.0, "last_attempt": 1.0,'
            ' "last_sent": 1.0, "suppressed": 0}}'
        )
        g = AlertGate(state_path=path)
        assert g.decide("b", has_condition=True, now=10.0)[0] is False

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
        from core.duckdb_store import DuckDBStore
        from core.scheduler import BackgroundScheduler

        store = DuckDBStore(db_path=tmp_path / "t.duckdb")
        await store.connect()
        try:
            scheduler = BackgroundScheduler()

            def sample(pct, free):
                return {"sampled_at": datetime.now(timezone.utc),
                        "db_size_mb": 2480, "disk_pct_used": pct,
                        "disk_free_gb": free}

            with patch("core.duckdb_store.get_store",
                       AsyncMock(return_value=store)), \
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
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_failed_dq_run_resolves_nothing(self):
        """A run that produced no verdict proves nothing: the condition is
        unknown, not cleared."""
        from core.scheduler import BackgroundScheduler

        scheduler = BackgroundScheduler()
        with patch("core.alerting.resolve_group", new=AsyncMock()) as rg:
            await scheduler._resolve_dq_layer("integrity", [], "boom")
        rg.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_warn_downgrade_clears_the_page_condition(self):
        """CRITICAL → WARN: the page-condition cleared even though the digest
        still carries the finding — the charter's lane split."""
        from core.data_quality import IntegrityIssue, Severity
        from core.scheduler import BackgroundScheduler

        scheduler = BackgroundScheduler()
        warn_issue = IntegrityIssue(
            check_name="mirror_retired_rows", table_name="t",
            severity=Severity.WARN, count=1, sample_ids=[], description="d",
        )
        with patch("core.alerting.resolve_group", new=AsyncMock()) as rg:
            await scheduler._resolve_dq_layer(
                "mirror_landing", [warn_issue], None,
            )
        assert rg.await_args.kwargs.get("still_firing") == [] or \
               rg.await_args.args[1:] == ([],)


class TestPageConditionsAreCriticalOnly:
    """The first real night found this: a CRITICAL alert's conditions list
    carried the WARN/INFO findings riding in the same run, and the resolve
    that followed — still_firing being CRITICAL-only — instantly announced
    '✅ mirror_retired_rows — stood 0m' about a standing INFO that had not
    gone anywhere. The page is about what pages; the digest owns the rest."""

    def test_the_five_dq_sites_pass_critical_only(self):
        import pathlib

        src = pathlib.Path("core/scheduler.py").read_text()
        assert src.count(
            "if i.severity == Severity.CRITICAL],"
        ) == 5
        # And the old unfiltered form is extinct.
        assert "conditions=[i.check_name for i in issues]," not in src
