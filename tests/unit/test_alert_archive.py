"""Step 03: the archive, the durable gate state, and the self-watch.

Three storages, three failure philosophies: the archive never raises and
never blocks (an alert must not die of its own bookkeeping); the gate state
file is tolerant on both ends (a corrupt file costs history, never a start);
the transport counter is process-local truth judged from the other container.
"""
import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from core import alert_archive, telegram_alerts
from core.alerting import AlertGate

# Bound at import, before conftest's autouse guard replaces the transport
# with a stub: the self-watch test needs the real sender's accounting.
_REAL_SEND = telegram_alerts.send_admin_message_http


class TestArchiveWriter:
    def test_no_dsn_means_quietly_disabled(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)

        async def main():
            return alert_archive.record_fired(
                ["disk:WARN"], message="x", delivered=2,
            )

        assert asyncio.run(main()) is None

    def test_a_dead_postgres_costs_one_warning_not_the_alert(
        self, monkeypatch, caplog,
    ):
        import logging

        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@nowhere:5/ks")
        monkeypatch.setattr(alert_archive, "_standing_down", False)

        async def boom(*a, **kw):
            raise ConnectionError("refused")

        async def main():
            with patch("core.alert_archive._write_fired", new=boom):
                with caplog.at_level(logging.WARNING, logger="core.alert_archive"):
                    for _ in range(3):
                        task = alert_archive.record_fired(
                            ["disk:WARN"], message="x", delivered=2,
                        )
                        assert task is not None
                        await task  # the task never raises

        asyncio.run(main())
        assert caplog.text.count("standing down") == 1

    def test_the_write_carries_its_own_timeout(self, monkeypatch):
        """A hung Postgres must not hold the task open past its budget."""
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@nowhere:5/ks")
        monkeypatch.setattr(alert_archive, "WRITE_TIMEOUT_S", 0.05)
        monkeypatch.setattr(alert_archive, "_standing_down", False)

        async def hang(*a, **kw):
            await asyncio.sleep(30)

        async def main():
            with patch("core.alert_archive._write_fired", new=hang):
                task = alert_archive.record_fired(
                    ["disk:WARN"], message="x", delivered=2,
                )
                await asyncio.wait_for(task, timeout=1.0)

        asyncio.run(main())

    def test_no_conditions_writes_nothing(self):
        async def main():
            return alert_archive.record_fired([], message="x", delivered=1)

        assert asyncio.run(main()) is None

    @pytest.mark.asyncio
    async def test_raise_alert_archives_only_delivered_sends(self, monkeypatch):
        from core.alerting import raise_alert, reset_gate

        reset_gate()
        recorded = []
        monkeypatch.setattr(
            "core.alert_archive.record_fired",
            lambda conditions, **kw: recorded.append((list(conditions), kw)),
        )
        with patch("bot.main.send_admin_message", new=AsyncMock(return_value=2)):
            await raise_alert("x", conditions=["disk:WARN"], bucket="disk:WARN")
            await raise_alert("x", conditions=["disk:WARN"], bucket="disk:WARN")
        # Second raise was gate-suppressed: one archive record, not two.
        assert len(recorded) == 1
        assert recorded[0][0] == ["disk:WARN"]
        assert recorded[0][1]["delivered"] == 2

        with patch("bot.main.send_admin_message", new=AsyncMock(return_value=0)):
            from core.alerting import _gate

            _gate.reset()
            await raise_alert("x", conditions=["disk:WARN"], bucket="disk:WARN")
        assert len(recorded) == 1  # undelivered → unarchived


class TestGateStateFile:
    def test_last_sent_survives_a_restart(self, tmp_path):
        """The property the file exists for: a deploy inside a standing
        condition's daily cooldown must not re-page."""
        path = tmp_path / "gate.json"
        g1 = AlertGate(state_path=path)
        assert g1.decide("b", has_condition=True, now=1000.0)[0]
        g1.record_delivery("b", now=1000.0)

        g2 = AlertGate(state_path=path)  # the restart
        ok, _ = g2.decide("b", has_condition=True, now=1200.0)
        assert ok is False  # still inside the 30-min cooldown

    def test_a_corrupt_file_costs_history_never_the_start(self, tmp_path):
        path = tmp_path / "gate.json"
        path.write_text("{ this is not json")
        g = AlertGate(state_path=path)
        assert g.decide("b", has_condition=True, now=0.0)[0]

    def test_a_missing_file_is_a_fresh_start(self, tmp_path):
        g = AlertGate(state_path=tmp_path / "absent.json")
        assert g.decide("b", has_condition=True, now=0.0)[0]

    def test_the_write_is_atomic_json(self, tmp_path):
        path = tmp_path / "gate.json"
        g = AlertGate(state_path=path)
        g.decide("b", has_condition=True, now=0.0)
        g.record_delivery("b", now=0.0)
        data = json.loads(path.read_text())
        # Format v2 since step 04: buckets + the delivered-conditions map.
        assert data["buckets"]["b"]["last_sent"] == 0.0
        assert "delivered" in data
        assert not list(tmp_path.glob("gate.json*[!n]"))  # no tmp leftovers

    def test_an_unwritable_path_never_breaks_a_decision(self, tmp_path):
        blocked = tmp_path / "not-a-dir-file"
        blocked.write_text("occupied")
        g = AlertGate(state_path=blocked / "gate.json")
        assert g.decide("b", has_condition=True, now=0.0)[0]
        assert g.record_delivery("b", now=0.0) == 0

    def test_record_delivery_returns_what_it_flushed(self):
        g = AlertGate()
        g.decide("b", has_condition=True, now=0.0)
        g.record_delivery("b", now=0.0)
        g.decide("b", has_condition=True, now=60.0)   # swallowed
        g.decide("b", has_condition=True, now=120.0)  # swallowed
        ok, _ = g.decide("b", has_condition=True, now=1900.0)
        assert ok
        assert g.record_delivery("b", now=1900.0) == 2


class TestTransportSelfWatch:
    def setup_method(self):
        telegram_alerts.reset_transport_health()

    teardown_method = setup_method

    def test_failures_count_and_a_delivery_resets(self):
        telegram_alerts.record_transport_outcome(0, attempted=2)
        telegram_alerts.record_transport_outcome(0, attempted=2)
        assert telegram_alerts.transport_health()[
            "consecutive_transport_failures"] == 2
        telegram_alerts.record_transport_outcome(1, attempted=2)
        health = telegram_alerts.transport_health()
        assert health["consecutive_transport_failures"] == 0
        assert health["last_delivery_at"] is not None

    def test_configuration_is_not_a_transport_failure(self):
        """Kill switch and empty recipients never attempt, never count."""
        telegram_alerts.record_transport_outcome(0, attempted=0)
        assert telegram_alerts.transport_health()[
            "consecutive_transport_failures"] == 0

    @pytest.mark.asyncio
    async def test_the_http_sender_feeds_the_counter(self, monkeypatch):
        monkeypatch.delenv(telegram_alerts.DISABLE_ENV, raising=False)

        class FailingClient:
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
            async def post(self, url, json=None, **kw):
                raise ConnectionError("net down")

        monkeypatch.setattr(telegram_alerts.httpx, "AsyncClient",
                            lambda **kw: FailingClient())
        assert await _REAL_SEND("боль", token="t", chat_ids=[1]) == 0
        assert telegram_alerts.transport_health()[
            "consecutive_transport_failures"] >= 1


class TestCanaryJudgesTheAlerting:
    def test_absence_is_a_failure(self):
        from bot.canary import check_alerting_health

        assert check_alerting_health({"status": "healthy"}) == [
            ("alerting_block_missing", "no alerting block in health")
        ]

    def test_three_consecutive_failures_page(self):
        from bot.canary import check_alerting_health

        block = {"alerting": {"consecutive_transport_failures": 3,
                              "last_delivery_at": None}}
        [(key, msg)] = check_alerting_health(block)
        assert key == "alerting_transport_failing"
        assert "3 delivery failures" in msg

    def test_a_single_hiccup_stays_quiet(self):
        from bot.canary import check_alerting_health

        block = {"alerting": {"consecutive_transport_failures": 1,
                              "last_delivery_at": 1.0}}
        assert check_alerting_health(block) == []
