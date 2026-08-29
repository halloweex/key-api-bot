"""Track Б, step 08: the spool between the Gate and the host agent."""
import json
from unittest.mock import AsyncMock, patch

import pytest

from core import alert_agent_spool
from core.alert_agent_spool import drop_task


class TestDropTask:
    def test_disabled_by_default(self, monkeypatch):
        """A laptop must not fill a spool nobody drains."""
        monkeypatch.delenv(alert_agent_spool.ENABLE_ENV, raising=False)
        assert drop_task(["disk:WARN"], "disk:WARN", "x") is None

    def test_writes_one_atomic_json(self, monkeypatch, tmp_path):
        monkeypatch.setenv(alert_agent_spool.ENABLE_ENV, "1")
        monkeypatch.setenv("KS_ALERT_GATE_STATE_DIR", str(tmp_path))
        monkeypatch.setenv("KS_INSTANCE", "test-instance")
        target = drop_task(["mirror_failing"], "dq:fp", "тело алерта")
        assert target is not None and target.exists()
        payload = json.loads(target.read_text())
        assert payload["conditions"] == ["mirror_failing"]
        assert payload["bucket"] == "dq:fp"
        assert payload["instance"] == "test-instance"
        assert not list(target.parent.glob("*.tmp"))

    def test_bucket_is_slugged_into_the_filename(self, monkeypatch, tmp_path):
        monkeypatch.setenv(alert_agent_spool.ENABLE_ENV, "1")
        monkeypatch.setenv("KS_ALERT_GATE_STATE_DIR", str(tmp_path))
        target = drop_task(["k"], "canary:a,b/c", "x")
        assert "/" not in target.name.replace(target.suffix, "")
        assert target.suffix == ".json"

    def test_failure_never_raises(self, monkeypatch, tmp_path):
        monkeypatch.setenv(alert_agent_spool.ENABLE_ENV, "1")
        blocked = tmp_path / "file"
        blocked.write_text("occupied")
        monkeypatch.setenv("KS_ALERT_GATE_STATE_DIR", str(blocked))
        assert drop_task(["k"], "b", "x") is None


class TestGateWiring:
    @pytest.mark.asyncio
    async def test_a_fresh_incident_spools_and_a_reminder_does_not(
        self, monkeypatch, tmp_path,
    ):
        from core.alerting import raise_alert, reset_gate

        monkeypatch.setenv(alert_agent_spool.ENABLE_ENV, "1")
        monkeypatch.setenv("KS_ALERT_GATE_STATE_DIR", str(tmp_path))
        reset_gate()
        pending = tmp_path / "alert-tasks" / "pending"

        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=2)):
            await raise_alert("x", conditions=["disk:WARN"],
                              bucket="disk:WARN", group="disk")
            assert len(list(pending.glob("*.json"))) == 1
            # The standing daily reminder must not re-summon the agent: force
            # the gate into the standing phase and deliver a reminder.
            from core.alerting import _gate
            import time

            st = _gate._state["disk:WARN"]
            st.first_seen = time.time() - 90000
            st.last_sent = time.time() - 87000
            st.last_attempt = time.time() - 120
            st.suppressed = 5
            delivered = await raise_alert(
                "x", conditions=["disk:WARN"],
                bucket="disk:WARN", group="disk",
            )
            assert delivered == 2  # the reminder went out…
        assert len(list(pending.glob("*.json"))) == 1  # …but spooled nothing

    @pytest.mark.asyncio
    async def test_an_undelivered_alert_spools_nothing(
        self, monkeypatch, tmp_path,
    ):
        from core.alerting import raise_alert, reset_gate

        monkeypatch.setenv(alert_agent_spool.ENABLE_ENV, "1")
        monkeypatch.setenv("KS_ALERT_GATE_STATE_DIR", str(tmp_path))
        reset_gate()
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=0)):
            await raise_alert("x", conditions=["disk:WARN"],
                              bucket="disk:WARN", group="disk")
        assert not list((tmp_path / "alert-tasks" / "pending").glob("*"))
