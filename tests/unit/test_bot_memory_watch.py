"""The bot watching its own 512 MB — same evaluator, own persistence.

The web monitor reads its own cgroup, so the container with the limit
fourteen times smaller had a healthcheck and no pressure telemetry. This
pins the parts that differ from the web path: the JSON state file (the bot
cannot open DuckDB), and the OOM delta surviving a restart through it.
"""
import json
from unittest.mock import AsyncMock, patch

import pytest

from bot import memory_watch
from tests.helpers.alerts_telegram import ADMINS, telegram_at_the_transport


@pytest.fixture(autouse=True)
def _state_in_tmp(monkeypatch, tmp_path):
    monkeypatch.setattr(memory_watch, "STATE_PATH", tmp_path / "mem.json")
    memory_watch._last_alert.clear()
    yield
    memory_watch._last_alert.clear()


def _mem(working=100, cache=50, limit=512, oom=0):
    mb = 1048576
    return {"working_set": working * mb, "page_cache": cache * mb,
            "limit": limit * mb, "oom_kills": oom}


class TestHealthyPath:
    @pytest.mark.asyncio
    async def test_ok_writes_state_and_resolves(self):
        with patch("core.memory_monitor.read_cgroup_memory",
                   return_value=_mem()), \
             patch("core.alerting.resolve_group", new=AsyncMock()) as rg:
            result = await memory_watch.check_bot_memory()
        assert result["alert_sent"] is None
        rg.assert_awaited_once_with("memory:bot")
        state = json.loads(memory_watch.STATE_PATH.read_text())
        assert state["oom_kills"] == 0

    @pytest.mark.asyncio
    async def test_no_cgroup_is_quiet(self):
        with patch("core.memory_monitor.read_cgroup_memory",
                   return_value=None), \
             patch("core.alerting.raise_alert", new=AsyncMock()) as ra:
            await memory_watch.check_bot_memory()
        ra.assert_not_awaited()


class TestPressure:
    @pytest.mark.asyncio
    async def test_critical_raises_its_own_condition_key(self):
        with patch("core.memory_monitor.read_cgroup_memory",
                   return_value=_mem(working=480)), \
             patch("core.alerting.raise_alert",
                   new=AsyncMock(return_value=2)) as ra:
            result = await memory_watch.check_bot_memory()
        assert result["alert_sent"] == "CRITICAL"
        kw = ra.await_args.kwargs
        assert kw["conditions"] == ["memory:bot:CRITICAL"]
        assert kw["group"] == "memory:bot"
        assert kw["spool_as"] == "memory:bot:CRITICAL"  # the diagnostician

    @pytest.mark.asyncio
    async def test_the_level_cooldown_holds_between_ticks(self):
        with patch("core.memory_monitor.read_cgroup_memory",
                   return_value=_mem(working=480)), \
             patch("core.alerting.raise_alert",
                   new=AsyncMock(return_value=2)) as ra:
            await memory_watch.check_bot_memory()
            second = await memory_watch.check_bot_memory()
        assert ra.await_count == 1
        assert "suppressed" in second["alert_sent"]

    @pytest.mark.asyncio
    async def test_an_undelivered_alert_does_not_burn_the_cooldown(self):
        with patch("core.memory_monitor.read_cgroup_memory",
                   return_value=_mem(working=480)), \
             patch("core.alerting.raise_alert",
                   new=AsyncMock(return_value=0)) as ra:
            await memory_watch.check_bot_memory()
            await memory_watch.check_bot_memory()
        assert ra.await_count == 2


class TestOomAcrossRestart:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("stored, now, killed", [
        pytest.param(0, 1, True, id="a kill since the stored sample is reported"),
        # Read as "no previous sample", the same count would be reported
        # again after every restart.
        pytest.param(3, 3, False, id="an old kill is not reported again"),
        # A count below the stored one is a recreated container, which
        # `evaluate_memory` does not read as a kill.
        pytest.param(3, 1, False, id="a reset counter is not a kill"),
    ])
    async def test_the_stored_sample_decides_whether_a_kill_is_new(
            self, monkeypatch, tmp_path, stored, now, killed):
        """The bot process forgets everything but its state file, and the
        kernel's counter resets when the container is recreated: one tick
        stores `stored`, the next, after a restart, reads `now`. A kill goes
        to every admin and spools the diagnostician as `memory:bot:oom`."""
        monkeypatch.setenv("KS_ALERT_AGENT", "1")
        monkeypatch.setenv("KS_ALERT_GATE_STATE_DIR", str(tmp_path))
        monkeypatch.delenv("KS_PG_DSN", raising=False)  # no archive: not this test's subject
        telegram = telegram_at_the_transport(monkeypatch)
        spool = tmp_path / "alert-tasks" / "pending"

        with patch("core.memory_monitor.read_cgroup_memory", return_value=_mem(oom=stored)):
            await memory_watch.check_bot_memory()
        sent, spooled = len(telegram.messages), set(spool.glob("*.json"))
        with patch("core.memory_monitor.read_cgroup_memory", return_value=_mem(oom=now)):
            result = await memory_watch.check_bot_memory()
        tasks = [json.loads(p.read_text()) for p in set(spool.glob("*.json")) - spooled]

        if killed:
            assert result["alert_sent"] == "CRITICAL"
            assert len(telegram.messages) - sent == len(ADMINS)
            assert [t["bucket"] for t in tasks] == ["memory:bot:oom"]
        else:
            assert result["alert_sent"] is None
            assert len(telegram.messages) == sent
            assert tasks == []

    @pytest.mark.asyncio
    async def test_oom_spools_the_diagnostician_without_conditions(self):
        with patch("core.memory_monitor.read_cgroup_memory",
                   return_value=_mem(oom=2)), \
             patch("core.alerting.raise_alert",
                   new=AsyncMock(return_value=2)) as ra:
            # previous state with fewer kills
            memory_watch._write_state(_mem(oom=0))
            await memory_watch.check_bot_memory()
        kw = ra.await_args.kwargs
        assert kw["conditions"] == []
        assert kw["spool_as"] == "memory:bot:oom"

    @pytest.mark.asyncio
    async def test_a_corrupt_state_file_never_breaks_the_tick(self):
        memory_watch.STATE_PATH.write_text("{broken")
        with patch("core.memory_monitor.read_cgroup_memory",
                   return_value=_mem()), \
             patch("core.alerting.resolve_group", new=AsyncMock()):
            result = await memory_watch.check_bot_memory()
        assert result is not None


class TestSpoolAs:
    @pytest.mark.asyncio
    async def test_bucketless_raise_with_spool_as_summons_the_agent(
        self, monkeypatch, tmp_path,
    ):
        from core.alerting import raise_alert, reset_gate

        monkeypatch.setenv("KS_ALERT_AGENT", "1")
        monkeypatch.setenv("KS_ALERT_GATE_STATE_DIR", str(tmp_path))
        reset_gate()
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=2)):
            await raise_alert("x", conditions=["memory:web:WARN"],
                              bucket=None, spool_as="memory:web:WARN")
        [task] = list((tmp_path / "alert-tasks" / "pending").glob("*.json"))
        payload = json.loads(task.read_text())
        assert payload["bucket"] == "memory:web:WARN"

    @pytest.mark.asyncio
    async def test_undelivered_spools_nothing_even_with_spool_as(
        self, monkeypatch, tmp_path,
    ):
        from core.alerting import raise_alert, reset_gate

        monkeypatch.setenv("KS_ALERT_AGENT", "1")
        monkeypatch.setenv("KS_ALERT_GATE_STATE_DIR", str(tmp_path))
        reset_gate()
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=0)):
            await raise_alert("x", conditions=[], bucket=None,
                              spool_as="memory:web:oom")
        assert not list((tmp_path / "alert-tasks" / "pending").glob("*"))
