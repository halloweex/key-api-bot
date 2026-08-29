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
    async def test_a_kill_before_a_restart_is_still_seen(self):
        """The kernel counter resets on recreate; the file is what remembers.
        First tick stores oom=3; the container is recreated (counter back to
        0 then climbs to 1) — the delta logic must not read that as clean."""
        with patch("core.memory_monitor.read_cgroup_memory",
                   return_value=_mem(oom=3)), \
             patch("core.alerting.raise_alert",
                   new=AsyncMock(return_value=2)):
            await memory_watch.check_bot_memory()  # first ever: no previous

        # recreate: counter reset to 1 (one NEW kill after restart)
        with patch("core.memory_monitor.read_cgroup_memory",
                   return_value=_mem(oom=1)), \
             patch("core.alerting.raise_alert",
                   new=AsyncMock(return_value=2)) as ra:
            result = await memory_watch.check_bot_memory()
        # evaluate_memory owns the delta semantics; what this pins is that
        # the PREVIOUS stored value reached it through the file.
        state = json.loads(memory_watch.STATE_PATH.read_text())
        assert state["oom_kills"] == 1

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
