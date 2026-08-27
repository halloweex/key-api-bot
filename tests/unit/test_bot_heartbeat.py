"""The healthcheck, and the two lies the old one told.

`os.path.exists('/app/data/bot.db')` said nothing about whether the bot was
polling, and after the move to Postgres on 2026-08-27 it reported healthy
against a file nothing writes any more. Both failures are pinned here as tests
that would fail if either property were lost again.
"""
from __future__ import annotations

import inspect
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from bot import heartbeat
from bot.heartbeat import (
    BEAT_INTERVAL_SECONDS,
    MAX_AGE_SECONDS,
    beat_age_seconds,
    beat_job,
    is_healthy,
    main,
    write_beat,
)

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture
def beat_file(tmp_path, monkeypatch):
    path = tmp_path / "bot-heartbeat"
    monkeypatch.setattr(heartbeat, "HEARTBEAT_PATH", path)
    return path


class TestReadingTheBeat:
    def test_a_fresh_beat_is_healthy(self, beat_file):
        write_beat()
        assert is_healthy() is True
        assert beat_age_seconds() < 1

    def test_no_beat_at_all_is_not_healthy(self, beat_file):
        """A container that has just started has written nothing, and saying
        so is correct — that is what `start_period` is for."""
        assert beat_age_seconds() == float("inf")
        assert is_healthy() is False

    def test_a_stale_beat_is_not_healthy(self, beat_file):
        beat_file.write_text(str(time.time() - MAX_AGE_SECONDS - 1))
        assert is_healthy() is False

    def test_the_boundary_is_inclusive(self, beat_file):
        beat_file.write_text(str(time.time() - MAX_AGE_SECONDS + 1))
        assert is_healthy() is True

    def test_a_corrupt_beat_reads_as_no_beat(self, beat_file):
        """Fail closed: a file nobody can read is not evidence of life."""
        beat_file.write_text("not a number")
        assert is_healthy() is False

    def test_the_window_is_four_beats(self):
        """One slow query or one missed tick is not an incident; a wedged loop
        shows up inside two minutes."""
        assert MAX_AGE_SECONDS == 4 * BEAT_INTERVAL_SECONDS


class TestWritingIt:
    def test_it_is_written_atomically(self, beat_file):
        """A healthcheck reading a file mid-write would see an empty one and
        call it a bad beat."""
        source = inspect.getsource(write_beat)
        assert ".replace(" in source
        write_beat()
        assert not beat_file.with_suffix(".tmp").exists()

    @pytest.mark.asyncio
    async def test_the_job_probes_the_store_before_it_beats(self, beat_file):
        """The property the old check could never have: an unreachable store
        must go stale, not report healthy."""
        with patch("bot.heartbeat.probe_store") as probe:
            await beat_job(context=None)
        probe.assert_called_once()
        assert is_healthy() is True

    @pytest.mark.asyncio
    async def test_a_store_that_cannot_answer_writes_no_beat(self, beat_file):
        with patch("bot.heartbeat.probe_store",
                   side_effect=RuntimeError("postgres is away")):
            await beat_job(context=None)
        assert not beat_file.exists()
        assert is_healthy() is False

    @pytest.mark.asyncio
    async def test_the_job_never_raises(self, beat_file):
        """A healthcheck is not worth taking the bot down for."""
        with patch("bot.heartbeat.probe_store",
                   side_effect=RuntimeError("boom")):
            await beat_job(context=None)

    def test_the_probe_goes_through_the_port(self):
        """Not a direct sqlite3 or asyncpg call: the check must exercise
        whichever engine is actually live."""
        source = inspect.getsource(heartbeat.probe_store)
        assert "get_bot_store()" in source
        assert "sqlite3" not in source and "asyncpg" not in source


class TestTheEntryPoint:
    def test_it_exits_zero_when_healthy(self, beat_file):
        write_beat()
        assert main() == 0

    def test_it_exits_one_when_not(self, beat_file):
        assert main() == 1


class TestItIsWiredUp:
    """Parsed, never grepped.

    The first draft of this class asserted the old check was gone with `not in
    compose` — and the comment explaining why it was removed contains the
    string, so the test failed against correct code. That is the seventh time
    this repository has hit it. YAML and AST below.
    """

    @staticmethod
    def _bot_healthcheck():
        import yaml

        compose = yaml.safe_load(
            (REPO / "docker-compose.yml").read_text(encoding="utf-8")
        )
        return compose["services"]["bot"]["healthcheck"]

    def test_the_bot_beats_on_its_own_loop(self):
        """A `job_queue` job, so it only happens if the loop that also answers
        Telegram is turning. A thread of its own would keep beating past a
        wedged loop and report a dead bot as alive."""
        import ast

        tree = ast.parse((REPO / "bot" / "main.py").read_text(encoding="utf-8"))
        scheduled = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "run_repeating"
            and any(
                isinstance(a, ast.Name) and a.id == "beat_job" for a in node.args
            )
        ]
        assert len(scheduled) == 1, "beat_job is not scheduled exactly once"
        assert any(
            isinstance(kw.value, ast.Name)
            and kw.value.id == "BEAT_INTERVAL_SECONDS"
            for kw in scheduled[0].keywords
            if kw.arg == "interval"
        ), "the interval must be the one MAX_AGE_SECONDS is derived from"

    def test_the_check_runs_the_heartbeat_module(self):
        assert self._bot_healthcheck()["test"] == [
            "CMD", "python", "-m", "bot.heartbeat",
        ]

    def test_the_check_touches_no_database(self):
        """Under KS_BOT_STORE=postgres a check that queried the database would
        build a pool and a loop thread every thirty seconds — 2,880 a day to
        answer a question the bot answers for free."""
        command = " ".join(self._bot_healthcheck()["test"])
        for forbidden in ("bot.db", "sqlite", "psql", "KS_PG_DSN", "-c"):
            assert forbidden not in command, forbidden

    def test_the_window_gives_a_fresh_container_time_to_beat(self):
        """It has written nothing yet and is correctly unhealthy until it
        does."""
        check = self._bot_healthcheck()
        assert check["start_period"] == "60s"
        assert check["interval"] == "30s"

    def test_the_beat_is_container_local(self):
        """`/app/data` is a bind mount shared with the web container: a beat
        there outlives the container that wrote it, and a dead bot would be
        reported alive by its own ghost."""
        assert "/app/data" not in str(heartbeat.HEARTBEAT_PATH)
