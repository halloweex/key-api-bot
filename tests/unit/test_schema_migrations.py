"""The schema ledger: run once, in order, and never fail in silence.

Migrations were 647 lines of `try / except / logger.debug` — thirty-seven blocks
re-run on every boot, any of which could fail leaving nothing above DEBUG. The
2026-08-09 incident started exactly there: an ALTER failed quietly, the code read
the column anyway, and a one-off became a rebuild every two minutes for hours.

What is pinned here is the guarantee, not the SQL: a step runs once, its outcome
is written down, a failure is loud and retried rather than forgotten, and the two
steps that must run every boot still do.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore
from core.migrations import ALWAYS, MIGRATIONS, ONCE, Migration


async def _make_store(tmp_path: Path, name: str = "m.duckdb") -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / name)
    await s.connect()
    return s


class TestTheLedger:
    @pytest.mark.asyncio
    async def test_a_fresh_database_applies_every_step_and_says_so(self, tmp_path):
        """Also the check that no migration is quietly broken: any exception in
        any of the 26 shows up here as a failure, which it never did before."""
        store = await _make_store(tmp_path)
        try:
            status = store.schema_status()

            assert status["failed"] == [], f"a migration failed: {status['failed']}"
            assert status["pending"] == [], f"never applied: {status['pending']}"
            assert status["status"] == "ok"
            assert status["applied"] == status["total"] == sum(
                1 for m in MIGRATIONS if m.mode == ONCE
            )
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_once_step_does_not_run_a_second_time(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            calls = []
            once_id = next(m.id for m in MIGRATIONS if m.mode == ONCE)
            always_id = next(m.id for m in MIGRATIONS if m.mode == ALWAYS)
            patched = [
                Migration(m.id, m.mode, lambda self, _i=m.id: calls.append(_i))
                if m.id in (once_id, always_id) else m
                for m in MIGRATIONS
            ]
            import core.migrations as mod
            original = mod.MIGRATIONS
            mod.MIGRATIONS = patched
            try:
                await store._run_migrations()
            finally:
                mod.MIGRATIONS = original

            assert once_id not in calls, "an applied step ran again"
            assert always_id in calls, "an ALWAYS step was skipped"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_failure_is_recorded_loudly_and_retried(self, tmp_path, caplog):
        """The whole point. Before this, the same failure logged at DEBUG and
        the next boot could not tell it had ever happened."""
        store = await _make_store(tmp_path)
        try:
            attempts = []

            def explode(self):
                attempts.append(1)
                raise RuntimeError("ALTER blew up")

            import core.migrations as mod
            original = mod.MIGRATIONS
            mod.MIGRATIONS = list(original) + [
                Migration("9999_deliberately_broken", ONCE, explode)
            ]
            try:
                with caplog.at_level(logging.ERROR):
                    await store._run_migrations()

                status = store.schema_status()
                assert [f["id"] for f in status["failed"]] == ["9999_deliberately_broken"]
                assert status["status"] == "failed"
                assert "9999_deliberately_broken" in status["pending"], (
                    "a failed step must not count as applied"
                )
                assert any("FAILED" in r.message or "FAILED" in r.getMessage()
                           for r in caplog.records), "the failure was not logged at ERROR"

                async with store.connection() as conn:
                    row = conn.execute(
                        "SELECT outcome, error_message FROM schema_migrations "
                        "WHERE id = '9999_deliberately_broken'"
                    ).fetchone()
                assert row[0] == "failed"
                assert "ALTER blew up" in row[1]

                # …and the next boot tries again rather than moving on.
                await store._run_migrations()
                assert len(attempts) == 2
            finally:
                mod.MIGRATIONS = original
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_ids_are_unique_and_ordered(self, tmp_path):
        """The id is what the ledger remembers; two steps sharing one, or a list
        that stops being sorted, both make 'has this run?' unanswerable."""
        ids = [m.id for m in MIGRATIONS]
        assert len(ids) == len(set(ids))
        assert ids == sorted(ids)
        assert all(m.mode in (ONCE, ALWAYS) for m in MIGRATIONS)

    @pytest.mark.asyncio
    async def test_the_two_always_steps_are_the_ones_we_meant(self, tmp_path):
        """If a third appears, it should be a decision, not a slip."""
        always = {m.id for m in MIGRATIONS if m.mode == ALWAYS}
        assert always == {
            "0004_drop_gold_daily_products_indexes",
            "0006_seed_manager_classifications",
            "0027_reset_sequences_after_compaction",
        }
