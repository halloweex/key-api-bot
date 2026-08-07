"""A stuck warehouse must be able to recover on its own.

On 2026-08-02 an OOM storm truncated the Gold layer, and the self-heal that
exists to repair exactly that had already spent its budget on the storm itself.
"Auto-retry stopped, manual fix needed" then held for five days while the
dashboard served ₴1.35M less than the warehouse knew about.
"""
import duckdb
import pytest

from core.duckdb_store import (
    MAX_VALIDATION_RETRIES,
    STUCK_REBUILD_COOLDOWN_SECONDS,
    DuckDBStore,
)


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    c.execute("""
        CREATE TABLE warehouse_refreshes (
            refreshed_at TIMESTAMPTZ,
            validation_passed BOOLEAN,
            error VARCHAR
        )
    """)
    yield c
    c.close()


def add(conn, minute, *, passed=None, error=None):
    conn.execute(
        "INSERT INTO warehouse_refreshes VALUES (?::TIMESTAMPTZ, ?, ?)",
        [f"2026-08-02 0{minute}:00:00+00", passed, error],
    )


def count_validation_failures(conn):
    """The production counter: consecutive validation failures, errors skipped."""
    rows = conn.execute(
        "SELECT validation_passed FROM warehouse_refreshes "
        "WHERE error IS NULL ORDER BY refreshed_at DESC LIMIT ?",
        [MAX_VALIDATION_RETRIES + 1],
    ).fetchall()
    consecutive = 0
    for (vp,) in rows:
        if vp is False:
            consecutive += 1
        else:
            break
    return consecutive


class TestBudgetsAreSeparate:
    def test_error_storm_does_not_spend_the_validation_budget(self, conn):
        """The 2026-08-02 shape: healthy, then OOMs, then real damage."""
        add(conn, 1, passed=True)
        for minute in (2, 3, 4, 5):
            add(conn, minute, passed=False, error="Out of Memory Error")

        assert count_validation_failures(conn) == 0, (
            "OOM rows must not count as validation failures — the budget has to "
            "survive to repair the damage the storm caused"
        )

    def test_validation_failures_still_accumulate(self, conn):
        add(conn, 1, passed=True)
        add(conn, 2, passed=False)
        add(conn, 3, passed=False)

        assert count_validation_failures(conn) == 2

    def test_a_pass_breaks_the_streak(self, conn):
        add(conn, 1, passed=False)
        add(conn, 2, passed=True)
        add(conn, 3, passed=False)

        assert count_validation_failures(conn) == 1

    def test_errors_interleaved_do_not_break_a_real_streak(self, conn):
        add(conn, 1, passed=False)
        add(conn, 2, passed=False, error="Out of Memory Error")
        add(conn, 3, passed=False)

        assert count_validation_failures(conn) == 2

    @pytest.mark.asyncio
    async def test_error_counter_counts_errors_not_validation(self, tmp_path):
        store = DuckDBStore(db_path=tmp_path / "t.duckdb")
        await store.connect()
        try:
            async with store.connection() as c:
                c.execute("DELETE FROM warehouse_refreshes")
                for _ in range(3):
                    c.execute(
                        "INSERT INTO warehouse_refreshes "
                        "(refreshed_at, trigger, validation_passed, error) "
                        "VALUES (CURRENT_TIMESTAMP, 'test', FALSE, NULL)"
                    )
            assert await store._count_consecutive_refresh_failures() == 0
        finally:
            await store.close()


class TestStuckRebuildSlot:
    def test_first_claim_succeeds(self):
        store = DuckDBStore.__new__(DuckDBStore)
        assert store._claim_stuck_rebuild_slot() is True

    def test_second_claim_within_cooldown_is_refused(self):
        store = DuckDBStore.__new__(DuckDBStore)
        store._claim_stuck_rebuild_slot()

        assert store._claim_stuck_rebuild_slot() is False

    def test_claim_succeeds_again_after_the_cooldown(self, monkeypatch):
        import core.duckdb_store as mod

        store = DuckDBStore.__new__(DuckDBStore)
        clock = {"t": 1000.0}
        monkeypatch.setattr(mod.time, "monotonic", lambda: clock["t"])

        assert store._claim_stuck_rebuild_slot() is True
        clock["t"] += STUCK_REBUILD_COOLDOWN_SECONDS - 1
        assert store._claim_stuck_rebuild_slot() is False
        clock["t"] += 2
        assert store._claim_stuck_rebuild_slot() is True

    def test_a_fresh_process_re_arms_immediately(self):
        """A restart is when someone has just changed something. Let it try."""
        first = DuckDBStore.__new__(DuckDBStore)
        first._claim_stuck_rebuild_slot()

        restarted = DuckDBStore.__new__(DuckDBStore)
        assert restarted._claim_stuck_rebuild_slot() is True

    def test_instances_do_not_share_the_slot_through_the_class(self):
        a = DuckDBStore.__new__(DuckDBStore)
        b = DuckDBStore.__new__(DuckDBStore)
        a._claim_stuck_rebuild_slot()

        assert b._claim_stuck_rebuild_slot() is True
        assert DuckDBStore._last_stuck_rebuild is None
