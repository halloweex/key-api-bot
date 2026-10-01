"""`scripts/goals_semantics_dryrun.py` — chain 7b-2's flip precondition.

It runs the real goal methods twice over one in-memory copy of a backup,
under `KS_GOALS_HISTORY=bridge` and `=silver`, and exits 0 only when nothing
differs. Proved here on a backup written for the purpose:

  * the three-year history reads clean — 0, and the backup's bytes unchanged;
  * an order KeyCRM groups as lost whose status is not on the list is counted
    by the bridge alone: the script names the cause, shows the numbers it
    moved, and exits 1 — so the silver side really ran Silver (mutation:
    compute both sides under `bridge`, and the numbers here agree);
  * no read leaves the copy, whatever `KS_READ_GOALS` says in the
    environment (mutation: drop the forced `duckdb`, and a read is routed —
    counted as a fallback here);
  * a missing backup, or one without a goal table, is refused with 2;
  * each path to DIFFERENCES on its own: a growth cap — a list leaf — moving
    alone, and an order counted by one side only while no number moves, on
    either side (mutations MXp, MXq, MXq2).
"""
from __future__ import annotations

import asyncio
import hashlib
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from core import read_fallback
from core.duckdb_store import DuckDBStore
from tests.unit.test_goals_off_duckdb_silver import _seed_history

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from scripts import goals_semantics_dryrun as dryrun  # noqa: E402

KYIV = ZoneInfo("Europe/Kyiv")
TODAY = "2026-10-01"


def _backup(tmp_path, *extra) -> Path:
    path = tmp_path / "analytics-backup.duckdb"

    async def build():
        store = DuckDBStore(db_path=path)
        await store.connect()
        try:
            await _seed_history(store)
            async with store.connection() as conn:
                for sql, params in extra:
                    conn.execute(sql, params)
            if extra:
                # No ids: a full rebuild of Silver, which then holds them.
                await store.refresh_warehouse_layers(trigger="manual")
        finally:
            await store.close()

    asyncio.run(build())
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(autouse=True)
def _clean_counts(monkeypatch):
    monkeypatch.delenv("KS_GOALS_HISTORY", raising=False)
    read_fallback.reset_counts()
    yield
    read_fallback.reset_counts()


class TestTheDryRun:
    def test_the_history_reads_clean_and_the_backup_is_untouched(
        self, tmp_path, capsys,
    ):
        backup = _backup(tmp_path)
        before = _sha(backup)
        assert dryrun.main(["--backup", str(backup), "--today", TODAY]) == 0
        out = capsys.readouterr().out
        assert "CLEAN" in out and "no difference" in out
        assert _sha(backup) == before, "the dry run changed the backup"

    def test_a_difference_is_named_with_its_cause_and_its_numbers(
        self, tmp_path, capsys,
    ):
        # Lost by KeyCRM's group, status 1 not on the list: the bridge counts
        # it, Silver does not (the return rule — 0 such orders in production).
        backup = _backup(tmp_path, (
            "INSERT INTO orders (id, source_id, status_id, status_group_id, "
            "grand_total, ordered_at, buyer_id) VALUES (900001, 1, 1, 6, 250000, ?, 1)",
            [datetime(2025, 3, 10, 12, tzinfo=KYIV)]))
        assert dryrun.main(["--backup", str(backup), "--today", TODAY, "--json"]) == 1
        import json

        report = json.loads(capsys.readouterr().out)
        assert report["clean"] is False
        retail = report["orders"]["retail"]
        assert retail["bridge"] == retail["silver"] + 1
        assert list(retail["bridge_only"]) == ["return rule: status 1, group 6"]
        paths = [d["path"] for d in report["differences"]]
        assert any(p.startswith("seasonality/retail/3/") for p in paths), paths[:10]
        assert any(p.startswith("monday_job/seasonal/3/") for p in paths)

    def test_no_read_leaves_the_copy(self, tmp_path, monkeypatch, capsys):
        backup = _backup(tmp_path)
        monkeypatch.setenv("KS_READ_GOALS", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://ks_app:x@127.0.0.1:9/ks")
        assert dryrun.main(["--backup", str(backup), "--today", TODAY]) == 0
        assert read_fallback.counts() == {}, "a goal read was routed off the copy"

    def test_a_missing_backup_is_refused(self, tmp_path, capsys):
        assert dryrun.main(["--backup", str(tmp_path / "none.duckdb")]) == 2
        assert "refused" in capsys.readouterr().err

    def test_a_backup_without_a_goal_table_is_refused(self, tmp_path, capsys):
        import duckdb

        path = tmp_path / "partial.duckdb"
        conn = duckdb.connect(str(path))
        conn.execute("CREATE TABLE orders (id INTEGER)")
        conn.close()
        assert dryrun.main(["--backup", str(path)]) == 2
        assert "silver_orders" in capsys.readouterr().err


class TestEveryVerdictPathCounts:
    """The two ways the dry run reaches DIFFERENCES, each pinned on its own
    (review of 7b-2: both mutations below passed the tests above, which only
    ever produced a difference in a number *and* in the orders at once)."""

    def test_a_list_leaf_is_compared(self):
        """The twelve growth caps per sales type are a list. Mutation MXp:
        skip lists in `_differences`, and a cap that moved reads clean."""
        bridge = {"caps/retail": [0.25, 0.3, 0.35], "nested": [{"a": [1.0, 2.0]}]}
        silver = {"caps/retail": [0.25, 0.31, 0.35], "nested": [{"a": [1.0, 2.5]}]}
        assert dryrun._differences(bridge, silver) == [
            ("caps/retail[1]", 0.3, 0.31),
            ("nested[0]/a[1]", 2.0, 2.5),
        ]
        # A list that changed length is one difference, not a silent pass.
        assert dryrun._differences({"x": [1.0]}, {"x": [1.0, 2.0]}) == [
            ("x", [1.0], [1.0, 2.0])]

    def test_a_cap_alone_moving_fails_the_run(self, tmp_path, monkeypatch, capsys):
        """End to end: the silver side's answers differ in one cap and in
        nothing else — exit 1, the cap named by its path."""
        import json

        from core import pg_goals_read

        backup = _backup(tmp_path)
        real = dryrun.answers

        async def one_cap_moved(conn, today):
            out = await real(conn, today)
            if pg_goals_read.history_mode() == "silver":
                out["caps/retail"][3] = round(out["caps/retail"][3] + 0.01, 6)
            return out

        monkeypatch.setattr(dryrun, "answers", one_cap_moved)
        assert dryrun.main(["--backup", str(backup), "--today", TODAY, "--json"]) == 1
        report = json.loads(capsys.readouterr().out)
        assert [d["path"] for d in report["differences"]] == ["caps/retail[3]"]
        assert all(not v["bridge_only"] and not v["silver_only"]
                   for v in report["orders"].values())

    @pytest.mark.parametrize("status,group,side", [
        (1, 6, "bridge_only"),    # lost by KeyCRM's group, status not listed
        (19, 4, "silver_only"),   # status listed, KeyCRM's group says not lost
    ])
    def test_orders_that_differ_fail_the_run_when_no_number_moves(
        self, tmp_path, capsys, status, group, side,
    ):
        """An order counted by one side only, worth nothing: no number moves,
        and the flip would still change which orders count. Mutation MXq:
        `Report.clean` reads the numbers alone, and both exit 0; MXq2: it
        reads `bridge_only` alone, and the second does."""
        import json

        backup = _backup(tmp_path, (
            "INSERT INTO orders (id, source_id, status_id, status_group_id, "
            "grand_total, ordered_at, buyer_id) VALUES (900001, 1, ?, ?, 0, ?, 1)",
            [status, group, datetime(2025, 3, 10, 12, tzinfo=KYIV)]))
        assert dryrun.main(["--backup", str(backup), "--today", TODAY, "--json"]) == 1
        report = json.loads(capsys.readouterr().out)
        assert report["differences"] == [] and report["clean"] is False
        retail = report["orders"]["retail"]
        assert list(retail[side]) == [f"return rule: status {status}, group {group}"]
        other = "silver_only" if side == "bridge_only" else "bridge_only"
        assert retail[other] == {}
