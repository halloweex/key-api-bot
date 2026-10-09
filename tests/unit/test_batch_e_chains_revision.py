"""Revision 0035, the lock on images older than the eight chains #280 registered.

It changes no schema — twenty table comments — and exists so that an image
without chains 3, 5, 6, 7b-3, 9, 10, 11a and 11b refuses a database that has
them (`REQUIRED_REVISION`). What was there before is not restated here: it is
read by replaying every revision up to 0034 against a recorder
(`tests/migration_replay.py`), which reproduces all 57 table comments of a real
0034 database. Each test names the mutation it kills.

**Frozen, like the revision.** Everything 0035 wrote is spelled here as a
literal — its twenty tables, the chain, flag and module each comment names, the
twelve that had no comment before — and nothing is read from the chain modules
as they are today. Once applied, a revision is never re-run, so its tests must
not move when the code does: tied to each module's live `CHAIN_TABLES`, a table
added to a chain failed four tests here, and the obvious repair, editing 0035,
would have changed what CI and the replay read and nothing in production. A
change to a chain fails `tests/unit/test_chain_lockout.py` instead, once, with
the instruction it needs: a new revision.
"""
from __future__ import annotations

from tests import migration_replay as replay

REV_ID = "0035_batch_e_chains"
REV = replay.VERSIONS / f"{REV_ID}.py"

# {table: (the chain's name in CLAUDE.md, its flag, its module)}, as 0035 wrote
# them on 2026-10-09.
TABLES = {
    "bronze.orders": ("chain 3", "KS_WRITE_ORDERS", "pg_orders_write"),
    "bronze.order_products": ("chain 3", "KS_WRITE_ORDERS", "pg_orders_write"),
    "bronze.expenses": ("chain 3", "KS_WRITE_ORDERS", "pg_orders_write"),
    "app.order_backfill_misses": ("chain 3", "KS_WRITE_ORDERS", "pg_orders_write"),
    "bronze.managers": ("chain 5", "KS_WRITE_MANAGERS", "pg_managers_write"),
    "app.manager_classifications": ("chain 5", "KS_WRITE_MANAGERS", "pg_managers_write"),
    "bronze.products": ("chain 6", "KS_WRITE_CATALOGUE", "pg_catalogue_write"),
    "bronze.categories": ("chain 6", "KS_WRITE_CATALOGUE", "pg_catalogue_write"),
    "app.seasonal_indices": ("chain 7b-3", "KS_WRITE_FORECAST", "pg_forecast_write"),
    "app.growth_metrics": ("chain 7b-3", "KS_WRITE_FORECAST", "pg_forecast_write"),
    "app.weekly_patterns": ("chain 7b-3", "KS_WRITE_FORECAST", "pg_forecast_write"),
    "app.revenue_predictions": ("chain 7b-3", "KS_WRITE_FORECAST", "pg_forecast_write"),
    "app.data_quality_runs": ("chain 9", "KS_WRITE_DQ_JOURNAL", "pg_dq_journal_write"),
    "app.data_quality_issues": ("chain 9", "KS_WRITE_DQ_JOURNAL", "pg_dq_journal_write"),
    "app.data_quality_diffs": ("chain 9", "KS_WRITE_DQ_JOURNAL", "pg_dq_journal_write"),
    "app.disk_samples": ("chain 10", "KS_WRITE_WATCHDOGS", "pg_watchdog_write"),
    "app.data_dir_samples": ("chain 10", "KS_WRITE_WATCHDOGS", "pg_watchdog_write"),
    "app.memory_samples": ("chain 10", "KS_WRITE_WATCHDOGS", "pg_watchdog_write"),
    "app.weekly_report_sends": ("chain 11a", "KS_WRITE_WEEKLY_LEDGER", "pg_weekly_ledger_write"),
    "app.traffic_report_sends": ("chain 11b", "KS_WRITE_TRAFFIC_LEDGER", "pg_traffic_ledger_write"),
}

# Created by 0025, 0026 and 0028 with no comment; 0035's downgrade clears them.
NEVER_COMMENTED = {
    "app.seasonal_indices", "app.growth_metrics", "app.weekly_patterns",
    "app.revenue_predictions", "app.data_quality_runs", "app.data_quality_issues",
    "app.data_quality_diffs", "app.disk_samples", "app.data_dir_samples",
    "app.memory_samples", "app.weekly_report_sends", "app.traffic_report_sends",
}


def _module():
    return replay.load(REV)


class TestTheLock:
    def test_it_follows_0034_and_stays_under_the_head_the_code_requires(self):
        """Kills: 0035 hung off anything but chain 4's lock, or a head that no
        longer descends from it — the pin left behind, so the lock does
        nothing. Not "is the head": a later revision keeps the lock as long as
        it descends from this one, and must not fail this test."""
        from core import pg

        rev = _module()
        assert rev.revision == REV_ID
        assert rev.down_revision == "0034_buyer_chain"
        assert REV_ID in [m.revision for m in replay.lineage(pg.REQUIRED_REVISION)]

    def test_both_directions_are_comment_on_table_and_nothing_else(self):
        """Kills: anything with a cost slipped into a revision whose only job
        is the pin — an ALTER, an index, a backfill — or a statement that does
        not parse as the one shape `migration_replay` follows."""
        rev = _module()
        for function in ("upgrade", "downgrade"):
            sql = replay.statements(rev, function)
            assert len(sql) == len(TABLES) == 20, (function, len(sql))
            assert all(replay.single_comment(s) for s in sql), function

    def test_it_comments_exactly_the_twenty_tables(self):
        """Kills: a table left out, or one that is no chain's commented."""
        rev = _module()
        up = {replay.single_comment(s)[0] for s in replay.statements(rev, "upgrade")}
        down = {replay.single_comment(s)[0] for s in replay.statements(rev, "downgrade")}
        assert up == down == set(TABLES)

    def test_each_comment_names_its_chain_flag_and_writer(self):
        """Kills: a comment copied from a neighbour that names the wrong chain,
        the wrong `KS_WRITE_*` or the wrong module — the one thing a comment
        on these tables is for."""
        after = replay.apply({}, replay.statements(_module(), "upgrade"))
        for table, (chain, flag, module) in TABLES.items():
            text = after[table]
            assert f"({chain})" in text, table
            assert f"{flag}=postgres" in text, table
            assert f"core/{module}.py" in text, table

    def test_the_downgrade_puts_back_exactly_what_0034_left(self):
        """Every table's comment, not only the twenty: at 0034, then up, then
        down, the state is 0034's again. Kills: a restored text that drifted
        from 0002/0003/0005/0008/0020's by a character (0008's is rendered by
        an f-string loop), NULL where a comment stood, or a comment invented
        for one of the twelve tables that never had one."""
        at_0034 = replay.comments_at("0034_buyer_chain")
        rev = _module()
        up = replay.apply(at_0034, replay.statements(rev, "upgrade"))
        down = replay.apply(up, replay.statements(rev, "downgrade"))
        assert down == at_0034
        assert {t for t in TABLES if at_0034[t] is None} == NEVER_COMMENTED

    def test_the_upgrade_changes_every_one_of_them(self):
        """Kills: a table listed with the text it already had — counted as
        commented, saying nothing about who writes it now."""
        at_0034 = replay.comments_at("0034_buyer_chain")
        up = replay.apply(at_0034, replay.statements(_module(), "upgrade"))
        for table in TABLES:
            assert up[table] and up[table] != at_0034[table], table

    def test_the_replay_reads_concatenated_literals_whole(self):
        """The helper this file stands on. 0032 writes its comment as two
        adjacent literals, which SQL glues together; reading the first piece
        alone truncated it (found comparing the replay with a real 0034
        database). Kills: the helper going back to one literal."""
        text = replay.comments_at("0034_buyer_chain")["meta.chain_watermarks"]
        assert text.endswith("see revision 0032."), text
