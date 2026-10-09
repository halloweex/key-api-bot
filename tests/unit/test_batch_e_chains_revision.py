"""Revision 0035, the lock on images older than the eight chains #280 registered.

It changes no schema — twenty table comments — and exists so that an image
without chains 3, 5, 6, 7b-3, 9, 10, 11a and 11b refuses a database that has
them (`REQUIRED_REVISION`). What was there before is not restated here: it is
read by replaying every revision up to 0034 against a recorder
(`tests/migration_replay.py`), which reproduces all 57 table comments of a real
0034 database. Each test names the mutation it kills.
"""
from __future__ import annotations

from tests import migration_replay as replay

REV_ID = "0035_batch_e_chains"
REV = replay.VERSIONS / f"{REV_ID}.py"

# The eight chains #280 registered, by module, with the name CLAUDE.md gives
# each. Their tables are read from the modules, never copied here.
BATCH_E = {
    "pg_orders_write": "chain 3",
    "pg_managers_write": "chain 5",
    "pg_catalogue_write": "chain 6",
    "pg_forecast_write": "chain 7b-3",
    "pg_dq_journal_write": "chain 9",
    "pg_watchdog_write": "chain 10",
    "pg_weekly_ledger_write": "chain 11a",
    "pg_traffic_ledger_write": "chain 11b",
}

def _module():
    return replay.load(REV)


def _chains():
    from core.write_chains import WRITE_CHAINS
    return {chain.__name__.rsplit(".", 1)[-1]: chain for chain in WRITE_CHAINS}


def _batch_e_tables():
    chains = _chains()
    return {table: name for name in BATCH_E for table in chains[name].CHAIN_TABLES}


class TestTheLock:
    def test_it_is_the_head_the_code_requires_and_follows_0034(self):
        """Kills: the pin left at 0034 (the lock does nothing), or 0035 hung
        off anything but chain 4's lock."""
        from core import pg

        rev = _module()
        assert pg.REQUIRED_REVISION == REV_ID
        assert rev.revision == REV_ID
        assert rev.down_revision == "0034_buyer_chain"

    def test_both_directions_are_comment_on_table_and_nothing_else(self):
        """Kills: anything with a cost slipped into a revision whose only job
        is the pin — an ALTER, an index, a backfill — or a statement that does
        not parse as the one shape `migration_replay` follows."""
        rev = _module()
        for function in ("upgrade", "downgrade"):
            sql = replay.statements(rev, function)
            assert len(sql) == 20, (function, len(sql))
            assert all(replay.single_comment(s) for s in sql), function

    def test_it_comments_exactly_the_eight_chains_tables(self):
        """Read from each module's `CHAIN_TABLES`. Kills: a chain table left
        out, a table that is no chain's commented, or a chain that grows a
        table this revision never named."""
        rev = _module()
        up = {replay.single_comment(s)[0] for s in replay.statements(rev, "upgrade")}
        down = {replay.single_comment(s)[0] for s in replay.statements(rev, "downgrade")}
        assert up == down == set(_batch_e_tables())

    def test_each_comment_names_its_chain_flag_and_writer(self):
        """Kills: a comment copied from a neighbour that names the wrong chain,
        the wrong `KS_WRITE_*` or the wrong module — the one thing a comment
        on these tables is for."""
        chains = _chains()
        after = replay.apply({}, replay.statements(_module(), "upgrade"))
        for table, name in _batch_e_tables().items():
            text = after[table]
            assert f"({BATCH_E[name]})" in text, table
            assert f"{chains[name].WRITE_ENV}=postgres" in text, table
            assert f"core/{name}.py" in text, table

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
        never = {t for t in _batch_e_tables() if at_0034[t] is None}
        assert len(never) == 12, sorted(never)

    def test_the_upgrade_changes_every_one_of_them(self):
        """Kills: a table listed with the text it already had — counted as
        commented, saying nothing about who writes it now."""
        at_0034 = replay.comments_at("0034_buyer_chain")
        up = replay.apply(at_0034, replay.statements(_module(), "upgrade"))
        for table in _batch_e_tables():
            assert up[table] and up[table] != at_0034[table], table

    def test_the_replay_reads_concatenated_literals_whole(self):
        """The helper this file stands on. 0032 writes its comment as two
        adjacent literals, which SQL glues together; reading the first piece
        alone truncated it (found comparing the replay with a real 0034
        database). Kills: the helper going back to one literal."""
        text = replay.comments_at("0034_buyer_chain")["meta.chain_watermarks"]
        assert text.endswith("see revision 0032."), text

