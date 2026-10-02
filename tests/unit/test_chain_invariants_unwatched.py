"""A blind part of the standing watch is one finding under its own name.

`app.data_quality_issues` is keyed on `(run_id, check_name, table_name)`.
`unwatched_issue` used to file every blindness under one constant table name,
so an integrity run in which two reader groups — or one group and the
watermarks — could not be read carried two `chain_invariants_unwatched`
findings under one key. Under `KS_WRITE_DQ_JOURNAL=postgres` Postgres refused
the whole run (it landed in neither store, and the failed attempt latched the
chain); under `duckdb` DuckDB took it and the hourly copy of the journal, never
pruned, failed every hour from then on.

The cases are walked from `Facts` itself — every field typed `Group` — and
from `_reader_groups`, never listed, so a group added later is in them. Every
guard names its mutation.
"""
from __future__ import annotations

import dataclasses
from datetime import datetime, timezone

import pytest

from core import pg_chain_invariants as inv

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
UNREGISTERED = "pg_not_a_registered_write"


def _group_fields():
    return sorted(f.name for f in dataclasses.fields(inv.Facts) if f.type == "Group")


def _chain_of(group: str) -> str:
    (chain,) = [c for c, g in inv._reader_groups().items() if g == group]
    return chain


def _keys(issues):
    return [(i.check_name, i.table_name) for i in issues]


class TestTheWalkSeesEveryGroup:
    def test_every_group_field_has_a_reader_and_every_reader_a_field(self):
        """The cases below are only as wide as this walk. Mutation: a reader
        group added under a field typed anything but `Group` — it would be
        left out of every case here."""
        assert _group_fields() == sorted(set(inv._reader_groups().values()))


class TestEveryBlindPartIsItsOwnFinding:
    def test_everything_blind_at_once_is_one_finding_per_part(self):
        """Every group, the watermarks and a chain with no reader, all blind in
        one run. Mutation: `table_name="(write chains)"` back as a constant in
        `unwatched_issue` — every one of these would share a key, and the run
        would be refused by Postgres or poison the hourly copy."""
        groups = _group_fields()
        facts = inv.Facts(
            watched=tuple(sorted(inv._reader_groups())) + (UNREGISTERED,),
            now=NOW,
            watermarks_unread=inv.Unwatched("QueryCanceledError: statement timeout"),
            unread=(UNREGISTERED,),
            **{g: inv.Unwatched("QueryCanceledError: statement timeout") for g in groups},
        )
        keys = _keys(inv.check_chain_invariants(facts))
        assert len(keys) == len(groups) + 2
        assert len(set(keys)) == len(keys), keys

    @pytest.mark.parametrize("group", _group_fields())
    def test_one_chain_its_group_and_the_watermarks(self, group):
        """One chain watched, its group and the watermarks blind together —
        the case a derivation from `watched` alone would still collide on,
        since both name the same single chain. Mutation: drop
        `WATERMARKS_PART` at the watermarks' call site."""
        chain = _chain_of(group)
        facts = inv.Facts(watched=(chain,), now=NOW,
                          watermarks_unread=inv.Unwatched("timeout"),
                          **{group: inv.Unwatched("timeout")})
        keys = _keys(inv.check_chain_invariants(facts))
        assert keys == [(inv.UNWATCHED, f"({chain})"),
                        (inv.UNWATCHED, inv.WATERMARKS_PART)]

    def test_the_reviewed_pair_is_two_keys(self):
        """The review's reproduction: the journal and the watchdogs blinded by
        one timeout."""
        facts = inv.Facts(watched=("pg_dq_journal_write", "pg_watchdog_write"), now=NOW,
                          journal=inv.Unwatched("QueryCanceledError: statement timeout"),
                          watchdogs=inv.Unwatched("QueryCanceledError: statement timeout"))
        assert _keys(inv.check_chain_invariants(facts)) == [
            (inv.UNWATCHED, "(pg_dq_journal_write)"),
            (inv.UNWATCHED, "(pg_watchdog_write)")]

    def test_a_blind_part_inside_a_read_group_is_its_own_finding(self):
        """Chain 6 reads its group and then finds a table it cannot judge — no
        full write recorded in `meta.mirror_state` — once per table. A fresh
        Postgres has neither, and both used to be filed under
        `(pg_catalogue_write)`, beside the watermarks' blindness in the same
        run. Mutation: drop `part=t.table` in `_catalogue_issues` — two
        findings under one key, the run Postgres refuses whole."""
        from core import pg_catalogue_write

        facts = inv.Facts(
            watched=(pg_catalogue_write.CHAIN,), now=NOW,
            watermarks_unread=inv.Unwatched("timeout"),
            catalogue=inv.Catalogue(tables=tuple(
                inv.CatalogueTable(table, rows=10)
                for table in pg_catalogue_write.CHAIN_TABLES)))
        keys = _keys(inv.check_chain_invariants(facts))
        assert keys == [(inv.UNWATCHED, table)
                        for table in pg_catalogue_write.CHAIN_TABLES] + [
            (inv.UNWATCHED, inv.WATERMARKS_PART)]
        assert len(set(keys)) == len(keys), keys

    def test_a_whole_blindness_keeps_its_name(self):
        """The one-finding cases keep `(write chains)` — the whole read, a
        forgotten pre-read, a verdict that raised — whatever `watched` holds."""
        for watched in (("pg_expenses_write",), ("pg_expenses_write", "pg_goals_write")):
            (issue,) = inv.check_chain_invariants(inv.Facts.blind(watched, "down"))
            assert issue.table_name == inv.WHOLE_PART

    def test_every_blindness_still_holds_every_condition(self):
        """The name moved; what a blind run does did not."""
        facts = inv.Facts(watched=("pg_dq_journal_write",), now=NOW,
                          journal=inv.Unwatched("boom"))
        assert inv.unverified_conditions(inv.check_chain_invariants(facts)) == sorted(
            inv.CONDITIONS)
