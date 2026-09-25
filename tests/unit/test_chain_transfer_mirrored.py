"""The copy-back learns mirrored tables (chain 4, PR-2, step 2.5).

`chain_specs` knew two shipping shapes — replaced whole, appended above a
watermark — and raised for anything else, so registering chain 4 with
`bronze.buyers` would have failed `test_every_chain_every_table` at once. The
buyers and their contacts are shipped by the mirror, from the same parse as
DuckDB, and nothing replaces them whole; this is the third source and the
handover rules that shape needs (decisions 5 and 6).

Everything here runs on a fake chain declaring chain 4's three tables: PR-2
registers no chain, and must not.
"""
from __future__ import annotations

import types
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from core import chain_transfer
from core.chain_transfer import chain_sequences, chain_specs, classify_handover
from core.landing_rows import BUYER_COLUMNS, CONTACT_COLUMNS
from core.mirror_reconciliation import _normalise_row

BUYERS = "bronze.buyers"
CONTACTS = "bronze.buyer_contacts"
GENDER = "app.buyer_gender"


def _fake_chain():
    fake = types.ModuleType("core.pg_buyers_fake_write")
    fake.CHAIN = "pg_buyers_fake_write"
    fake.WRITE_ENV = "KS_WRITE_BUYERS_FAKE"
    fake.CHAIN_TABLES = (BUYERS, CONTACTS, GENDER)
    return fake


@pytest.fixture
def specs():
    return {s.pg_table: s for s in chain_specs(_fake_chain())}


def _buyer(spec, bid, name="Олена", phone="+380501"):
    raw = {c: None for c in BUYER_COLUMNS}
    raw.update(id=bid, full_name=name, phone=phone,
               loyalty_discount=0.0, loyalty_amount=0.0,
               birthday=date(1990, 1, 5),
               created_at=datetime(2026, 9, 1, tzinfo=timezone.utc))
    row = tuple(raw[c] for c in spec.compare.columns)
    return _normalise_row(row, spec.compare.columns, spec.compare.numeric,
                          spec.compare.ignore_columns)


def _contact(spec, bid, value, primary=False):
    raw = {"buyer_id": bid, "contact_type": "phone", "value": value,
           "is_primary": primary}
    return tuple(raw[c] for c in spec.compare.columns)


def _names(issues):
    return {(i.check_name, i.severity.value) for i in issues}


class TestTheThirdSource:
    def test_three_specs_in_chain_order(self, specs):
        assert list(specs) == [BUYERS, CONTACTS, GENDER]
        assert [specs[t].kind for t in (BUYERS, CONTACTS, GENDER)] == [
            "mirrored", "mirrored", "operational"]

    def test_the_columns_are_what_the_shared_parse_writes(self, specs):
        assert specs[BUYERS].columns == BUYER_COLUMNS
        assert specs[CONTACTS].columns == CONTACT_COLUMNS

    def test_the_contact_key_is_the_natural_triple_without_the_duckdb_id(self, specs):
        assert specs[CONTACTS].compare.key_columns == (
            "buyer_id", "contact_type", "value")
        assert "id" not in specs[CONTACTS].columns

    def test_the_copy_back_compares_with_no_clock_and_no_grace(self, specs):
        for table in (BUYERS, CONTACTS):
            assert specs[table].compare.synced_column is None
            assert specs[table].clock == ()

    def test_each_mirrored_table_says_which_buyer_dates_it(self, specs):
        assert specs[BUYERS].rewritten_by == "id"
        assert specs[CONTACTS].rewritten_by == "buyer_id"

    def test_the_gender_verdict_hands_over_on_decided_at(self, specs):
        """Decision 7 made `decided_at` a compared value, which is what makes
        it the shared clock here."""
        assert specs[GENDER].kind == "operational"
        assert specs[GENDER].clock == ("decided_at",)

    def test_the_contact_allocator_has_no_postgres_counterpart(self):
        seqs = {s.dk_sequence: s for s in chain_sequences(_fake_chain())}
        assert seqs["seq_buyer_contacts_id"].pg_sequence == "bronze.buyer_contacts_id_seq"

    def test_a_mirrored_table_with_no_rewrite_clock_raises(self, monkeypatch):
        monkeypatch.setattr(chain_transfer, "_REWRITTEN_BY", {BUYERS: "id"})
        with pytest.raises(LookupError, match="_REWRITTEN_BY"):
            chain_specs(_fake_chain())

    def test_a_table_in_no_source_still_raises(self):
        fake = _fake_chain()
        fake.CHAIN_TABLES = ("bronze.nowhere",)
        with pytest.raises(LookupError, match="MIRRORED_LANDING_TABLES"):
            chain_specs(fake)


class TestBeforeTheFlip:
    """DuckDB is the writer and the mirror its only copy: the two must agree,
    and every disagreement names the lever that fixes it."""

    def test_a_buyer_only_in_duckdb_is_stranded_by_a_flip(self, specs):
        s = specs[BUYERS]
        issues = classify_handover(s, {1: _buyer(s, 1)}, {}, moved_on=False)
        assert _names(issues) == {("handover_rows_missing", "CRITICAL")}
        assert "/api/mirror/backfill/buyers" in issues[0].description

    def test_a_differing_buyer_names_the_reship(self, specs):
        s = specs[BUYERS]
        issues = classify_handover(s, {1: _buyer(s, 1)}, {1: _buyer(s, 1, "Інша")},
                                   moved_on=False)
        assert _names(issues) == {("handover_rows_differ", "CRITICAL")}
        assert "/api/mirror/backfill/buyers" in issues[0].description

    def test_a_contact_only_in_postgres_is_critical_and_reship_removes_it(self, specs):
        s = specs[CONTACTS]
        key = (1, "phone", "+380509")
        issues = classify_handover(s, {}, {key: _contact(s, *key[::2])},
                                   moved_on=False)
        assert _names(issues) == {("handover_rows_ahead", "CRITICAL")}
        assert "removes a contact" in issues[0].description

    def test_a_buyer_only_in_postgres_is_critical_and_needs_a_person(self, specs):
        s = specs[BUYERS]
        issues = classify_handover(s, {}, {1: _buyer(s, 1)}, moved_on=False)
        assert _names(issues) == {("handover_rows_ahead", "CRITICAL")}
        assert "never deletes a buyer" in issues[0].description

    def test_equal_stores_say_nothing(self, specs):
        s = specs[BUYERS]
        assert classify_handover(s, {1: _buyer(s, 1)}, {1: _buyer(s, 1)},
                                 moved_on=False) == []


class TestAfterTheLatch:
    """Postgres writes; a difference is the chain's only for a buyer it
    rewrote since the latch."""

    def test_a_differing_buyer_the_chain_rewrote_is_carried(self, specs):
        s = specs[BUYERS]
        issues = classify_handover(s, {1: _buyer(s, 1)}, {1: _buyer(s, 1, "Нова")},
                                   moved_on=True, rewritten=frozenset({1}))
        assert _names(issues) == {("handover_rows_differ", "INFO")}

    def test_a_differing_buyer_it_never_rewrote_refuses(self, specs):
        s = specs[BUYERS]
        issues = classify_handover(s, {1: _buyer(s, 1)}, {1: _buyer(s, 1, "Нова")},
                                   moved_on=True, rewritten=frozenset())
        assert _names(issues) == {("handover_rows_differ", "CRITICAL")}
        assert "sync-all-buyers" in issues[0].description

    def test_the_split_is_per_buyer(self, specs):
        s = specs[BUYERS]
        dk = {1: _buyer(s, 1), 2: _buyer(s, 2)}
        pg = {1: _buyer(s, 1, "Нова"), 2: _buyer(s, 2, "Нова")}
        issues = classify_handover(s, dk, pg, moved_on=True, rewritten=frozenset({1}))
        by = {i.severity.value: i for i in issues}
        assert by["INFO"].sample_ids == (1,) and by["CRITICAL"].sample_ids == (2,)

    def test_a_contact_the_chain_dropped_is_carried(self, specs):
        s = specs[CONTACTS]
        key = (1, "phone", "+380501")
        issues = classify_handover(s, {key: _contact(s, 1, "+380501")}, {},
                                   moved_on=True, rewritten=frozenset({1}))
        assert _names(issues) == {("handover_rows_missing", "INFO")}

    def test_a_contact_of_a_buyer_it_never_rewrote_refuses(self, specs):
        s = specs[CONTACTS]
        key = (1, "phone", "+380501")
        issues = classify_handover(s, {key: _contact(s, 1, "+380501")}, {},
                                   moved_on=True, rewritten=frozenset({2}))
        assert _names(issues) == {("handover_rows_missing", "CRITICAL")}

    def test_a_buyer_only_in_duckdb_refuses_whatever_else_was_rewritten(self, specs):
        """The chain's writer never deletes a buyer."""
        s = specs[BUYERS]
        issues = classify_handover(s, {1: _buyer(s, 1)}, {},
                                   moved_on=True, rewritten=frozenset({2}))
        assert _names(issues) == {("handover_rows_missing", "CRITICAL")}

    def test_rows_only_in_postgres_are_the_copy(self, specs):
        s = specs[BUYERS]
        issues = classify_handover(s, {}, {1: _buyer(s, 1)}, moved_on=True,
                                   rewritten=frozenset({1}))
        assert _names(issues) == {("handover_rows_ahead", "INFO")}

    def test_no_owner_row_for_the_buyers_means_nothing_is_the_chains(self, specs):
        """`rewritten` empty — `_rewritten_since_latch` with no buyers owner
        row — keeps every difference CRITICAL."""
        s = specs[BUYERS]
        issues = classify_handover(s, {1: _buyer(s, 1)}, {1: _buyer(s, 1, "Нова")},
                                   moved_on=True, rewritten=frozenset())
        assert _names(issues) == {("handover_rows_differ", "CRITICAL")}


class TestARowTheCopyCouldNotWrite:
    @pytest.mark.parametrize("moved_on", [False, True])
    def test_a_null_name_is_critical_in_the_handover_itself(self, specs, moved_on):
        """The copy's INSERT would fail on it; `--handover` must say so first."""
        s = specs[BUYERS]
        row = _buyer(s, 1)
        pos = s.compare.columns.index("full_name")
        broken = row[:pos] + (None,) + row[pos + 1:]
        issues = classify_handover(s, {1: row}, {1: broken}, moved_on=moved_on,
                                   required=("id", "full_name"),
                                   rewritten=frozenset({1}))
        assert ("handover_rows_unwritable", "CRITICAL") in _names(issues)
        unwritable = next(i for i in issues if i.check_name == "handover_rows_unwritable")
        assert unwritable.sample_ids == (1,)
        assert "full_name" in unwritable.description

    def test_the_required_columns_come_from_duckdbs_own_catalogue(self, specs, tmp_path):
        """Derived, never listed: `buyers.full_name` is NOT NULL in DuckDB."""
        from core.duckdb_store import DuckDBStore

        async def read():
            store = DuckDBStore(db_path=tmp_path / "c.duckdb")
            await store.connect()
            try:
                async with store.connection() as conn:
                    return (chain_transfer._not_null_columns(conn, specs[BUYERS]),
                            chain_transfer._not_null_columns(conn, specs[CONTACTS]))
            finally:
                await store.close()

        import asyncio
        buyers, contacts = asyncio.run(read())
        assert "full_name" in buyers and "id" in buyers
        assert "birthday" not in buyers
        assert {"buyer_id", "contact_type", "value"} <= set(contacts)


class TestOperationalTablesAreUnchanged:
    def test_a_clockless_difference_after_the_latch_is_still_postgres_newer(self):
        """The mirrored rules are additive: an operational table without a
        shared clock keeps the old INFO."""
        from core import pg_inventory_write

        spec = next(s for s in chain_specs(pg_inventory_write)
                    if s.pg_table == "app.sku_inventory_status")
        width = len(spec.compare.columns)
        a = tuple(Decimal("1") if c in spec.compare.numeric else i
                  for i, c in enumerate(spec.compare.columns))
        b = (a[0],) + tuple("x" for _ in range(width - 1))
        issues = classify_handover(spec, {a[0]: a}, {a[0]: b}, moved_on=True)
        assert _names(issues) == {("handover_rows_differ", "INFO")}


class TestTheRunbookTellsTheShippersApart:
    """A mirrored table is never stamped by `replicate_operational` and never
    listed under its `replaced`: the runbook must not send an operator there
    to wait for chain 4's buyers."""

    def test_the_mirrored_tables_are_sent_to_the_mirror(self):
        said = " ".join(chain_transfer._runbook(
            _fake_chain(), executed=True, released=True))
        operational, _, mirrored = said.partition("; and ")
        assert GENDER in operational and "replicate_operational" in operational
        assert BUYERS not in operational and CONTACTS not in operational
        assert BUYERS in mirrored and CONTACTS in mirrored
        assert "buyers mirror" in mirrored
        assert "replicate_operational" not in mirrored

    def test_a_marker_without_owner_rows_names_the_reship_for_this_chain_only(self):
        from core import pg_expenses_write

        mixed = " ".join(chain_transfer._marker_steps(_fake_chain(), "fake"))
        plain = " ".join(chain_transfer._marker_steps(pg_expenses_write, "pg_expenses_write"))
        assert "/api/mirror/backfill/buyers" in mixed
        assert "buyers mirror" not in plain

    def test_help_offers_every_registered_chain(self):
        from core.write_chains import WRITE_CHAINS
        from scripts import chain_copy_back as script

        names = script._short_names()
        assert len(names) == len(WRITE_CHAINS)
        for name, chain in zip(names, WRITE_CHAINS):
            assert chain_transfer.resolve_chain(name) is chain
