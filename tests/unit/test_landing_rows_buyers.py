"""The one reading of a KeyCRM buyer, for both stores.

Each writer used to render the Buyer model for itself, and they had drifted: the
DuckDB writer passed the raw birthday string to DuckDB's CAST, the Postgres
mirror cut it with `date.fromisoformat(v[:10])`. So `1990-1-5` landed in one
store and failed the other, and a birthday DuckDB refuses rolled back the whole
batch — once a minute, taking the offers and stocks syncs behind it along.

Now both are handed `core.landing_rows.parse_buyers`, which makes them agree by
construction. What these tests protect is that the parser changes no birthday
DuckDB accepted before: every case below is judged against DuckDB 1.5.5's own
CAST, run in this process, not against what somebody believes it does.
"""
from __future__ import annotations

import ast
import inspect
import re
import textwrap
from datetime import date, datetime, timezone

import duckdb
import pytest

from core import landing_rows
from core.landing_rows import (
    BUYER_COLUMNS, CONTACT_COLUMNS, UNKNOWN_NAME, ContactRow, _as_date,
    birthday_is_unreadable, buyer_row, contact_rows, parse_buyers,
)


class _Buyer:
    def __init__(self, **kw):
        self.id = kw.pop("id", 1)
        for field in BUYER_COLUMNS:
            if field != "id":
                setattr(self, field, None)
        self.full_name = "Олена Петренко"
        self.phones = None
        self.emails = None
        for k, v in kw.items():
            setattr(self, k, v)


# Every shape probed against DuckDB while writing the parser, plus the ones a
# person would type. Kept broad on purpose: a case added here is judged by
# DuckDB, so it can only make the test stricter.
DATE_CASES = [
    "1990-01-05", "1990-1-5", " 1990-01-05 ", "1990-01-5 ", "1990-1-05",
    "1990-01-05T10:00:00", "1990-01-05 10:00:00", "1990-01-05T10:00:00+03:00",
    "1990-01-05Z", "1990-01-05T", "1990-01-05\t10:00", "1990-01-05\n",
    "1990-01-05 garbage", "1990-01-05garbage", "1990-01-05x", "1990-01-05:",
    "1990-1-5-", "1990-01-05 12", "1990-01-05+03", "1990-01-05.5",
    "1990-01-05 BC", "1990-01-05\x00",
    "1990/01/05", "1990 01 05", "1990\\01\\05",
    "90-01-05", "19-01-05", "199-01-05", "1-1-1", "0001-01-01", "9999-12-31",
    "2000-02-29",
    # refused by DuckDB, or not representable in Python
    "", "  ", "0000-00-00", "1990-02-30", "1900-02-29", "1990-13-01",
    "1990-00-05", "1990-01-0", "1990-01-32", "05.01.1990", "1990.01.05",
    "05-01-1990", "05/01/1990", "5-1-1990", "19900105", "1990-001-05",
    "1990-01-051", "1990-01/05", "+1990-01-05", "-1990-01-05", "0000-01-01",
    "12345-01-01", "1990-01-05 (BC)",
    # Found by PR-1's review, each disagreeing with the first parser: the BC
    # tail in any case and after exactly one ASCII space; `epoch`; leading
    # zeros in the year; a NUL AFTER the day; Unicode spaces and digits DuckDB
    # does not count as either.
    "1990-01-05 (bc)", "1990-01-05 (Bc)", "1990-01-05(BC)", "1990-01-05  (BC)",
    "1990-01-05\t(BC)", "1990-01-05\n(bc)", "1990-01-05\v(BC)", "1990-01-05\f(BC)",
    "1990-01-05\r(BC)", "1990-01-05 (bc)x",
    "epoch", "EPOCH", " epoch", "epoch ", "epochx",
    "01990-01-05", "001990-01-05", "0000000001990-01-05",
    "1990-01-05\x001", "1990-01-05\u0661", "\u0661990-01-05", "1990-0\u0661-05",
    "\u00a01990-01-05", "1990-01-05\u00a0(BC)", "\u20031990-01-05",
]

# Where the parser disagrees with DuckDB on purpose, and why. Anything else
# disagreeing fails.
INTENDED = {
    # A LEADING run of NULs is skipped, where DuckDB refuses it — the rule that
    # strips NUL from every text value, applied only where it cannot change a
    # date. No production birthday carries one.
    "\x001990-01-05": date(1990, 1, 5),
}


def _duckdb_reads(text):
    """DuckDB's CAST, reduced to what Python can hold: a date or None."""
    conn = duckdb.connect()
    try:
        value = conn.execute("SELECT CAST(? AS DATE)", [text]).fetchone()[0]
    except duckdb.Error:
        return None
    # A BC date comes back as a string such as '1991-01-05 (BC)', and a year
    # past 9999 the same way: DuckDB can hold them, asyncpg and Python cannot.
    return value if isinstance(value, date) else None


class TestBirthdaysReadAsDuckDBReadsThem:
    @pytest.mark.parametrize("text", DATE_CASES)
    def test_every_case_matches_duckdb(self, text):
        assert _as_date(text) == _duckdb_reads(text), text

    @pytest.mark.parametrize("text,expected", sorted(INTENDED.items()))
    def test_the_intended_differences_are_the_only_ones(self, text, expected):
        assert _duckdb_reads(text) != expected   # still a difference…
        assert _as_date(text) == expected        # …and still the one we chose

    def test_a_date_or_datetime_passes_through(self):
        assert _as_date(date(1990, 1, 5)) == date(1990, 1, 5)
        assert _as_date(datetime(1990, 1, 5, 23, 59)) == date(1990, 1, 5)
        assert _as_date(None) is None

    @pytest.mark.parametrize("text", ["", "0000-00-00", "1990-02-30", "05.01.1990"])
    def test_what_duckdb_refuses_is_unreadable_not_an_exception(self, text):
        assert _as_date(text) is None
        assert birthday_is_unreadable(text)

    def test_no_birthday_is_not_an_unreadable_one(self):
        assert not birthday_is_unreadable(None)
        assert not birthday_is_unreadable("1990-01-05")


class TestTheRow:
    def test_nul_is_stripped_from_every_text_value(self):
        row = buyer_row(_Buyer(
            full_name="Оле\x00на", note="a\x00b", phone="\x00+380", email="e\x00@x",
            company_name="c\x00", city="Київ\x00", region="\x00r",
            loyalty_program_name="p\x00", loyalty_level_name="l\x00"))
        texts = [v for v in row if isinstance(v, str)]
        assert texts and not any("\x00" in v for v in texts)
        assert row.full_name == "Олена"

    @pytest.mark.parametrize("name", ["", "   ", "\t\n", "\x00\x00", None])
    def test_a_blank_name_is_the_unknown_one(self, name):
        assert buyer_row(_Buyer(full_name=name)).full_name == UNKNOWN_NAME

    def test_a_real_name_is_kept_exactly_padding_and_all(self):
        # Only blank becomes Unknown. Trimming a real name would rewrite a
        # value both stores already hold and make it a difference.
        assert buyer_row(_Buyer(full_name=" Олена ")).full_name == " Олена "

    def test_an_unreadable_birthday_is_none_and_the_row_still_builds(self):
        assert buyer_row(_Buyer(birthday="1990-02-30")).birthday is None

    def test_timestamps_stay_strict(self):
        stamp = datetime(2026, 8, 27, 12, tzinfo=timezone.utc)
        assert buyer_row(_Buyer(created_at=stamp)).created_at == stamp
        with pytest.raises(ValueError):
            buyer_row(_Buyer(created_at="вчора ввечері"))


class TestContactsAsTheyHaveAlwaysBeenWritten:
    def test_first_occurrence_wins_with_its_own_primary_flag(self):
        # Reads like a bug and is pinned as it stands: both stores have always
        # written it this way, and changing it would turn every such buyer into
        # a difference between them.
        buyer = _Buyer(phones=["", "p", "p"], phone="")
        assert contact_rows(buyer) == [ContactRow(1, "phone", "p", False)]
        assert buyer_row(buyer).phone == ""

    def test_an_object_or_list_among_the_phones_is_skipped_not_raised(self):
        """KeyCRM documents strings. An object used to reach DuckDB as a bound
        parameter and be coerced to text; in the dict that collapses duplicates
        it cannot even be hashed, and the whole portion would fail."""
        buyer = _Buyer(phones=["p", {"v": "q"}, ["a"]], phone={"v": "q"})
        assert contact_rows(buyer) == [ContactRow(1, "phone", "p", True)]
        assert buyer_row(buyer).phone is None     # Postgres would refuse a dict
        assert parse_buyers([buyer]).rows[0].id == 1

    def test_a_number_is_kept_as_its_digits(self):
        rows = contact_rows(_Buyer(phones=[380501112233]))
        assert rows == [ContactRow(1, "phone", "380501112233", True)]

    def test_nul_in_a_contact_is_stripped_and_an_all_nul_one_skipped(self):
        rows = contact_rows(_Buyer(phones=["+38\x0050", "\x00"], emails=["a@b.ua"]))
        assert rows == [ContactRow(1, "phone", "+3850", True),
                        ContactRow(1, "email", "a@b.ua", True)]


class TestTheBatch:
    def test_unreadable_birthdays_are_reported_by_id_only(self):
        parsed = parse_buyers([
            _Buyer(id=7, birthday="1990-01-05"),
            _Buyer(id=8, birthday="0000-00-00"),
            _Buyer(id=9, birthday=None),
        ])
        assert parsed.unreadable_birthdays == [8]
        assert [r.id for r in parsed.rows] == [7, 8, 9]
        assert [bid for bid, _ in parsed.contacts] == [7, 8, 9]


def _sql_in(func, table):
    """The column list of the INSERT naming `table` inside `func`, parsed."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            m = re.search(rf"INSERT(?: OR REPLACE)? INTO {table}\s*\(([^)]*)\)",
                          node.value)
            if m:
                return [c.strip() for c in m.group(1).split(",")]
    raise AssertionError(f"no INSERT INTO {table} in {func.__qualname__}")


class TestTheDuckDBWriterFollowsTheContract:
    """The DuckDB writer — `_upsert_buyer_portion`, one transaction of the
    portions `upsert_buyers` splits a batch into — passes each row POSITIONALLY. Reordering `BuyerRow`
    would shift every value into its neighbour's column without an error — a
    birthday in `note`, a phone in `email`. This is the only thing that says so."""

    def test_the_buyers_insert_lists_the_contract_in_order(self):
        from core.duckdb_store import DuckDBStore
        assert _sql_in(DuckDBStore._upsert_buyer_portion, "buyers") == \
            [*BUYER_COLUMNS, "synced_at"]

    def test_the_contacts_insert_lists_the_contract_in_order(self):
        from core.duckdb_store import DuckDBStore
        assert _sql_in(DuckDBStore._upsert_buyer_portion, "buyer_contacts") == \
            list(CONTACT_COLUMNS)

    def test_the_contract_has_one_home(self):
        from core import pg_buyers
        # The mirror builds its SQL from the same tuple the parser fills...
        assert pg_buyers.BUYER_COLUMNS is landing_rows.BUYER_COLUMNS
        # ...and nobody re-exports it: the comparison imports it from home.
        assert not hasattr(pg_buyers, "CONTACT_COLUMNS")
