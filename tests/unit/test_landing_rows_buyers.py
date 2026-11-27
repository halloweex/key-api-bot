"""The one reading of a KeyCRM buyer, for both stores.

Each writer used to render the Buyer model for itself, and they had drifted: the
DuckDB writer passed the raw birthday string to DuckDB's CAST, the Postgres
mirror cut it with `date.fromisoformat(v[:10])`. So `1990-1-5` landed in one
store and failed the other, and a birthday DuckDB refuses rolled back the whole
batch — once a minute, taking the offers and stocks syncs behind it along.

Now both are handed `core.landing_rows.parse_buyers`, which makes them agree by
construction. What these tests protect is the reading itself: a birthday a
person could type becomes the date they meant, and one that cannot be a date
becomes None — never an exception, which cost the batch and the syncs behind
it. The rule was measured against DuckDB 1.5.5's own CAST when the parser was
written (80 shapes, the parser's comment); the cases below state its answers.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from core.landing_rows import (
    BUYER_COLUMNS, UNKNOWN_NAME, ContactRow, _as_date,
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


class TestBirthdays:
    @pytest.mark.parametrize("text,expected", [
        # Read as the date a person meant.
        ("1990-1-5", date(1990, 1, 5)),          # the incident: one store refused it
        (" 1990-01-05 ", date(1990, 1, 5)),
        ("1990-01-05T10:00", date(1990, 1, 5)),  # a time after the day
        ("1990/01/05", date(1990, 1, 5)),        # another separator, used twice
        ("01990-01-05", date(1990, 1, 5)),       # zeros in front of the year
        ("epoch", date(1970, 1, 1)),
        ("\x001990-01-05", date(1990, 1, 5)),    # a leading NUL is skipped
        # Not a date Python, or Postgres through asyncpg, could hold.
        ("1990-01-05 (BC)", None),
        ("0000-01-01", None),                    # year 0
        ("12345-01-01", None),                   # past 9999
        # Not a date at all.
        ("05-01-1990", None),                    # day first never reads as year 5
        ("1990-01-051", None),                   # a digit after the day
        ("\u0661990-01-05", None),               # ASCII digits only
        ("\xa01990-01-05", None),                # a no-break space in front
    ])
    def test_a_birthday_reads_as_a_date_or_none(self, text, expected):
        assert buyer_row(_Buyer(birthday=text)).birthday == expected
        assert birthday_is_unreadable(text) is (expected is None)

    def test_a_date_or_datetime_passes_through(self):
        assert _as_date(date(1990, 1, 5)) == date(1990, 1, 5)
        assert _as_date(datetime(1990, 1, 5, 23, 59)) == date(1990, 1, 5)
        assert _as_date(None) is None

    @pytest.mark.parametrize("text", ["", "0000-00-00", "1990-02-30", "05.01.1990"])
    def test_an_unreadable_one_is_none_not_an_exception(self, text):
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
