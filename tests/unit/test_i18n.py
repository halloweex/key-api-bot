"""Three languages, and the guarantee a string table actually owes you.

The one failure a hand-written translation table invites is a key that exists
in English and nowhere else — invisible in review, invisible in tests that only
ever assert English, and visible to exactly one person: the reader who picked
Ukrainian and got half a report.
"""
from __future__ import annotations

from datetime import date

import pytest

from core.i18n import (
    DEFAULT_LANGUAGE,
    EN,
    LANGUAGE_NAMES,
    LANGUAGES,
    RU,
    UK,
    _STRINGS,
    fmt_int,
    fmt_money,
    fmt_window,
    normalize,
    t,
)


class TestCompleteness:
    def test_every_key_carries_every_language(self):
        missing = [
            f"{key}/{lang}"
            for key, entry in _STRINGS.items()
            for lang in LANGUAGES
            if not entry.get(lang)
        ]
        assert missing == []

    def test_every_language_has_a_name_to_show_in_the_picker(self):
        assert set(LANGUAGE_NAMES) == set(LANGUAGES)

    def test_placeholders_match_across_languages(self):
        """A translation that drops `{share}` silently loses the number.

        `t()` swallows a bad format rather than raising, which is right at
        runtime and exactly why the mismatch has to be caught here instead.
        """
        import re

        for key, entry in _STRINGS.items():
            expected = set(re.findall(r"{(\w+)}", entry[EN]))
            for lang in LANGUAGES:
                assert set(re.findall(r"{(\w+)}", entry[lang])) == expected, (
                    f"{key}/{lang} does not use the same placeholders as English"
                )


class TestLookup:
    def test_it_translates(self):
        assert t("report.orders", UK) == "Замовлення"
        assert t("report.orders", RU) == "Заказы"
        assert t("report.orders", EN) == "Orders"

    def test_an_unknown_key_returns_itself_rather_than_raising(self):
        """One odd line in a delivered report beats no report at all."""
        assert t("report.nonexistent", UK) == "report.nonexistent"

    def test_an_unknown_language_falls_back_to_english(self):
        assert t("report.orders", "de") == "Orders"

    def test_a_missing_placeholder_leaves_the_sentence_standing(self):
        assert t("report.vs_average", EN) == "vs {weeks}-week average"

    def test_it_formats(self):
        assert t("report.vs_average", RU, weeks=12) == "против среднего за 12 нед."


class TestNormalize:
    @pytest.mark.parametrize("given,expected", [
        ("uk", UK), ("UK", UK), ("uk-UA", UK), ("ru_RU", RU), ("en-GB", EN),
        ("de", DEFAULT_LANGUAGE), ("", DEFAULT_LANGUAGE), (None, DEFAULT_LANGUAGE),
    ])
    def test_it_accepts_what_telegram_sends(self, given, expected):
        """`language_code` arrives as "uk-UA", not "uk"."""
        assert normalize(given) == expected


class TestNumbers:
    def test_english_groups_with_commas_and_the_others_with_spaces(self):
        assert fmt_int(968_638, EN) == "968,638"
        assert fmt_int(968_638, UK) == "968 638"
        assert fmt_int(968_638, RU) == "968 638"

    def test_the_space_is_non_breaking(self):
        """A number that wraps mid-digit in a Telegram bubble is unreadable."""
        assert " " not in fmt_int(1_234_567, UK)

    def test_money_keeps_the_currency_off_the_digits(self):
        """DejaVu sets ₴ tight enough that "₴9" reads as one glyph."""
        assert fmt_money(2_729, EN) == "₴ 2,729"


class TestWindow:
    def test_the_week_is_numeric_in_every_language(self):
        """Month names inflect in Ukrainian and Russian — a date takes the
        genitive — so a table of nominative month names is a table of wrong
        ones. Numbers cannot be declined wrong.
        """
        window = fmt_window(date(2026, 5, 18), date(2026, 5, 24), UK)
        assert window == "18.05 – 24.05.2026"
        assert all(
            fmt_window(date(2026, 5, 18), date(2026, 5, 24), lang) == window
            for lang in LANGUAGES
        )
