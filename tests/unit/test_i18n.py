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
from tests.integration.pg_tab_seed import needs_postgres


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
    @pytest.mark.parametrize("lang", LANGUAGES)
    def test_it_translates_into_the_language_asked_for(self, lang):
        assert t("report.orders", lang) == _STRINGS["report.orders"][lang]

    def test_an_unknown_key_returns_itself_rather_than_raising(self):
        """One odd line in a delivered report beats no report at all."""
        assert t("report.nonexistent", UK) == "report.nonexistent"

    def test_an_unknown_language_falls_back_to_english(self):
        assert t("report.orders", "de") == "Orders"

    @pytest.mark.parametrize("lang", LANGUAGES)
    def test_a_missing_placeholder_leaves_the_sentence_standing(self, lang):
        """No value for `{weeks}`, or values for other names: the template
        comes back whole rather than an error."""
        template = _STRINGS["report.vs_average"][lang]
        assert t("report.vs_average", lang) == template
        assert t("report.vs_average", lang, share=3) == template

    @pytest.mark.parametrize("lang", LANGUAGES)
    def test_it_formats(self, lang):
        text = t("report.vs_average", lang, weeks=12)
        assert "12" in text and "{weeks}" not in text


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


class TestTrafficPlatformsAreNamed:
    """Every platform the traffic tab can show has a label in the table.

    `_platform_label` falls back to the raw key on purpose, so a new platform
    reaches the report rather than disappearing from it. That fallback protects
    the *runtime*; it is not meant to be the delivered spelling, and when it is,
    nobody finds out until a Monday — `telegram` reached the first traffic
    report in lowercase beside "Instagram" and "Google Ads", which is what this
    test exists to stop happening twice.

    Read off the tab's answer: one order per verdict the classifier gives over
    a sweep of inputs, plus orders with no UTM row on each source the tab
    places by its source, over the test Postgres.
    """

    @staticmethod
    async def _populate(conn, pool):
        from datetime import datetime, timezone

        from core.utm_classify import classify_traffic
        from tests.helpers.utm_classify_inputs_chains_reports import CLASSIFY_INPUTS
        from tests.integration.pg_tab_seed import derive, seed

        verdicts = sorted({classify_traffic(d) for d in CLASSIFY_INPUTS})
        when = datetime(2026, 9, 2, 9, tzinfo=timezone.utc)
        # Orders 1..n carry a verdict; the next four have no UTM row, on the
        # sources the tab places by themselves (Instagram, Telegram, the
        # offline store, and Shopify/the fair, which it cannot place).
        bare = [(len(verdicts) + 1 + i, source) for i, source in enumerate((1, 2, 7, 4, 5))]
        await seed(conn, orders=[
            (oid, source, 1, 100, when, oid, None, None)
            for oid, source in [(i + 1, 4) for i in range(len(verdicts))] + bare])
        await derive(pool)
        await conn.executemany(
            "INSERT INTO silver.order_utm (order_id, traffic_type, platform, parsed_at)"
            " VALUES ($1, $2, $3, now())",
            [(i + 1, traffic_type, platform)
             for i, (traffic_type, platform) in enumerate(verdicts)])

    @needs_postgres
    @pytest.mark.asyncio
    async def test_the_tab_names_no_platform_the_table_cannot_label(self, monkeypatch):
        import os

        from tests.helpers.app_client import signed_in
        from tests.integration.pg_tab_seed import tab_in_postgres

        async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, self._populate):
            async with signed_in("viewer", tabs="traffic") as viewer:
                r = await viewer.get("/api/traffic/analytics", params={
                    "start_date": "2026-09-01", "end_date": "2026-09-03",
                    "sales_type": "all"})
        assert r.status_code == 200, r.text
        shown = set(r.json()["by_platform"])
        # The sweep reaches the classifier's platforms and the SQL fallback's.
        assert {"facebook", "instagram", "telegram", "offline_store",
                "unattributed", "google_ads", "google_organic"} <= shown
        unnamed = sorted(p for p in shown if f"traffic.platform.{p}" not in _STRINGS)
        assert unnamed == [], (
            f"these platforms would reach a report as their raw key: {unnamed}")
