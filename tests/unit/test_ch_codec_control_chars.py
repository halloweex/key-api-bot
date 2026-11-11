"""The ClickHouse TSV codec round-trips every control character (L2-12).

ClickHouse's TabSeparated format escapes backspace, form feed and NUL as
`\\b`, `\\f` and `\\0`. `unescape` knew only `\\t`, `\\n`, `\\r` and `\\\\`, so
`PROMO\\bX\\fY` read back as `PROMObXfY`: a promocode or comment carrying one
came back from the round trip as another string, and the zero-tolerance
comparison blamed the copy. Both directions know ClickHouse's set now.
"""
from __future__ import annotations

import pytest

from core.ch_common import escape, unescape


@pytest.mark.parametrize("raw,escaped", [
    ("PROMO\x08X\x0cY", "PROMO\\bX\\fY"),
    ("a\x00b", "a\\0b"),
    ("tab\there", "tab\\there"),
    ("line\nbreak\r", "line\\nbreak\\r"),
    ("back\\slash", "back\\\\slash"),
    ("back\\0slash", "back\\\\0slash"),
])
def test_escape_writes_clickhouse_s_escapes(raw, escaped):
    """Mutation: drop the three new pairs from `_ESCAPES`."""
    assert escape(raw) == escaped


@pytest.mark.parametrize("raw", [
    "PROMO\x08X\x0cY", "a\x00b", "every \x08\x0c\x00\t\n\r\\ one",
    "\\b is not a backspace", "", "Олена \\ кавычки ' \"",
])
def test_every_value_comes_back_as_it_went(raw):
    assert unescape(escape(raw)) == raw


def test_the_reproduction():
    """Mutation: drop `b`, `f`, `0` from `_UNESCAPES` — 'PROMObXfY'."""
    assert unescape("PROMO\\bX\\fY") == "PROMO\x08X\x0cY"
