"""Cost is joined one row per SKU, everywhere.

`offer_stocks` is unique only on an offer's `id`: a SKU can sit on two offers,
and a query joining the table by SKU then repeats every line of that SKU, its
revenue twice and both costs. The Rust port of /margin found it there (fixed
with `_SKU_COST`, core/repositories/margin.py); the SMS audience
(`core.sql_dialect._SMS_SEGMENTS_BODY`) and the campaign results
(`get_sms_campaign_results`) joined the same way. Production held no such SKU
when they were fixed (935 offers with a SKU, 935 distinct, 2026-11-04);
nothing forbids one.

So no SQL in the code joins the table itself on `sku`: it joins a source that
is one row per SKU. Read from the string literals, f-string parts included, so
a comment in the Python can neither satisfy this nor trip it.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
TREES = ("core", "web", "bot", "scripts")

# The table, as the hole a renderer fills ({offer_stocks}, {stocks}) or by
# name, joined straight on a condition that names `sku`.
DIRECT = re.compile(
    r"JOIN\s+(?:\{(?:offer_stocks|stocks)\}|(?:bronze\.)?offer_stocks)\s+\w+\s+ON\b[^\n]*\bsku\b",
    re.IGNORECASE,
)
ONE_ROW_PER_SKU = re.compile(
    r"SELECT\s+sku,\s+MAX\(purchased_price\)\s+AS\s+purchased_price\s+FROM\s+"
    r"\{(?:offer_stocks|stocks)\}\s+WHERE\s+sku\s+IS\s+NOT\s+NULL\s+GROUP\s+BY\s+sku",
    re.IGNORECASE,
)


def _sql_strings():
    """(path, line, text) of every string literal under TREES; an f-string is
    read whole, its expressions as `{…}`, and its literal parts on their own."""
    for tree in TREES:
        for path in sorted((REPO / tree).rglob("*.py")):
            module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(module):
                if isinstance(node, ast.JoinedStr):
                    text = "".join(
                        part.value if isinstance(part, ast.Constant) else "{…}"
                        for part in node.values
                    )
                    yield path, node.lineno, text
                elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                    yield path, node.lineno, node.value


def test_no_sql_joins_offer_stocks_straight_on_sku():
    straight = [
        f"{path.relative_to(REPO)}:{line}"
        for path, line, text in _sql_strings()
        if DIRECT.search(text)
    ]
    assert not straight, (
        "offer_stocks joined straight on sku — a SKU on two offers repeats its "
        "lines; join one row per SKU instead (margin's `_SKU_COST`):\n  "
        + "\n  ".join(straight)
    )


def test_the_walk_reads_the_one_row_per_sku_sources():
    """A walk that read nothing would pass the test above."""
    sources = {
        str(path.relative_to(REPO))
        for path, _line, text in _sql_strings()
        if ONE_ROW_PER_SKU.search(text)
    }
    assert {"core/repositories/margin.py", "core/sql_dialect.py",
            "core/repositories/customers.py"} <= sources, sources
