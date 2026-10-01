"""Revision 0034, chain 4's lock: what it writes and what it puts back.

It changes no schema — three table comments — and exists so that an image
without chain 4 refuses a database that has it (`REQUIRED_REVISION`). Its
downgrade must put back exactly what 0011 and 0024 wrote, and clear the one
table that never had a comment, or a round trip would leave a comment no
revision owns. Parsed from the files, not restated.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

VERSIONS = Path(__file__).resolve().parents[2] / "migrations" / "versions"


def _comments(path: Path, function: str) -> dict:
    """`{table: text or None}` for every `COMMENT ON TABLE` a function runs."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    fn = next(n for n in tree.body
              if isinstance(n, ast.FunctionDef) and n.name == function)
    out = {}
    for call in ast.walk(fn):
        if not (isinstance(call, ast.Call) and call.args):
            continue
        try:
            sql = ast.literal_eval(call.args[0])
        except ValueError:
            continue
        m = re.match(r"COMMENT ON TABLE (\S+) IS (NULL|'(.*)')$", sql, re.S)
        if m:
            out[m.group(1)] = None if m.group(2) == "NULL" else m.group(3)
    return out


REV = VERSIONS / "0034_buyer_chain.py"


class TestTheLock:
    def test_it_is_the_head_the_code_requires(self):
        from core import pg

        assert pg.REQUIRED_REVISION == "0034_buyer_chain"
        src = REV.read_text(encoding="utf-8")
        assert re.search(r'^revision = "0034_buyer_chain"$', src, re.M)
        assert re.search(r'^down_revision = "0033_derivation_signal"$', src, re.M)

    def test_it_comments_the_three_tables_and_touches_nothing_else(self):
        tree = ast.parse(REV.read_text(encoding="utf-8"))
        up = next(n for n in tree.body
                  if isinstance(n, ast.FunctionDef) and n.name == "upgrade")
        statements = [ast.literal_eval(c.args[0]) for c in ast.walk(up)
                      if isinstance(c, ast.Call) and c.args
                      and isinstance(c.args[0], (ast.Constant, ast.JoinedStr, ast.BinOp))]
        assert statements and all(s.startswith("COMMENT ON TABLE ") for s in statements)
        assert set(_comments(REV, "upgrade")) == {
            "bronze.buyers", "bronze.buyer_contacts", "app.buyer_gender"}

    def test_the_downgrade_puts_back_exactly_what_was_there(self):
        before = {**_comments(VERSIONS / "0011_buyers_lines_vitrina.py", "upgrade"),
                  **_comments(VERSIONS / "0024_buyer_gender.py", "upgrade")}
        restored = _comments(REV, "downgrade")
        assert restored["bronze.buyers"] == before["bronze.buyers"]
        assert restored["app.buyer_gender"] == before["app.buyer_gender"]
        assert "bronze.buyer_contacts" not in before, (
            "buyer_contacts has a comment after all; restore it instead of NULL")
        assert restored["bronze.buyer_contacts"] is None

    def test_the_new_text_names_the_chains_writer(self):
        for table, text in _comments(REV, "upgrade").items():
            assert "chain 4" in text and "KS_WRITE_BUYERS" in text or \
                table == "bronze.buyer_contacts", table
