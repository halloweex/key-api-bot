"""The contract the Rust expense-type load (rust/crates/ks-worker) is held to.

The plan's К2 (revision 4), step 1: chain 6a's dictionary ported to Rust and
written beside `bronze.expense_types` before it ever writes it. What the load
does that is a text or a rule is exported here, from the Python modules that
own it, into `rust/contract/expense_types.json`:

* the table and its columns (`core.landing_rows.EXPENSE_TYPE_COLUMNS`), the
  localisation prefix, the name of a payload without one, and the upsert
  `core.pg_expense_types_write.upsert_expense_types` sends;
* what `core.landing_rows.expense_type_row` makes of every payload below: the
  row, or the exception it raises;
* what `str.title()` makes of every ASCII character in five positions, and of
  texts that are not ASCII, which the Rust port refuses rather than guesses.

`rust/crates/ks-worker/tests/k2_contract.rs` holds the Rust side to the file,
and `tests/unit/test_rust_k2_contract.py` holds the file to the app: a change
to the parse or the writer fails the Python suite until the file is
regenerated, and then the Rust suite until Rust follows. What asyncpg makes of
each parameter only a real asyncpg answers, so the harness asks it
(`tests/integration/test_rust_k2_expense_types.py`).

Every payload is synthetic: the repository is public.

Regenerate (in the Python image CI uses):

    python tests/rust_k2_contract.py
"""
from __future__ import annotations

import asyncio
import json
import pathlib
import sys
from typing import Any, Dict, List, Tuple
from unittest.mock import AsyncMock, patch

ROOT = pathlib.Path(__file__).resolve().parent.parent
PATH = ROOT / "rust" / "contract" / "expense_types.json"

KEY = "dictionaries.expense_types."

# Every shape `expense_type_row` distinguishes, and the accidents it keeps.
CASES: List[Tuple[str, Any]] = [
    # The three shapes.
    ("a display name is kept as typed",
     {"id": 1, "name": "Пакування замовлення", "alias": "packaging", "is_active": True}),
    ("a display name in ASCII", {"id": 2, "name": "Courier fee", "alias": None}),
    ("a key with an alias takes the alias",
     {"id": 3, "name": KEY + "delivery", "alias": "cash_on_delivery"}),
    ("a key without an alias takes its own tail", {"id": 4, "name": KEY + "bank_commission"}),
    ("a key with a null alias takes its own tail",
     {"id": 5, "name": KEY + "packaging_cost", "alias": None}),
    # Falsy aliases: the tail is the name, and the alias is still stored.
    ("an empty alias is falsy", {"id": 6, "name": KEY + "return_shipping", "alias": ""}),
    ("a zero alias is falsy", {"id": 7, "name": KEY + "other_costs", "alias": 0}),
    ("a 0.0 alias is falsy", {"id": 8, "name": KEY + "other_costs", "alias": 0.0}),
    ("a false alias is falsy", {"id": 9, "name": KEY + "other_costs", "alias": False}),
    ("an empty list alias is falsy", {"id": 10, "name": KEY + "other_costs", "alias": []}),
    ("an empty object alias is falsy", {"id": 11, "name": KEY + "other_costs", "alias": {}}),
    # The rule's own edges.
    ("every copy of the prefix goes", {"id": 12, "name": KEY + "a_" + KEY + "b"}),
    ("the bare prefix is an empty name", {"id": 13, "name": KEY}),
    ("a prefix not at the start is not a key", {"id": 14, "name": "x" + KEY + "delivery"}),
    ("the prefix's case matters", {"id": 15, "name": "Dictionaries.Expense_Types.delivery"}),
    ("an alias is used only for a key", {"id": 16, "name": "Plain", "alias": "some_alias"}),
    ("title: mixed case", {"id": 17, "name": KEY + "x", "alias": "dELIVERY_EXPRESS"}),
    ("title: apostrophes and digits", {"id": 18, "name": KEY + "x", "alias": "o'neil_2nd_day"}),
    ("title: punctuation and doubled underscores",
     {"id": 19, "name": KEY + "x", "alias": "cod-fee.v2__x"}),
    ("title: a space is a truthy alias", {"id": 20, "name": KEY + "x", "alias": " "}),
    # Defaults.
    ("no name is Unknown", {"id": 21}),
    ("no is_active is true", {"id": 22, "name": "x"}),
    ("a null is_active stays null", {"id": 23, "name": "x", "is_active": None}),
    ("a false is_active", {"id": 24, "name": "x", "is_active": False}),
    ("an empty object", {}),
    ("extra fields are ignored",
     {"id": 26, "name": "x", "color": "red", "nested": {"a": [1, {"b": None}]}}),
    # Passed through for asyncpg to type (the harness asks it).
    ("a float id", {"id": 27.0, "name": "x"}),
    ("a float id with a fraction", {"id": 28.9, "name": "x"}),
    ("true for an id", {"id": True, "name": "x"}),
    ("a string id", {"id": "30", "name": "x"}),
    ("a huge id", {"id": 10 ** 20, "name": "x"}),
    ("a number for is_active", {"id": 32, "name": "x", "is_active": 1}),
    ("text for is_active", {"id": 33, "name": "x", "is_active": "true"}),
    ("a number alias on a display name", {"id": 34, "name": "Plain", "alias": 7}),
    # Refused by the parse itself.
    ("a null name", {"id": 35, "name": None}),
    ("a number for a name", {"id": 36, "name": 5}),
    ("a list for a name", {"id": 37, "name": ["x"]}),
    ("a truthy number alias on a key", {"id": 38, "name": KEY + "x", "alias": 7}),
    ("a true alias on a key", {"id": 39, "name": KEY + "x", "alias": True}),
    ("a list alias on a key", {"id": 40, "name": KEY + "x", "alias": ["a"]}),
    ("a payload that is text", "delivery"),
    ("a payload that is a list", [1, 2]),
    ("a payload that is null", None),
    ("a payload that is a number", 5),
    # Text that is not ASCII, where no title is taken.
    ("a Cyrillic display name is never titlecased", {"id": 45, "name": "доставка нова пошта"}),
    ("a Cyrillic alias on a display name is stored as typed",
     {"id": 46, "name": "Plain", "alias": "доставка"}),
]

# Parsed by Python and refused by the Rust port on purpose: the name needs
# `str.title()` of text that is not ASCII (the module docs of
# rust/crates/ks-worker/src/landing/expense_types.rs).
GAPS: List[Tuple[str, Any]] = [
    ("a Cyrillic alias on a key", {"id": 47, "name": KEY + "x", "alias": "доставка_курʼєром"}),
    ("a German alias on a key", {"id": 48, "name": KEY + "x", "alias": "straße"}),
    ("a Greek alias on a key", {"id": 49, "name": KEY + "x", "alias": "ΣΑΣ_σας"}),
    ("a key whose tail is not ASCII", {"id": 50, "name": KEY + "пакування"}),
]

# Unicode's own traps for a titlecaser: final sigma, digraphs, a ligature, a
# capital with a dot, a number form, a combining accent.
NOT_ASCII = ("доставка курʼєром", "straße", "ΣΑΣ σας", "ǆemal", "ﬁle", "ǅ", "İstanbul",
             "ⅷ", "áb")

WORDS = ("cash on delivery", "o'neil 2nd day", "dELIVERY EXPRESS", "x-y.z", "", " ",
         "a  b", "ALL CAPS", "mIxEd123cAsE", "already Title Case", "tab\tand\nnewline")


def _parsed(payload: Any) -> Dict[str, Any]:
    from core.landing_rows import expense_type_row

    try:
        row = expense_type_row(payload)
    except Exception as exc:  # the type is the contract, the text is not
        return {"error": type(exc).__name__}
    return {"row": {"id": row.id, "name": row.name, "alias": row.alias,
                    "is_active": row.is_active}}


def _upsert_sql() -> str:
    """The statement `upsert_expense_types` sends, captured through a pool
    that records it; the latch is not taken."""
    from core import pg_expense_types_write as chain
    from core.landing_rows import ExpenseTypeRow

    sent: List[str] = []

    class _Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    class _Conn:
        def transaction(self):
            return _Transaction()

        async def executemany(self, sql, args):
            sent.append(sql)

    class _Acquire:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *exc):
            return False

    class _Pool:
        def acquire(self):
            return _Acquire()

    with patch.object(chain, "_pool", AsyncMock(return_value=_Pool())), \
            patch.object(chain, "_latch", return_value="stamp"), \
            patch.object(chain.chain_latch, "claim", AsyncMock()):
        asyncio.run(chain.upsert_expense_types([ExpenseTypeRow(1, "x", None, True)]))
    assert len(sent) == 1, sent
    return sent[0]


def _titles() -> Dict[str, List[List[str]]]:
    ascii_cases = []
    for code in range(128):
        c = chr(code)
        for text in (c, f"{c}ab", f"a{c}b", f"ab{c}", f"AB{c}CD"):
            ascii_cases.append([text, text.title()])
    ascii_cases += [[text, text.title()] for text in WORDS]
    assert not any(text.isascii() for text in NOT_ASCII)
    return {"ascii": ascii_cases,
            "not_ported": [[text, text.title()] for text in NOT_ASCII]}


def render() -> str:
    from core.landing_rows import EXPENSE_TYPE_COLUMNS, _LOCALISATION_PREFIX, expense_type_row
    from core.pg_expense_types_write import CHAIN_TABLES

    cases = [{"label": label, "payload": payload, "python": _parsed(payload)}
             for label, payload in CASES]
    for label, payload in GAPS:
        python = _parsed(payload)
        assert "row" in python, f"{label}: a gap is a payload Python parses"
        cases.append({"label": label, "payload": payload, "python": python,
                      "gap": "title_not_ported"})
    contract = {
        "about": "Generated by tests/rust_k2_contract.py from the Python app; "
                 "regenerate with `python tests/rust_k2_contract.py`. Do not edit.",
        "table": CHAIN_TABLES[0],
        "columns": list(EXPENSE_TYPE_COLUMNS),
        "localisation_prefix": _LOCALISATION_PREFIX,
        "unknown_name": expense_type_row({"id": 1}).name,
        "upsert_sql": _upsert_sql(),
        "parse_cases": cases,
        "titles": _titles(),
    }
    return json.dumps(contract, ensure_ascii=False, indent=1) + "\n"


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    PATH.write_text(render(), encoding="utf-8")
    print(f"wrote {PATH.relative_to(ROOT)}")
