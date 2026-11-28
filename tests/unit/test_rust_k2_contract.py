"""The Rust expense-type load's contract is what the Python app does today.

`rust/contract/expense_types.json` is what
`rust/crates/ks-worker/tests/k2_contract.rs` holds the Rust port to (plan,
revision 4, К2, step 1). This holds the file to the app: a change to
`core.landing_rows.expense_type_row`, its columns, its localisation prefix or
the statement `core.pg_expense_types_write` sends fails here first, with the
command that regenerates the file — and the Rust suite then fails until the
port follows. The drift sensor of the plan's step 2, for the dictionary.
"""
from __future__ import annotations

import json

from tests.rust_k2_contract import PATH, render


def test_the_contract_is_what_python_does_today():
    assert json.loads(PATH.read_text(encoding="utf-8")) == json.loads(render()), (
        "core/landing_rows.py or core/pg_expense_types_write.py moved what the "
        "expense-type load does. Regenerate rust/contract/expense_types.json with "
        "`python tests/rust_k2_contract.py`, then make rust/crates/ks-worker "
        "follow until `cargo test -p ks-worker` passes."
    )
