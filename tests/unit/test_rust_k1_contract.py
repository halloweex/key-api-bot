"""The Rust `ch_sync`'s contract is what the Python app renders today.

`rust/contract/ch_sync.json` is what `rust/crates/ks-worker/tests/contract.rs`
holds the Rust port to (plan, revision 4, К1). This holds the file to the
app: a change to a ClickHouse DDL, a column, the Gold measures, the
watermark statements or the TabSeparated codec fails here first, with the
command that regenerates the file — and the Rust suite then fails until the
port follows. The drift sensor of the plan's step 2, for the copy.
"""
from __future__ import annotations

import json

from tests.rust_k1_contract import PATH, render


def test_the_contract_is_what_python_renders_today():
    assert json.loads(PATH.read_text(encoding="utf-8")) == json.loads(render()), (
        "core/ch_*.py or core/warehouse_sql.py moved what the ClickHouse copy "
        "sends. Regenerate rust/contract/ch_sync.json with "
        "`python tests/rust_k1_contract.py`, then make rust/crates/ks-worker "
        "follow until `cargo test -p ks-worker` passes."
    )
