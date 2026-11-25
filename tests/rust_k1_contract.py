"""The contract the Rust `ch_sync` (rust/crates/ks-worker) is held to.

The plan's К1 (revision 4): the ClickHouse copy ported to Rust, run in a
shadow beside Python's before it ever replaces it. Everything the copy
sends ClickHouse or Postgres that is a text — column lists and wire types,
the three DDL templates, the Gold derivation, the watermark statements, the
TabSeparated escapes and rows rendered through them — is exported here from
the Python modules that own it, into `rust/contract/ch_sync.json`, with the
Kyiv date CPython's `zoneinfo` gives instants either side of every clock
change. `rust/crates/ks-worker/tests/contract.rs` holds the Rust side to the
file, and `tests/unit/test_rust_k1_contract.py` holds the file to the app:
a change to `core/ch_*.py` or `core/warehouse_sql.py` fails the Python suite
until the file is regenerated, and then the Rust suite until Rust follows.

Regenerate (in the Python image CI uses):

    python tests/rust_k1_contract.py
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = pathlib.Path(__file__).resolve().parent.parent
PATH = ROOT / "rust" / "contract" / "ch_sync.json"

_UTC = timezone.utc


def _statement(fn, *args) -> str:
    """The SQL `fn` sends, captured through a pool that records it."""
    captured: List[str] = []

    class _Conn:
        async def execute(self, sql, *params):
            captured.append(sql)

    class _Acquire:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *exc):
            return False

    class _Pool:
        def acquire(self):
            return _Acquire()

    async def _get_pool():
        return _Pool()

    with patch("core.pg.get_pool", _get_pool):
        asyncio.run(fn(*args))
    return captured[0]


def _typed(value: Any, kind: str) -> List[str]:
    if value is None:
        return ["null", ""]
    if kind == "bool":
        return ["bool", "true" if value else "false"]
    if kind in ("ts", "ts6"):
        return ["ts", value.isoformat()]
    if kind == "date":
        return ["date", value.isoformat()]
    if kind == "decimal":
        return ["decimal", str(value)]
    if kind == "int":
        return ["int", str(value)]
    return ["str", value]


_NASTY = "tab\tline\nback\\slash\rbell\x08form\x0cend"


def _samples() -> List[Dict[str, Any]]:
    from core import ch_history, ch_silver
    from core.pg_silver import SILVER_COLUMNS

    late = datetime(2026, 3, 28, 21, 59, 59, 999999, tzinfo=_UTC)
    silver = [
        (1, 1, 1, Decimal("1234.50"), late, 17358, None, date(2026, 3, 28), False,
         "retail", True, "Instagram", True, date(2026, 3, 28), _NASTY),
        (2, 5, 19, Decimal("0.00"), None, None, 15, date(2026, 3, 29), True,
         "exhibition", True, "Виставка", False, None, None),
        (3, 7, 12, Decimal("-12.50"), datetime(2025, 10, 25, 21, tzinfo=_UTC), 4001, 4,
         date(2025, 10, 26), False, "offline", True, "Офлайн-магазин", False,
         date(2024, 1, 1), "\\N"),
        (4, 3, 20, Decimal("99999999.99"), datetime(2023, 12, 2, tzinfo=_UTC), 1, 7,
         date(2023, 12, 2), False, "internal", False, "Other", False, None,
         "кириллица 🎁 it's"),
        (5, 4, 1, Decimal("0.01"), datetime(2027, 1, 1, tzinfo=_UTC), 2, None,
         date(2027, 1, 1), False, "retail", True, "Shopify", False, date(2027, 1, 1), ""),
    ]
    history = [
        (1, 1, late, "baseline", 1, 1, None, Decimal("1234.50"), late, 17358, None,
         "utm_source=instagram\nutm_medium=stories\r\n", _NASTY, late),
        (99_999_999_999, 2, datetime(2026, 8, 27, 3, 0, 0, 1, tzinfo=_UTC), "change",
         None, None, None, None, None, None, None, None, None, None),
        (3, 3, datetime(2026, 11, 25, tzinfo=_UTC), "backfill", 7, 12, 4,
         Decimal("-0.50"), datetime(2026, 11, 25, 0, 0, 0, 123456, tzinfo=_UTC), 4, 8,
         "\\N", "", datetime(2026, 11, 25, tzinfo=_UTC)),
    ]

    def sample(table, rows, columns, types, render):
        return {
            "table": table,
            "rows": [[_typed(v, types[c]) for c, v in zip(columns, row)] for row in rows],
            "rendered": render(rows).decode("utf-8"),
        }

    return [
        sample("silver", silver, SILVER_COLUMNS, ch_silver._TYPES, ch_silver.render_tsv),
        sample("history", history, ch_history.COLUMNS, ch_history._TYPES,
               ch_history.render_tsv),
    ]


def _last_sunday(year: int, month: int) -> date:
    return max(date(year, month, d) for d in range(25, 32)
               if date(year, month, d).weekday() == 6)


def _kyiv_dates() -> List[List[str]]:
    """Every half hour, and the second before it, across three days around
    each clock change of 2024–2027, and either side of each year's end."""
    kyiv = ZoneInfo("Europe/Kyiv")
    out: List[List[str]] = []
    for year in (2024, 2025, 2026, 2027):
        for month in (3, 10):
            change = _last_sunday(year, month)
            start = datetime(change.year, change.month, change.day, tzinfo=_UTC) - timedelta(days=1)
            for step in range(3 * 24 * 2):
                at = start + timedelta(minutes=30 * step)
                for instant in (at - timedelta(seconds=1), at):
                    out.append([instant.isoformat(), instant.astimezone(kyiv).date().isoformat()])
        year_end = datetime(year, 12, 31, 21, 59, 59, tzinfo=_UTC)
        for instant in (year_end, year_end + timedelta(seconds=1)):
            out.append([instant.isoformat(), instant.astimezone(kyiv).date().isoformat()])
    return out


def build() -> Dict[str, Any]:
    from core import ch_common, ch_gold, ch_history, ch_silver
    from core.pg_gold import GOLD_COLUMNS
    from core.pg_silver import SILVER_COLUMNS
    from core.warehouse_sql import GOLD_MEASURES

    environment = {k: v for k, v in os.environ.items() if k != ch_history.DB_ENV}
    with patch.dict(os.environ, environment, clear=True):
        history_table = ch_history.table()
    fine, rollup = ch_silver.derive_gold_sql()
    return {
        "silver_columns": [[c, ch_silver._TYPES[c]] for c in SILVER_COLUMNS],
        "history_columns": [[c, ch_history._TYPES[c]] for c in ch_history.COLUMNS],
        "gold_columns": list(GOLD_COLUMNS),
        "gold_measures": [[name, text] for name, text in GOLD_MEASURES.items()],
        "silver_ddl": ch_silver._SILVER_DDL,
        "gold_ddl": ch_gold._TABLE_DDL,
        "history_ddl": ch_history._DDL,
        "derive_gold_fine": fine,
        "derive_gold_rollup": rollup,
        "tables": {
            "silver": ch_silver.SILVER_TABLE,
            "silver_staging": ch_silver.SILVER_STAGING,
            "gold": ch_silver.GOLD_TABLE,
            "gold_staging": ch_silver.GOLD_STAGING,
            "history": history_table,
        },
        "state_rows": {
            "silver": ch_silver.SILVER_STATE,
            "gold": ch_silver.GOLD_STATE,
            "history": ch_history.STATE_ROW,
        },
        "mark_ok_sql": _statement(ch_common.mark_ok, "x", 1),
        "mark_failed_sql": _statement(ch_common.mark_failed, "x", "e"),
        "escapes": [list(pair) for pair in ch_common._ESCAPES],
        "tsv_samples": _samples(),
        "kyiv_dates": _kyiv_dates(),
    }


def render() -> str:
    return json.dumps(build(), ensure_ascii=False, indent=1) + "\n"


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    PATH.write_text(render(), encoding="utf-8")
    print(f"wrote {PATH.relative_to(ROOT)}")
