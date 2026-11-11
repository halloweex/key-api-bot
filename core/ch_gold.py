"""gold.daily_revenue in ClickHouse: the table's shape and its wire format.

What is left of step 4 of «Одна бронза». Step 4 copied Postgres
`gold.daily_revenue` into ClickHouse verbatim, hourly, and compared the copy
daily. Step 5 superseded it: `core/ch_silver.py` ships Silver and derives Gold
*inside* ClickHouse, the hourly `ch_sync` has called that and nothing here
since, and the daily verdict compares the two engines' Gold against each
other (`ch_engines_gold_*`). The copy mode — its shipper, its read-back
comparison and the five `ch_gold_*` conditions they filed — was kept as a
fallback nobody ran, and was deleted by the owner's decision O11 (a)
(2026-11-11). Those condition names are not reused.

What stays is what the derivation and its comparison import rather than
restate: the table's DDL (`_TABLE_DDL`, created by `ch_silver._derive_gold`
for both the live table and its staging twin), its column list (`COLUMNS`,
the same object as `core.pg_gold.GOLD_COLUMNS`) and the TSV codec typed to
it (`parse_tsv`, which `ch_silver` reads ClickHouse's Gold back with).
`TABLE` names the ClickHouse successor the DuckDB table fates point to, and
`STATE_ROW` the `meta.mirror_state` row the derivation writes
(`ch_silver.GOLD_STATE`).
"""
from __future__ import annotations

from core.ch_common import (
    parse_tsv as _parse_generic,
    render_tsv as _render_generic,
)
from core.pg_gold import GOLD_COLUMNS as COLUMNS

# The row in meta.mirror_state. Prefixed so the store is unmistakable in a
# listing where every other row is a Postgres table.
STATE_ROW = "clickhouse.gold_daily_revenue"

TABLE = "gold.daily_revenue"

# COLUMNS is imported from core.pg_gold.GOLD_COLUMNS — the one home for the
# cell shape; a test pins the identity so a 0012 cannot drift this copy.

_DECIMAL_COLUMNS = {"revenue", "returns_revenue", "avg_order_value"}
_INT_COLUMNS = {
    "orders_count", "unique_customers", "new_customers",
    "returning_customers", "returns_count",
}

# MergeTree, not Replacing: the table is replaced whole via EXCHANGE, so there
# are never two versions of a row for a Replacing engine to collapse. ORDER BY
# stays non-nullable on purpose — source_id (NULL = the roll-up) rides as a
# data column, because MergeTree enforces no uniqueness either way and the
# comparison keys on all three columns itself.
_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {name} (
    date                Date,
    sales_type          LowCardinality(String),
    source_id           Nullable(Int32),
    revenue             Decimal(14, 2),
    orders_count        Int32,
    unique_customers    Int32,
    new_customers       Int32,
    returning_customers Int32,
    returns_count       Int32,
    returns_revenue     Decimal(14, 2),
    avg_order_value     Decimal(12, 2)
)
ENGINE = MergeTree
ORDER BY (date, sales_type)
"""


# ─── TSV: the wire format both directions ────────────────────────────────────
#
# TabSeparated with ClickHouse's own escaping. sales_type is the only string
# column and its values are a known closed set, but the escaping is written
# anyway: a format that is correct only for today's data is a format that
# breaks silently.

_GOLD_TYPES = {
    "date": "date", "sales_type": "str",
    **{c: "int" for c in _INT_COLUMNS}, "source_id": "int",
    **{c: "decimal" for c in _DECIMAL_COLUMNS},
}


def render_tsv(rows):
    return _render_generic(rows, COLUMNS, _GOLD_TYPES)


def parse_tsv(text):
    return _parse_generic(text, COLUMNS, _GOLD_TYPES)
