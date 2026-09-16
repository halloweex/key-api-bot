"""The forecast model's training frame, read from Postgres.

`PredictionService._query_daily_revenue` is the input to everything the model
does: training twice a week, `evaluate`, `tune`, and — on every request to
`/api/revenue/trend?include_forecast=true&period=week` — `predict_range`, which
reads 780 days of it live. Stage 1 never counted it, because it is not a
dashboard repository method.

WHY IT IS NOT ONE BODY FOR TWO ENGINES

Every other read in this migration shares its SQL. This one deliberately does
not, and the reason is measured rather than preferred. A shared body would need
`GROUP BY date, sales_type` with `SUM(...)` on the DuckDB side too — DuckDB's
Gold is already one row per `(date, sales_type)`, so each `SUM` covers one row
and the numbers would not move. The *types* would: DuckDB widens `SUM` over an
INTEGER to HUGEINT, and the frame the model trains on today is `int32` for its
counts. Changing the working path's dtypes to share a text is a regression
bought to satisfy a rule.

What does not change between the two is the measures — they are stored columns
of Gold, defined once in `GOLD_MEASURES`. What changes is only the shape: DuckDB
holds a column per channel, Postgres holds `source_id`. That is the same split
`gold.daily_revenue` itself was built with, and the channel spelling still comes
from its one home, `core.sql_dialect._channel_items`.

THE FRAME MUST COME BACK IDENTICAL, DOWN TO THE DTYPE

The model does not read numbers, it reads a DataFrame. `Decimal` where it had
`float64`, `date` where it had `datetime64[us]`, `int64` where it had `int32` —
none of those raises; each quietly changes what feature engineering computes.
So this returns the frame DuckDB returns, and `normalise_frame` is where that
promise is kept.

`sales_type="all"` returns one row **per (date, sales_type)**, not per date —
about 2.2 rows a day on the production catalogue (1,729 rows over 780 days).
That is how DuckDB answers and it is reproduced, not corrected: it feeds a
time-series model duplicate dates, which is a question about the model, not
about this port.

THE FLAG SHIPS OFF

Moving this changes the trained model, which retrains on its own schedule —
so its effect lands days after the switch and is hard to attribute. It gets a
switch of its own for that reason, and is compared at the level of the model's
predictions before anybody sets it.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)

ENV = "KS_READ_FORECAST_INPUT"
_VALID = ("duckdb", "postgres")

# The frame's columns, in the order `_query_daily_revenue` has always returned
# them. The model's feature code indexes by name, but the order is part of what
# "identical" means here, so it is spelled once and checked.
COLUMNS = (
    "date", "revenue", "instagram_revenue", "telegram_revenue",
    "shopify_revenue", "orders_count", "unique_customers", "new_customers",
    "returning_customers", "returns_count", "returns_revenue",
    "instagram_orders", "telegram_orders", "shopify_orders", "avg_order_value",
)
FLOAT_COLUMNS = (
    "revenue", "instagram_revenue", "telegram_revenue", "shopify_revenue",
    "returns_revenue", "avg_order_value",
)
INT_COLUMNS = (
    "orders_count", "unique_customers", "new_customers", "returning_customers",
    "returns_count", "instagram_orders", "telegram_orders", "shopify_orders",
)


def enabled() -> bool:
    """Whether the training frame should come from Postgres.

    An unknown value raises — `KS_BOT_STORE`'s rule. This decides what the
    model learns from; serving the other store's frame silently is worse than
    stopping.
    """
    value = os.getenv(ENV, "duckdb").strip().lower() or "duckdb"
    if value not in _VALID:
        raise ValueError(
            f"{ENV}={value!r} — unknown engine. Expected one of {_VALID}; "
            f"a typo here must stop the read, not point it at the other store."
        )
    return value == "postgres"


def available() -> bool:
    return bool(os.getenv("KS_PG_DSN", "").strip())


def _select_sql(exclude_today: bool, filter_sales_type: bool) -> str:
    """The Postgres SELECT, with the roll-up measures and the channels.

    Every roll-up measure reads `source_id IS NULL` only. Three of them are
    `COUNT(DISTINCT buyer_id)` and distinct counts do not add up across
    sources — folding the fine rows overstates `unique_customers` in 29 of
    2,107 production cells — so summing the fine rows is not available.
    Channels read the fine rows through `_channel_items`, the one place a
    channel is spelled for either Gold.
    """
    from core.sql_dialect import POSTGRES, channel_totals_items

    rollup = "source_id IS NULL"
    channels = {name.lower(): (rev, orders)
                for name, rev, orders in channel_totals_items(POSTGRES)}

    def r(col):
        return f"COALESCE(SUM({col}) FILTER (WHERE {rollup}), 0) AS {col}"

    select = ",\n            ".join([
        "date",
        r("revenue"),
        f"{channels['instagram'][0]} AS instagram_revenue",
        f"{channels['telegram'][0]} AS telegram_revenue",
        f"{channels['shopify'][0]} AS shopify_revenue",
        r("orders_count"),
        r("unique_customers"),
        r("new_customers"),
        r("returning_customers"),
        r("returns_count"),
        r("returns_revenue"),
        f"{channels['instagram'][1]} AS instagram_orders",
        f"{channels['telegram'][1]} AS telegram_orders",
        f"{channels['shopify'][1]} AS shopify_orders",
        r("avg_order_value"),
    ])
    where = ["date >= ?"]
    if exclude_today:
        where.append("date < ?")
    if filter_sales_type:
        where.append("sales_type = ?")
    # `sales_type` in the ORDER BY makes the order within a date defined. It is
    # undefined in DuckDB's `ORDER BY date`, which only matters for "all".
    return f"""
        SELECT {select}
        FROM gold.daily_revenue
        WHERE {" AND ".join(where)}
        GROUP BY date, sales_type
        ORDER BY date, sales_type
    """


def normalise_frame(rows):
    """Plain tuples in, the exact DataFrame DuckDB's `fetchdf()` produces out."""
    import pandas as pd

    df = pd.DataFrame([tuple(r) for r in rows], columns=list(COLUMNS))
    df["date"] = pd.to_datetime(df["date"]).astype("datetime64[us]")
    for col in FLOAT_COLUMNS:
        df[col] = df[col].astype("float64")
    for col in INT_COLUMNS:
        df[col] = df[col].astype("int32")
    return df


async def fetch_frame(
    start_date: date, today: date, sales_type: str, exclude_today: bool,
):
    """The training frame from Postgres, shaped and typed as DuckDB's."""
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    params: list = [start_date]
    if exclude_today:
        params.append(today)
    if sales_type != "all":
        params.append(sales_type)

    sql = _select_sql(exclude_today, sales_type != "all")
    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(numbered(sql), *params)
    return normalise_frame(rows)
