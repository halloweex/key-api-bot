"""Whether a number fits a Postgres `NUMERIC(precision, scale)` column.

Asked by the write chains BEFORE they latch, because the latch is permanent: a
value Postgres rejects raises inside the write, after the marker is on disk, and
the chain is then latched for good with no owner row behind it — a CRITICAL
`chain_latch_disagrees` the next morning and a copy-back that refuses — for an
input DuckDB would have refused without a trace. Chain 7a found that first
(`core/pg_goals_write._fits`); chain 4 asks the same question of a buyer's
loyalty figures. One home for Postgres' rule, so the two cannot come to
disagree about where a boundary lies.
"""
from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Optional


def refusal(column: str, value, precision: int, scale: int, *,
            nullable: bool) -> Optional[str]:
    """Why Postgres would refuse `value` in `column`, or None when it fits.

    Postgres' own rule, written out: the value rounded half away from zero to
    `scale` places must be under `10^(precision - scale)`, and an infinity
    never fits. NaN does fit a `NUMERIC` and is refused anyway: DuckDB's
    `DECIMAL` refuses it, and it is not a number anybody typed.
    `Decimal(str(value))` is what asyncpg sends for a float, so the boundary
    lands where the server's does — `tests/integration/test_goals_writer.py`
    writes both sides of it.
    """
    if value is None:
        return None if nullable else f"{column} must not be NULL"
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return f"{column}={value!r} is not a number"
    exact = Decimal(str(value))
    if not exact.is_finite():                  # inf and NaN, float or Decimal
        return f"{column}={value!r} is not a finite number"
    digits = precision - scale
    # The magnitude first: `quantize` itself raises `InvalidOperation`, not
    # `ValueError`, on a number too long for the decimal context (1e30).
    if exact.adjusted() >= digits or abs(exact.quantize(
            Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)) >= 10 ** digits:
        return (f"{column}={value!r} does not fit NUMERIC({precision},{scale}): "
                f"it must round to under 10^{digits}")
    return None
