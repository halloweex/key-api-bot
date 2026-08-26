"""One Silver projection, two engines.

Step 05 ends with Silver computed inside Postgres. The naive way to get there
is to write the projection a second time in Postgres dialect, and that is
exactly the mistake charter rule 1 exists to prevent: a rule with two homes is
a rule that will differ. The shortest recorded time it took to differ in this
codebase is **under a day** — #101 updated `sales_type` in one copy of the
Silver projection and not the other.

The good news, found by reading rather than assumed: the projection is almost
entirely ordinary SQL. Between DuckDB and PostgreSQL it differs in exactly
**two** places.

    the Kyiv date   DATE(timezone('Europe/Kyiv', x))     DuckDB
                    (timezone('Europe/Kyiv', x))::date   PostgreSQL

    table names     orders                    bronze.orders
                    managers                  bronze.managers
                    manager_classifications   app.manager_classifications

Everything else — the CASE, the EXISTS subqueries, the status tuple, the
boolean expressions — is identical text. So the projection stays in one place
and takes a dialect.

`timezone(zone, timestamptz)` means the same thing in both engines, which is
the one thing here worth doubting and was checked against production: 1,007
orders have a Kyiv date different from their UTC one, and the two stores agree
on every one of them.

WHY A DIALECT OBJECT AND NOT AN `if postgres:`

Because the difference is data, and writing it as data makes it testable. A
test renders both and asserts they are the same string once the two known
substitutions are undone — which proves there is no *third* divergence, the
thing a branchy implementation could hide indefinitely.
"""
from __future__ import annotations

from dataclasses import dataclass

from core.duckdb_constants import DISPLAY_TIMEZONE


@dataclass(frozen=True)
class Dialect:
    """Where the Silver projection's tables live, and how it reads a date."""

    name: str
    orders: str
    managers: str
    classifications: str
    silver_orders: str
    # A format template with one `{column}` hole. Not a function, so the whole
    # dialect stays comparable, printable and trivially frozen.
    date_template: str

    def kyiv_date(self, column: str) -> str:
        """The order's calendar date in the timezone the business reads."""
        return self.date_template.format(column=column, zone=DISPLAY_TIMEZONE)


DUCKDB = Dialect(
    name="duckdb",
    orders="orders",
    managers="managers",
    classifications="manager_classifications",
    silver_orders="silver_orders",
    # Byte-for-byte what `core.duckdb_constants._date_in_kyiv` has always
    # emitted. Changing it here changes stored Silver on the next rebuild.
    date_template="DATE(timezone('{zone}', {column}))",
)

POSTGRES = Dialect(
    name="postgres",
    # `bronze` is landing from KeyCRM; `app` is what nothing can decide again.
    # `postgres/initdb/30-app.sql` draws that line — see revision 0005 for why
    # the classification sits on the other side of it from the managers.
    orders="bronze.orders",
    managers="bronze.managers",
    classifications="app.manager_classifications",
    silver_orders="silver.orders",
    # `DATE(x)` also exists in PostgreSQL, but the cast is what the rest of
    # this repository's Postgres SQL uses, so it reads the same as its
    # neighbours in `core/reconciliation_io.py`.
    date_template="(timezone('{zone}', {column}))::date",
)
