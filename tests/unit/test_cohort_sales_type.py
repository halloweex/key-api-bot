"""The cohort tab now means the same thing by "retail" as every other tab.

Five customer-analytics methods used to re-derive `sales_type` from
`manager_id` against the RETAIL_MANAGER_IDS constant, instead of reading the
column Silver materialises. That is charter rule 1's prohibition — a rule with
two homes — and this copy had gone stale in two separate ways:

* it required `source_id = 4` for a manager-less order, where Silver requires
  nothing at all;
* it read the *constant* list rather than `managers.is_retail`, which is the
  column a human edits when they classify a manager — so a classification made
  through `POST /api/managers/{id}/retail-status` never reached these five.

Measured on production Silver, non-return orders: **1,781 were retail to every
other tab and invisible here, and 175 were the other way round**, out of
45,709. The cohort tab was describing a different population than the revenue
tab beside it.

The column is also the only spelling that survives a third engine: a manager
list rendered into SQL has to be rendered three ways, a column does not.
"""
from __future__ import annotations

import ast
import inspect

import pytest

from core.repositories import customers as module

BODIES = (
    "cohort_retention_select",
    "enhanced_cohort_retention_select",
    "days_to_second_purchase_select",
    "cohort_ltv_select",
    "at_risk_customers_select",
)

FIVE = (
    "get_cohort_retention",
    "get_enhanced_cohort_retention",
    "get_days_to_second_purchase",
    "get_cohort_ltv",
    "get_at_risk_customers",
)


def _statements(name: str) -> str:
    """One method's source with its `--` and `#` commentary removed.

    Parsed and stripped rather than grepped: every one of these methods now
    *explains* the old rule in a comment, and a grep for the constant would be
    satisfied by the explanation — [[feedback_assert_on_structure_not_prose]],
    which this repository has now paid for three times in one day.
    """
    src = inspect.getsource(module)
    tree = ast.parse(src)
    lines = src.splitlines()
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            body = lines[node.lineno - 1:node.end_lineno]
            return "\n".join(
                line.split("--")[0].split("#")[0] for line in body
            )
    raise AssertionError(f"{name} not found")


@pytest.mark.parametrize("name", FIVE)
def test_it_reads_the_column_silver_materialised(name):
    code = _statements(name)
    assert "o.sales_type" in code, f"{name} does not filter on the Silver column"


@pytest.mark.parametrize("name", FIVE)
def test_it_no_longer_re_derives_the_rule(name):
    code = _statements(name)
    for stale in ("RETAIL_MANAGER_IDS", "B2B_MANAGER_ID", "manager_id IN"):
        assert stale not in code, (
            f"{name} still carries a second home for the sales_type rule: {stale}"
        )


def test_the_constants_are_no_longer_imported_here():
    """They belong to the warehouse projection and to manager seeding, not to
    a read path. Leaving the import invites the next copy."""
    src = inspect.getsource(module)
    assert "RETAIL_MANAGER_IDS" not in src.split("\n\n")[0]
    tree = ast.parse(src)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert "RETAIL_MANAGER_IDS" not in imported
    assert "B2B_MANAGER_ID" not in imported


@pytest.mark.parametrize("name", FIVE)
def test_all_still_means_no_filter(name):
    """`sales_type=all` spans every category, including `internal`, and must
    not quietly become a predicate."""
    code = _statements(name)
    assert 'if sales_type == "all"' in code or '"" if sales_type == "all"' in code


class TestTheOneSpellingThatCannotBeShared:
    """`today` is a hole for two reasons, and the second is the one that bit.

    **Spelling.** No form of `CURRENT_DATE` is accepted by all three engines —
    measured, not assumed:

        bare `CURRENT_DATE`      DuckDB OK   PostgreSQL OK   ClickHouse no
        `CURRENT_DATE()`         DuckDB OK   PostgreSQL no   ClickHouse OK

    PostgreSQL treats it as a reserved keyword and rejects the parentheses;
    ClickHouse has no bare keyword and rejects their absence.

    **Meaning.** Neither keyword names the same *day* on both engines. The
    `keycrm-web` container runs `TZ=Europe/Kyiv`, so DuckDB's day is Kyiv's;
    `ks-clickhouse` answers in UTC. Between 21:00 and midnight UTC the two are
    on different dates — moving `days_since_last` in the at-risk query by a day
    for three hours every night, and moving four month-truncated cohort windows
    by a whole month in the last three hours of a month. That was live for a
    day after `KS_READ_COHORTS=clickhouse` went on.

    A differential test cannot find it: under the gate both engines sit in one
    timezone and are wrong together. So the check is not "do they agree" but
    "does each rendering name the timezone rather than inherit one".

    Everything else in the cohort queries *is* shared — `DATE_TRUNC`,
    `DATEDIFF`, `median`, `FILTER (WHERE …)`, CTEs, `COUNT(DISTINCT …)` and
    `ROUND` were each run on both engines and agree.
    """

    @pytest.mark.parametrize("fn_name", BODIES)
    def test_the_extracted_body_renders_each_engine_its_own_today(self, fn_name):
        """All five queries left their methods, so the check follows them: the
        hole exists precisely because no literal serves both engines, and the
        table name must be the only *other* difference — that is what would
        catch a sixth divergence sneaking in."""
        import core.sql_dialect as dialects

        render = getattr(dialects, fn_name)
        kw = dict(sales_type_filter="AND o.sales_type = 'retail'", months_back=12)
        duck = render(dialects.DUCKDB_ANALYTICS, **kw)
        clickhouse = render(dialects.CLICKHOUSE_ANALYTICS, **kw)

        assert duck != clickhouse, f"{fn_name}: the dialect does nothing"
        undone = clickhouse.replace("silver.orders", "silver_orders").replace(
            dialects.CLICKHOUSE_ANALYTICS.today, dialects.DUCKDB_ANALYTICS.today)
        assert undone == duck, (
            f"{fn_name} differs between engines somewhere other than the table "
            f"name and today's date"
        )

    @pytest.mark.parametrize("fn_name", BODIES)
    def test_no_body_carries_a_duckdb_only_spelling(self, fn_name):
        import core.sql_dialect as dialects

        sql = getattr(dialects, fn_name)(
            dialects.CLICKHOUSE_ANALYTICS,
            sales_type_filter="", months_back=12,
        )
        code = "\n".join(line.split("--")[0] for line in sql.splitlines())
        for banned in ("strftime", "QUALIFY", "list_slice", "GROUP BY ALL"):
            assert banned not in code, f"{fn_name} still speaks DuckDB only"

    @pytest.mark.parametrize("fn_name", BODIES)
    def test_neither_rendering_inherits_a_timezone(self, fn_name):
        """The bug this replaced a spelling check with. A keyword that takes
        its day from whatever timezone the engine happens to be in is the
        defect; naming the zone is the fix."""
        import core.sql_dialect as dialects
        from core.duckdb_constants import DISPLAY_TIMEZONE

        kw = dict(sales_type_filter="", months_back=12)
        for dialect in (dialects.DUCKDB_ANALYTICS, dialects.CLICKHOUSE_ANALYTICS):
            sql = getattr(dialects, fn_name)(dialect, **kw)
            code = "\n".join(line.split("--")[0] for line in sql.splitlines())
            assert "CURRENT_DATE" not in code, (
                f"{fn_name} on {dialect.name} still takes its day from the "
                f"engine's timezone"
            )
            assert DISPLAY_TIMEZONE in code, (
                f"{fn_name} on {dialect.name} does not name the timezone"
            )

    def test_each_engine_keeps_the_spelling_it_can_parse(self):
        """PostgreSQL rejects `AT TIME ZONE`'s absence of nothing, ClickHouse
        rejects `AT TIME ZONE` outright — measured on 24.8.14.39, production's
        version. So the hole survives the timezone fix; only its reason grew."""
        import core.sql_dialect as dialects

        assert "AT TIME ZONE" in dialects.DUCKDB_ANALYTICS.today
        assert "toTimeZone" in dialects.CLICKHOUSE_ANALYTICS.today
        assert "AT TIME ZONE" not in dialects.CLICKHOUSE_ANALYTICS.today

    def test_the_sms_predicate_keeps_the_form_postgres_accepts(self):
        """The audience filter runs on DuckDB and PostgreSQL, and both accept
        the `AT TIME ZONE` form — which they must, because the bare keyword
        would slide the window by a day for three hours a night once
        `KS_SMS_STORE=postgres`."""
        from core.repositories.customers import SmsAudienceFilters

        sql, _params = SmsAudienceFilters(
            brands=("Cosrx",), bought_within_days=30,
        ).predicate("revenue_ltv", "retail")
        from core.sql_dialect import TODAY_IN_KYIV

        assert TODAY_IN_KYIV in sql
        assert "CURRENT_DATE" not in sql, (
            "the audience window takes its day from the engine's timezone, "
            "which differs between the two once KS_SMS_STORE=postgres"
        )
        assert "CURRENT_DATE()" not in sql


class TestTheColumnTypesDescribeTheProjection:
    """A type list one entry short silently drops a column.

    `_typed` zips the row against the list, so a list that is too short throws
    the tail away and one that is mistyped raises deep inside the reader. Both
    happened while the lists lived at the call site: the repository claimed
    seven columns where the enhanced matrix has eight, and six where the
    at-risk projection has seven — and every test stayed green, because the
    differential test carried a third copy of its own.

    They live beside the body now, and this counts them against the projection
    itself rather than against another list written by the same hand.
    """

    PAIRS = (
        ("cohort_retention_select", "COHORT_RETENTION_TYPES"),
        ("enhanced_cohort_retention_select", "ENHANCED_RETENTION_TYPES"),
        ("days_to_second_purchase_select", "DAYS_TO_SECOND_TYPES"),
        ("cohort_ltv_select", "COHORT_LTV_TYPES"),
        ("at_risk_customers_select", "AT_RISK_TYPES"),
    )

    @staticmethod
    def _projection_width(sql: str) -> int:
        """Columns in the outermost SELECT, counted by splitting on the commas
        that separate them at depth zero — a comment or a nested `ROUND(a, 1)`
        must not be mistaken for a column boundary."""
        import re

        tail = sql[sql.rindex("\n            SELECT"):]
        head = tail[:tail.index("\n            FROM")]
        head = "\n".join(l.split("--")[0] for l in head.splitlines())
        head = head.replace("SELECT", "", 1)

        depth, count = 0, 1
        for ch in head:
            if ch in "([":
                depth += 1
            elif ch in ")]":
                depth -= 1
            elif ch == "," and depth == 0:
                count += 1
        return count

    @pytest.mark.parametrize("fn_name,types_name", PAIRS)
    def test_the_list_is_exactly_as_wide_as_the_projection(self, fn_name, types_name):
        import core.sql_dialect as dialects

        sql = getattr(dialects, fn_name)(
            dialects.DUCKDB_ANALYTICS, sales_type_filter="", months_back=12,
        )
        assert len(getattr(dialects, types_name)) == self._projection_width(sql), (
            f"{types_name} does not describe {fn_name}'s projection"
        )

    @pytest.mark.parametrize("fn_name,types_name", PAIRS)
    def test_every_entry_is_a_kind_the_reader_knows(self, fn_name, types_name):
        import core.sql_dialect as dialects
        from core.ch_cohorts import FLOAT, INT, TEXT

        assert set(getattr(dialects, types_name)) <= {TEXT, INT, FLOAT}
