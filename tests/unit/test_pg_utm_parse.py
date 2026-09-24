"""The pure half of the Postgres UTM parse (DN-18), without a database.

Everything that needs a server — the predicate, the transactions, the locks,
the two engines agreeing — is proved in `tests/integration/test_pg_utm_parse.py`.
What is here is arithmetic and shape: where the shrink guard draws its line,
what a refusal says, and that the rows the parse writes are the rows the
columns name.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from core import pg_utm_parse as parse
from core.pg_order_utm import UTM_COLUMNS
from core.utm_classify import utm_columns


class TestTheShrinkGuard:
    @pytest.mark.parametrize("parsed, current", [
        (900, 1000), (1000, 1000), (1200, 1000), (9, 10), (0, 0), (5, 0)])
    def test_at_or_above_the_floor_it_may_replace(self, parsed, current):
        """900 of 1,000 is exactly the floor, and the floor admits it —
        integer arithmetic, so no float lands either side of the line. An
        empty table has nothing to protect."""
        assert parse.refusal(parsed, current) is None

    @pytest.mark.parametrize("parsed, current", [(899, 1000), (8, 10), (0, 1)])
    def test_under_the_floor_it_refuses(self, parsed, current):
        assert parse.refusal(parsed, current) is not None

    def test_the_refusal_names_both_counts_and_the_lever_early(self):
        """`mirror_failing` quotes the first 300 characters of `last_error`;
        a lever past them is a lever nobody reads."""
        why = parse.refusal(123, 45678)
        assert why.startswith("refused:")
        head = why[:300]
        assert "123" in head and "45678" in head and "→" in head
        assert "force=True" in head


class TestTheRowsItWrites:
    def test_a_row_is_the_order_the_verdict_and_updated_at(self):
        stamp = datetime(2026, 9, 7, 9, 0, tzinfo=timezone.utc)
        comment = "UTM: utm_source: fbads; utm_medium: cpc; utm_campaign: spring"
        rows = parse.parse_rows([(7, comment, stamp), (8, "дзвінок", None)])

        assert rows == [(7, *utm_columns(comment), stamp),
                        (8, *utm_columns("дзвінок"), None)]
        assert all(len(r) == len(UTM_COLUMNS) for r in rows)

    def test_both_statements_name_every_column_in_order(self):
        """The rows are positional, so the statements must list the columns
        in `UTM_COLUMNS` order, one parameter each, `parsed_at` last."""
        columns = "(" + ", ".join(UTM_COLUMNS) + ")"
        for sql in (parse.insert_sql(), parse.upsert_sql()):
            assert columns in sql
            assert f"${len(UTM_COLUMNS)}::timestamptz" in sql
            assert f"${len(UTM_COLUMNS) + 1}" not in sql

    def test_the_upsert_replaces_every_verdict_column(self):
        """DuckDB's INSERT OR REPLACE replaces the whole row. An upsert that
        forgot a column would keep a value the comment no longer carries."""
        sets = parse.upsert_sql().split("DO UPDATE SET", 1)[1]
        for column in UTM_COLUMNS[1:]:
            assert f"{column} = EXCLUDED.{column}" in sets


class TestTheLockBound:
    @pytest.mark.parametrize("seconds, literal", [
        (120, "120000ms"), (1, "1000ms"), (0.05, "50ms"), (0.0001, "1ms"), (0, "1ms")])
    def test_it_is_milliseconds_and_never_zero(self, seconds, literal):
        """Postgres reads a `lock_timeout` of 0 as no timeout at all. Whole
        seconds would render any bound under one second as exactly that — an
        unbounded wait wearing a bound's name."""
        assert parse.lock_timeout_setting(seconds) == literal


def test_the_postgres_bound_sits_below_the_production_statement_timeout():
    """Above `KS_PG_TIMEOUT`'s default, asyncpg cancels the advisory wait first
    and the watermark records a bare `TimeoutError` that names no lock. The
    default is read from core/pg.py itself, so the two cannot drift apart."""
    import re
    from pathlib import Path

    from core import pg_utm_parse

    src = (Path(__file__).resolve().parents[2] / "core" / "pg.py").read_text()
    (default,) = re.findall(r'os\.getenv\("KS_PG_TIMEOUT",\s*"([0-9.]+)"\)', src)
    assert pg_utm_parse.PG_LOCK_WAIT_S < float(default)
