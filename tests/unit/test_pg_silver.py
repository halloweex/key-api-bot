"""Silver computed inside Postgres — the first derived table, not a copy.

Verified end to end before this file was written: throwaway
`postgres:17.2-alpine` at revision 0006, the production catalogue of orders
backfilled into `bronze.orders`, the classification replicated, then
`rebuild_silver()`.

    rebuild        46,446 rows in 2.19 s
    against DuckDB 0 only on either side, **0 differing rows** across all
                   fifteen columns
    sales_type     b2b 1189 · exhibition 178 · internal 2288 · retail 42791
                   — identical on both sides

What is pinned here is the wiring that result depended on, because every part
of it is silent when wrong: a column order that shifts values one place left,
a statement rendered for the wrong engine, a rebuild split across
transactions. And since the review's A23, the write shape: one `DELETE` and
one `INSERT`, never an `UPDATE` (the old pass 2 rewrote every row a second
time) and never a `TRUNCATE` (which made every read of Silver wait). The
same shape against a real Postgres is `tests/integration/test_pg_rebuild_shape.py`.
"""
from __future__ import annotations

import asyncio
import inspect
import re
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from core.pg_silver import (CLEAR_SQL, REBUILD_LOCK_TIMEOUT, SILVER_COLUMNS, SILVER_TABLE,
                            insert_sql, rebuild_silver)
from core.sql_dialect import POSTGRES
from core.warehouse_sql import silver_recompute_ctes, silver_select_sql

REPO = Path(__file__).resolve().parents[2]


class TestTheColumnOrderIsTheProjectionsOrder:
    def test_it_matches_the_migration(self):
        """A shift here writes `status_id` into `source_id` and raises nothing:
        both are integers."""
        ddl = (REPO / "migrations" / "versions" / "0006_silver_orders.py").read_text()
        body = ddl[ddl.index("CREATE TABLE silver.orders"):]
        declared = [
            m.group(1) for m in re.finditer(r"^\s{12}([a-z_]+)\s+[A-Z]", body, re.M)
        ]
        assert declared == list(SILVER_COLUMNS)

    def test_the_insert_names_every_column_explicitly(self):
        """Positional INSERT into a table whose column order changed later is
        the same failure with no test to catch it — and the SELECT names the
        same columns in the same order."""
        lines = insert_sql().splitlines()
        (head,) = [line for line in lines if line.startswith("INSERT INTO")]
        assert head == f"INSERT INTO {SILVER_TABLE} ({', '.join(SILVER_COLUMNS)})"
        assert lines[-1] == f"SELECT {', '.join(SILVER_COLUMNS)} FROM recomputed"


class TestTheRebuildIsRenderedForPostgres:
    def test_it_reads_bronze_and_casts_the_date(self):
        sql = insert_sql()
        assert f"FROM {POSTGRES.orders} o" in sql
        assert "::date" in sql
        assert "DATE(timezone(" not in sql

    def test_the_rule_is_the_shared_text(self):
        """Not a second Postgres-flavoured copy — rule 1, and #101 is the
        proof it takes under a day to bite. The rebuild inserts the very text
        the row-values twin recomputes with."""
        assert silver_select_sql(POSTGRES) in insert_sql()
        assert insert_sql().startswith(f"WITH {silver_recompute_ctes(POSTGRES)}\n")

    def test_no_statement_rewrites_rows(self):
        """A23: pass 2 was a full-table UPDATE, a second version of nearly
        every row. Mutation: append the UPDATE back to the rebuild."""
        assert not re.search(r"\bUPDATE\b", insert_sql())
        assert not re.search(r"\bTRUNCATE\b", insert_sql())


class _Conn:
    """Records every statement the rebuild runs, in order."""

    def __init__(self):
        self.statements, self.in_transaction, self.transactions = [], False, 0

    def transaction(self):
        conn = self

        class _Tx:
            async def __aenter__(self):
                conn.in_transaction = True
                conn.transactions += 1

            async def __aexit__(self, *exc):
                conn.in_transaction = False
                return False
        return _Tx()

    async def execute(self, sql, *args):
        assert self.in_transaction, f"outside the transaction: {sql[:60]}"
        self.statements.append(" ".join(sql.split()))
        return "INSERT 0 3"

    async def fetchval(self, sql, *args):
        assert self.in_transaction, f"outside the transaction: {sql[:60]}"
        self.statements.append(" ".join(sql.split()))
        return datetime.now(timezone.utc) if "now()" in sql else 3


class _Pool:
    def __init__(self):
        self.conn = _Conn()

    def acquire(self):
        conn = self.conn

        class _Acquire:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False
        return _Acquire()


def _recorded(rebuild):
    pool = _Pool()
    with patch("core.pg.require_revision", new=AsyncMock()):
        asyncio.run(rebuild(pool))
    return pool.conn


class TestTheRebuildIsOneTransaction:
    def test_every_statement_runs_inside_one_transaction(self):
        conn = _recorded(rebuild_silver)
        assert conn.transactions == 1

    def test_the_statements_in_order(self):
        """Bound the lock wait, clear, insert once, count, stamp. Mutation:
        a `TRUNCATE` back as the clear, the UPDATE back after the INSERT, or
        the `lock_timeout` dropped or set after the clear."""
        statements = _recorded(rebuild_silver).statements
        assert statements[0] == f"SET LOCAL lock_timeout = '{REBUILD_LOCK_TIMEOUT}'"
        assert statements[1] == CLEAR_SQL == f"DELETE FROM {SILVER_TABLE}"
        assert statements[2] == " ".join(insert_sql().split())
        assert statements[3] == f"SELECT count(*) FROM {SILVER_TABLE}"
        assert len(statements) == 6
        assert not [s for s in statements if re.match(r"(UPDATE|TRUNCATE)\b", s)]
        assert sum(s.startswith("WITH ") and " INSERT INTO " in s for s in statements) == 1

    def test_the_bound_is_under_the_pools_command_timeout(self):
        """Above the pool's 30 s, asyncpg cancels first and the run records a
        bare `TimeoutError` naming no lock."""
        assert REBUILD_LOCK_TIMEOUT.endswith("s")
        assert 0 < float(REBUILD_LOCK_TIMEOUT[:-1]) < 30

    def test_it_moves_the_same_watermark_the_mirror_uses(self):
        """So a check can tell 'not rebuilt yet' from 'rebuilt wrong' without a
        second mechanism to learn."""
        source = inspect.getsource(rebuild_silver)
        assert "_WATERMARK_OK" in source

    def test_it_raises_rather_than_reporting_success(self):
        """Unlike the mirror in the sync path: the derivation records the
        failure, and a rebuild that reports success while writing nothing is
        the worst outcome."""
        source = inspect.getsource(rebuild_silver)
        assert "except" not in source
