"""A pool that records every statement it is handed, for the rebuilds' tests.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch


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
