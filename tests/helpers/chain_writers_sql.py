"""A connection that records the statements a chain writer sends, and the
golden they are compared with.
"""
from __future__ import annotations

from pathlib import Path


GOLDEN = Path(__file__).resolve().parents[1] / "integration" / "golden" / "chain_writers_sql.json"


class _Recorder:
    """A connection that answers what the writers ask and keeps what they sent."""

    def __init__(self, rows=None):
        self.calls = []
        self.rows = rows or (lambda sql: [])
        self.depth = 0

    def is_in_transaction(self):
        return self.depth > 0

    async def execute(self, sql, *args, **kw):
        self.calls.append(["execute", sql, len(args)])
        return "UPDATE 1"

    async def executemany(self, sql, rows, **kw):
        self.calls.append(["executemany", sql, len(list(rows))])

    async def fetch(self, sql, *args, **kw):
        self.calls.append(["fetch", sql, len(args)])
        return self.rows(sql)

    async def fetchval(self, sql, *args, **kw):
        self.calls.append(["fetchval", sql, len(args)])
        if "FOR UPDATE" in sql and "chain_watermarks" in sql:
            return '{"stamps": [], "previous": null}'
        return None

    async def fetchrow(self, sql, *args, **kw):
        self.calls.append(["fetchrow", sql, len(args)])
        return None

    def transaction(self):
        recorder = self

        class _Tx:
            async def __aenter__(self):
                recorder.calls.append(["begin"])
                recorder.depth += 1

            async def __aexit__(self, *exc):
                recorder.depth -= 1
                recorder.calls.append(["end", exc[0] is None])
                return False

        return _Tx()
