"""Reclassifying a manager must not restate the reports already written.

`sales_type` is materialised into Silver from the manager's classification
as of the order's own date (the owner's ruling of 2026-08-20), from
`manager_classifications`, by one CASE every consumer derives it from
(`silver_sales_type_case`).

Until step 14's S14-11 this module pinned those semantics on DuckDB's writer
and DuckDB's own warehouse rebuild — a change applying forward, three eras
intact, a baseline for every synced manager, a failed write leaving the open
interval open, and an exhibition order never counted as retail. That writer
went with `KS_WRITE_MANAGERS`, and DuckDB derives nothing since step 13. The
same properties are held in Postgres: chain 5's writer by
`tests/integration/test_managers_chain_writer.py` (forward close-and-open in
one transaction, the same date twice is one row, a failure before the insert
leaves everything as it was, a baseline for every manager), and Postgres'
Silver, every sales type on every branch with classified intervals and the
exhibition source, by `tests/unit/test_goals_history_silver.py` and
`tests/unit/test_goals_off_duckdb_silver.py`. What stays here is the one
pure rule the goal history's filter keeps.
"""
from __future__ import annotations

import pytest


class TestConsumersUseTheOneDefinition:
    def test_an_unknown_sales_type_is_refused_rather_than_read_as_retail(self):
        """It used to fall through the else and quietly mean 'retail'."""
        from core.repositories.goals import _silver_history_where

        with pytest.raises(ValueError):
            _silver_history_where("wholsale")
        # Not a return, and not a sale to the company's own legal entity
        # (the owner's decision of 2026-11-14), on every sales type.
        assert _silver_history_where("all") == (
            "NOT s.is_return AND (s.buyer_id IS NULL OR s.buyer_id NOT IN (17358))", [])
