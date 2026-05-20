"""Tests for Layer-1 integrity checks.

These take a real DuckDB connection (set up in tmp_path) and validate that
each check fires on synthetic violations and stays silent on clean data.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from core.data_quality import (
    KNOWN_SOURCE_IDS,
    KNOWN_STATUS_IDS,
    Severity,
    check_internal_integrity,
    summarize_issues,
    _fk_orphan_check,
    _null_constraint_check,
    _pk_uniqueness_check,
    _value_domain_check,
)
from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await s.connect()
    return s


def _insert_minimal_order(conn, oid: int, **kw):
    defaults = {
        "source_id": 1, "status_id": 12, "grand_total": "100.00",
        "ordered_at": "2026-04-01T10:00:00+03:00",
        "created_at": "2026-04-01T10:00:00+03:00",
        "updated_at": "2026-04-01T10:00:00+03:00",
        "buyer_id": None, "manager_id": None,
        "manager_comment": None, "promocode": None,
    }
    defaults.update(kw)
    conn.execute(
        "INSERT INTO orders (id, source_id, status_id, grand_total, ordered_at, "
        "created_at, updated_at, buyer_id, manager_id, manager_comment, promocode) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [oid, defaults["source_id"], defaults["status_id"], defaults["grand_total"],
         defaults["ordered_at"], defaults["created_at"], defaults["updated_at"],
         defaults["buyer_id"], defaults["manager_id"], defaults["manager_comment"],
         defaults["promocode"]],
    )


# ─── PK uniqueness check ──────────────────────────────────────────────────────


class TestPkUniqueness:
    @pytest.mark.asyncio
    async def test_clean_orders_no_issues(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1)
                _insert_minimal_order(conn, 2)
                issues = _pk_uniqueness_check(conn, "orders")
            assert issues == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_empty_table_no_issues(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                issues = _pk_uniqueness_check(conn, "orders")
            assert issues == []
        finally:
            await store.close()


# ─── FK orphan check ──────────────────────────────────────────────────────────


class TestFkOrphans:
    @pytest.mark.asyncio
    async def test_clean_no_orphans(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 100)
                conn.execute(
                    "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [1, 100, 1, "test", 1, 100.0],
                )
                issues = _fk_orphan_check(conn, "order_products", "order_id", "orders")
            assert issues == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_orphan_detected(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                conn.execute(
                    "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [1, 99999, 1, "orphan", 1, 100.0],
                )
                issues = _fk_orphan_check(conn, "order_products", "order_id", "orders")
            assert len(issues) == 1
            assert issues[0].severity == Severity.CRITICAL
            assert issues[0].count == 1
            assert issues[0].check_name == "fk_orphan_order_products_order_id"
            assert 99999 in issues[0].sample_ids
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_multiple_orphans_sampled(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                for i, fake_oid in enumerate(range(90000, 90015)):
                    conn.execute(
                        "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        [i + 1, fake_oid, 1, "orphan", 1, 100.0],
                    )
                issues = _fk_orphan_check(conn, "order_products", "order_id", "orders")
            assert len(issues) == 1
            assert issues[0].count == 15
            assert 1 <= len(issues[0].sample_ids) <= 10
        finally:
            await store.close()


# ─── NULL constraint check ────────────────────────────────────────────────────


class TestNullChecks:
    @pytest.mark.asyncio
    async def test_clean_no_nulls(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1)
                issues = _null_constraint_check(conn, "orders", "ordered_at")
            assert issues == []
        finally:
            await store.close()


# ─── Value domain check ───────────────────────────────────────────────────────


class TestValueDomain:
    @pytest.mark.asyncio
    async def test_known_values_pass(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1, status_id=12)
                _insert_minimal_order(conn, 2, status_id=19)
                issues = _value_domain_check(
                    conn, "orders", "status_id", KNOWN_STATUS_IDS, Severity.WARN,
                )
            assert issues == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_unknown_status_detected(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1, status_id=999)
                issues = _value_domain_check(
                    conn, "orders", "status_id", KNOWN_STATUS_IDS, Severity.WARN,
                )
            assert len(issues) == 1
            assert issues[0].count == 1
            assert issues[0].severity == Severity.WARN
            assert "999" in issues[0].description
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_unknown_source_detected(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1, source_id=99)
                issues = _value_domain_check(
                    conn, "orders", "source_id", KNOWN_SOURCE_IDS, Severity.WARN,
                )
            assert len(issues) == 1
            assert "99" in issues[0].description
        finally:
            await store.close()


# ─── Aggregate check_internal_integrity ───────────────────────────────────────


class TestCheckInternalIntegrity:
    @pytest.mark.asyncio
    async def test_clean_db_zero_issues(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                issues = check_internal_integrity(conn)
            assert issues == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_combined_violations_aggregated(self, tmp_path):
        store = await _make_store(tmp_path)
        try:
            async with store.connection() as conn:
                _insert_minimal_order(conn, 1, status_id=999)
                _insert_minimal_order(conn, 2, source_id=99)
                conn.execute(
                    "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [1, 88888, 1, "orphan", 1, 100.0],
                )
                issues = check_internal_integrity(conn)

            check_names = {i.check_name for i in issues}
            assert "fk_orphan_order_products_order_id" in check_names
            assert "value_domain_orders_status_id" in check_names
            assert "value_domain_orders_source_id" in check_names

            critical = [i for i in issues if i.severity == Severity.CRITICAL]
            warn = [i for i in issues if i.severity == Severity.WARN]
            assert len(critical) >= 1
            assert len(warn) >= 2
        finally:
            await store.close()


# ─── summarize_issues ─────────────────────────────────────────────────────────


class TestSummarizeIssues:
    def test_stable_shape(self):
        s = summarize_issues([])
        assert set(s.keys()) == {sv.value for sv in Severity}
        assert all(v == 0 for v in s.values())
