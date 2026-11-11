"""A manual expense Postgres would refuse is refused here first (L3-02).

`POST /api/expenses` took a date the pattern lets through but the calendar
does not (2026-02-30, 0000-01-01), an amount past `NUMERIC(12,2)` or `1e400`
(inf), and a NUL in any text field, and each answered 500: `strptime` raised
out of the handler, or asyncpg refused the INSERT. Each is a 400 or 422 now,
and the store is never asked.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock

import pytest

from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture

BASE = {"expense_date": "2026-02-10", "amount": 5, "category": "c", "expense_type": "t"}


@pytest.fixture
def store(monkeypatch):
    fake = AsyncMock()
    fake.add_expense = AsyncMock(return_value={"id": 1})
    monkeypatch.setattr("web.routes.api.expenses.get_store", AsyncMock(return_value=fake))
    return fake


@pytest.mark.parametrize("body,status,field", [
    ({**BASE, "expense_date": "2026-02-30"}, 400, "expense_date"),
    ({**BASE, "expense_date": "0000-01-01"}, 400, "expense_date"),
    ({**BASE, "expense_date": "1999-12-31"}, 400, "expense_date"),
    ({**BASE, "amount": 99_999_999_999}, 422, "amount"),
    ({**BASE, "note": "a\x00b"}, 422, "note"),
    ({**BASE, "category": "a\x00b"}, 422, "category"),
    ({**BASE, "note": "x" * 2001}, 422, "note"),
], ids=["feb30", "year0", "before-window", "amount-overflow", "nul-note",
        "nul-category", "long-note"])
def test_refused_before_the_store(admin_client, store, body, status, field):  # noqa: F811
    """Mutation: drop the bound or the date check — a 500, or the store asked."""
    response = admin_client.post("/api/expenses", json=body)
    assert response.status_code == status, response.text
    assert field in response.text
    store.add_expense.assert_not_awaited()


def test_an_infinite_amount_is_refused(admin_client, store):  # noqa: F811
    response = admin_client.post(
        "/api/expenses", headers={"content-type": "application/json"},
        content=b'{"expense_date":"2026-02-10","amount":1e400,"category":"c",'
                b'"expense_type":"t"}')
    assert response.status_code == 422, response.text
    store.add_expense.assert_not_awaited()


def test_a_good_expense_is_written(admin_client, store):  # noqa: F811
    response = admin_client.post("/api/expenses", json={**BASE, "note": "ok"})
    assert response.status_code == 200, response.text
    assert store.add_expense.await_args.kwargs["expense_date"] == date(2026, 2, 10)


def test_a_nan_amount_is_refused(admin_client, store):  # noqa: F811
    response = admin_client.post(
        "/api/expenses", headers={"content-type": "application/json"},
        content=b'{"expense_date":"2026-02-10","amount":NaN,"category":"c",'
                b'"expense_type":"t"}')
    assert response.status_code == 422, response.text
    store.add_expense.assert_not_awaited()
