"""An SMS audience id past its column is a 400, in both forms (L3-01).

`category_id` and `source_id` reach the audience as a comma-separated string
(`_int_list`) and inside `or_groups` as JSON lists; neither was bounded, so
`category_id=99999999999` reached asyncpg and the route answered 500. Both
forms are held to the INTEGER column now, by the same bound.
"""
from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from core.validators import INT32_MAX
from tests.integration.test_sms_audience_api import _FakeStore, _headers
from web.main import app
from web.routes.api import customers as api

SEGMENTS = "/api/customers/sms-segments"
EXPORT = "/api/customers/sms-segments/export/csv"
TOO_BIG = INT32_MAX + 1


@pytest.fixture
def store(monkeypatch):
    from web.ratelimit import limiter

    limiter.reset()
    fake = _FakeStore()

    async def _get_store():
        return fake

    monkeypatch.setattr("web.routes.api.customers.get_store", _get_store)
    yield fake
    limiter.reset()


@pytest.mark.parametrize("path,params,field", [
    (SEGMENTS, {"category_id": str(TOO_BIG)}, "category_id"),
    (SEGMENTS, {"source_id": str(TOO_BIG)}, "source_id"),
    (SEGMENTS, {"category_id": str(-INT32_MAX - 2)}, "category_id"),
    (EXPORT, {"category_id": f"1,{TOO_BIG}", "campaign": "probe"}, "category_id"),
])
def test_a_flat_id_past_its_column_is_a_400_naming_it(store, path, params, field):
    """Mutation: append `int(piece)` unchecked again — the store is asked."""
    response = TestClient(app).get(path, params=params, headers=_headers())
    assert response.status_code == 400, response.text
    assert response.json()["detail"].startswith(f"{field}: ")
    assert store.calls == []


@pytest.mark.parametrize("group", [{"source_id": [TOO_BIG]}, {"category_id": [1, TOO_BIG]}])
def test_an_id_inside_or_groups_is_a_400_naming_the_group(store, group):
    response = TestClient(app).get(
        SEGMENTS, params={"or_groups": json.dumps([group])}, headers=_headers())
    assert response.status_code == 400, response.text
    assert "group 2" in response.json()["detail"]
    assert store.calls == []


def test_ids_inside_the_column_still_reach_the_store(store):
    response = TestClient(app).get(
        SEGMENTS, params={"category_id": f"1,{INT32_MAX}",
                          "or_groups": json.dumps([{"source_id": [INT32_MAX]}])},
        headers=_headers())
    assert response.status_code == 200, response.text
    assert store.calls


@pytest.mark.parametrize("value,accepted", [
    (INT32_MAX, True), (-INT32_MAX - 1, True), (1, True),
    (INT32_MAX + 1, False), (-INT32_MAX - 2, False),
])
def test_the_two_forms_share_one_bound(value, accepted):
    """The JSON item (`_DbInt`) accepts exactly what the flat parser does."""
    from fastapi import HTTPException
    from pydantic import TypeAdapter, ValidationError

    try:
        TypeAdapter(api._DbInt).validate_python(value)
        json_ok = True
    except ValidationError:
        json_ok = False
    try:
        api._int_list(str(value), "category_id")
        flat_ok = True
    except HTTPException:
        flat_ok = False
    assert json_ok == flat_ok == accepted
