"""The SMS roster CSV never hands a spreadsheet a formula a customer typed (L3-07).

`fullName`, `city` and `lastOrderItems` come from KeyCRM buyers and order
lines, which customers type into the shop. Written verbatim, a buyer named
`=HYPERLINK("http://example.invalid","x")` or living in `+1+1` reached the file
a marketer opens in Excel as a formula. Each of those three cells now gets a
leading apostrophe when it starts with a formula character; the phone column,
written by this code for the gateway, stays `+380…`.
"""
from __future__ import annotations

import csv
import io

import pytest
from fastapi.testclient import TestClient

from web.csv_cells import text_cell
from web.main import app
from tests.integration.test_sms_audience_api import _FakeStore, _customer, _headers

CSV_PATH = "/api/customers/sms-segments/export/csv"


class TestTheCell:
    @pytest.mark.parametrize("raw", ["=1+1", "+1+1", "-2", "@SUM(A1)", "\t=1", "\r=1",
                                     '=HYPERLINK("http://example.invalid","x")'])
    def test_a_formula_lead_is_quoted(self, raw):
        assert text_cell(raw) == "'" + raw

    @pytest.mark.parametrize("raw", ["Олена", "Kyiv", "", "1+1", "Crème = good", None, 42])
    def test_anything_else_is_unchanged(self, raw):
        assert text_cell(raw) == raw


@pytest.fixture
def store(monkeypatch):
    from web.ratelimit import limiter

    limiter.reset()
    fake = _FakeStore()
    hostile = _customer(1, "target")
    hostile.update(fullName='=HYPERLINK("http://example.invalid","x")', city="+1+1",
                   lastOrderItems="-Крем, @тонер")
    fake.customers = [hostile]

    async def _get_store():
        return fake

    monkeypatch.setattr("web.routes.api.customers.get_store", _get_store)
    yield fake
    limiter.reset()


class TestTheExport:
    def test_customer_text_is_never_a_formula(self, store):
        """Mutation: write `c["fullName"]` bare again."""
        response = TestClient(app).get(
            f"{CSV_PATH}?campaign=probe&grouping=single", headers=_headers())
        assert response.status_code == 200
        body = response.content.decode("utf-8-sig")
        assert response.content.startswith(b"\xef\xbb\xbf"), "the BOM went"
        header, row = list(csv.reader(io.StringIO(body)))[:2]
        cell = dict(zip(header, row))
        assert cell["full_name"] == '\'=HYPERLINK("http://example.invalid","x")'
        assert cell["city"] == "'+1+1"
        assert cell["last_order_items"] == "'-Крем, @тонер"
        # Written by this code for the gateway: E.164, untouched.
        assert cell["phone"] == "+380900000001"
        assert cell["orders"] == "1" and cell["ltv"] == "900.0"
