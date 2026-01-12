"""
Tests for core.pagination module.
"""
import pytest
from unittest.mock import MagicMock, call

from core.pagination import KeyCRMPaginator
from core.exceptions import KeyCRMAPIError, KeyCRMConnectionError, KeyCRMDataError


class TestKeyCRMPaginator:
    """Tests for KeyCRMPaginator class."""

    def test_single_page(self):
        """Single page should yield one batch."""
        fetch_func = MagicMock(return_value={
            "data": [{"id": 1}, {"id": 2}]
        })

        paginator = KeyCRMPaginator(fetch_func, page_size=50, rate_limit=0)
        batches = list(paginator.paginate({}))

        assert len(batches) == 1
        assert batches[0] == [{"id": 1}, {"id": 2}]
        fetch_func.assert_called_once()

    def test_multiple_pages(self):
        """Multiple pages should yield multiple batches."""
        # First page: 3 items (full page with page_size=3)
        # Second page: 2 items (partial page)
        fetch_func = MagicMock(side_effect=[
            {"data": [{"id": 1}, {"id": 2}, {"id": 3}]},
            {"data": [{"id": 4}, {"id": 5}]},
        ])

        paginator = KeyCRMPaginator(fetch_func, page_size=3, rate_limit=0)
        batches = list(paginator.paginate({}))

        assert len(batches) == 2
        assert batches[0] == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert batches[1] == [{"id": 4}, {"id": 5}]
        assert fetch_func.call_count == 2

    def test_empty_response(self):
        """Empty response should stop pagination."""
        fetch_func = MagicMock(return_value={"data": []})

        paginator = KeyCRMPaginator(fetch_func, page_size=50, rate_limit=0)
        batches = list(paginator.paginate({}))

        assert len(batches) == 0

    def test_max_pages(self):
        """max_pages should limit pagination."""
        fetch_func = MagicMock(return_value={
            "data": [{"id": i} for i in range(50)]  # Full page
        })

        paginator = KeyCRMPaginator(fetch_func, page_size=50, rate_limit=0, max_pages=2)
        batches = list(paginator.paginate({}))

        assert len(batches) == 2
        assert fetch_func.call_count == 2

    def test_api_error(self):
        """API error should raise KeyCRMAPIError."""
        fetch_func = MagicMock(return_value={"error": "Rate limited"})

        paginator = KeyCRMPaginator(fetch_func, page_size=50, rate_limit=0)

        with pytest.raises(KeyCRMAPIError) as exc_info:
            list(paginator.paginate({}))

        assert "Rate limited" in str(exc_info.value)

    def test_connection_error(self):
        """Connection error should raise KeyCRMConnectionError."""
        fetch_func = MagicMock(side_effect=Exception("Network error"))

        paginator = KeyCRMPaginator(fetch_func, page_size=50, rate_limit=0)

        with pytest.raises(KeyCRMConnectionError) as exc_info:
            list(paginator.paginate({}))

        assert "Network error" in str(exc_info.value)

    def test_invalid_response_type(self):
        """Non-dict response should raise KeyCRMDataError."""
        fetch_func = MagicMock(return_value="invalid")

        paginator = KeyCRMPaginator(fetch_func, page_size=50, rate_limit=0)

        with pytest.raises(KeyCRMDataError) as exc_info:
            list(paginator.paginate({}))

        assert "response type" in str(exc_info.value).lower()

    def test_missing_data_field(self):
        """Response without 'data' should raise KeyCRMDataError."""
        fetch_func = MagicMock(return_value={"meta": {}})

        paginator = KeyCRMPaginator(fetch_func, page_size=50, rate_limit=0)

        with pytest.raises(KeyCRMDataError) as exc_info:
            list(paginator.paginate({}))

        assert "data" in str(exc_info.value).lower()

    def test_data_not_list(self):
        """Response with non-list 'data' should raise KeyCRMDataError."""
        fetch_func = MagicMock(return_value={"data": "not a list"})

        paginator = KeyCRMPaginator(fetch_func, page_size=50, rate_limit=0)

        with pytest.raises(KeyCRMDataError) as exc_info:
            list(paginator.paginate({}))

        assert "list" in str(exc_info.value)

    def test_fetch_all_flattened(self):
        """fetch_all should return flattened list."""
        fetch_func = MagicMock(side_effect=[
            {"data": [{"id": 1}, {"id": 2}]},
            {"data": [{"id": 3}]},
        ])

        paginator = KeyCRMPaginator(fetch_func, page_size=2, rate_limit=0)
        items = paginator.fetch_all({})

        assert items == [{"id": 1}, {"id": 2}, {"id": 3}]

    def test_fetch_all_not_flattened(self):
        """fetch_all with flatten=False should return list of batches."""
        fetch_func = MagicMock(side_effect=[
            {"data": [{"id": 1}, {"id": 2}]},
            {"data": [{"id": 3}]},
        ])

        paginator = KeyCRMPaginator(fetch_func, page_size=2, rate_limit=0)
        batches = paginator.fetch_all({}, flatten=False)

        assert batches == [[{"id": 1}, {"id": 2}], [{"id": 3}]]

    def test_count(self):
        """count should return total number of items."""
        fetch_func = MagicMock(side_effect=[
            {"data": [{"id": 1}, {"id": 2}, {"id": 3}]},
            {"data": [{"id": 4}, {"id": 5}]},
        ])

        paginator = KeyCRMPaginator(fetch_func, page_size=3, rate_limit=0)
        total = paginator.count({})

        assert total == 5

    def test_params_not_modified(self):
        """Original params should not be modified."""
        fetch_func = MagicMock(return_value={"data": []})
        original_params = {"filter": "test"}

        paginator = KeyCRMPaginator(fetch_func, page_size=50, rate_limit=0)
        list(paginator.paginate(original_params))

        assert "page" not in original_params
        assert "limit" not in original_params

    def test_page_size_in_params(self):
        """Page size should be added to params."""
        fetch_func = MagicMock(return_value={"data": []})

        paginator = KeyCRMPaginator(fetch_func, page_size=25, rate_limit=0)
        list(paginator.paginate({}))

        call_params = fetch_func.call_args[0][0]
        assert call_params["limit"] == 25
        assert call_params["page"] == 1
