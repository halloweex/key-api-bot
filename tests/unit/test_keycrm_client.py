"""
Tests for core.keycrm module.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from core.keycrm import KeyCRMClient
from core.exceptions import KeyCRMAPIError, KeyCRMConnectionError


class TestKeyCRMClient:
    """Tests for KeyCRMClient class."""

    def test_init_with_api_key(self):
        """Should initialize with provided API key."""
        client = KeyCRMClient(api_key="test-key")
        assert client.api_key == "test-key"
        assert client.base_url == "https://openapi.keycrm.app/v1"

    def test_init_without_api_key_raises(self):
        """Should raise error if no API key provided."""
        with patch("core.keycrm.KEYCRM_API_KEY", ""):
            with pytest.raises(ValueError, match="KEYCRM_API_KEY is required"):
                KeyCRMClient(api_key=None)

    def test_headers(self):
        """Should generate correct auth headers."""
        client = KeyCRMClient(api_key="my-secret-key")
        headers = client.headers
        assert headers["Authorization"] == "Bearer my-secret-key"
        assert headers["Accept"] == "application/json"

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Should work as async context manager."""
        client = KeyCRMClient(api_key="test-key")
        async with client:
            assert client._client is not None
        assert client._client is None

    @pytest.mark.asyncio
    async def test_get_orders_success(self):
        """Should fetch orders successfully."""
        client = KeyCRMClient(api_key="test-key")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"data": [{"id": 1}]}'
        mock_response.json.return_value = {"data": [{"id": 1}]}

        with patch.object(client, "_client") as mock_client:
            mock_client.request = AsyncMock(return_value=mock_response)
            client._client = mock_client

            result = await client.get_orders({"limit": 10})

        assert result == {"data": [{"id": 1}]}

    @pytest.mark.asyncio
    async def test_api_error_handling(self):
        """Should raise KeyCRMAPIError on 4xx/5xx responses."""
        client = KeyCRMClient(api_key="test-key")

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Not found"

        with patch.object(client, "_client") as mock_client:
            mock_client.request = AsyncMock(return_value=mock_response)
            client._client = mock_client

            with pytest.raises(KeyCRMAPIError) as exc_info:
                await client.get_orders()

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_search_orders_by_id(self):
        """Should search by order ID when query is numeric."""
        client = KeyCRMClient(api_key="test-key")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"id": 12345}'
        mock_response.json.return_value = {"id": 12345}

        with patch.object(client, "_client") as mock_client:
            mock_client.request = AsyncMock(return_value=mock_response)
            client._client = mock_client

            result = await client.search_orders("12345")

        assert result == {"data": [{"id": 12345}], "total": 1}

    @pytest.mark.asyncio
    async def test_search_orders_by_email(self):
        """Should search by email when query contains @."""
        client = KeyCRMClient(api_key="test-key")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"data": []}'
        mock_response.json.return_value = {"data": []}

        with patch.object(client, "_client") as mock_client:
            mock_client.request = AsyncMock(return_value=mock_response)
            client._client = mock_client

            await client.search_orders("test@example.com")

            # Verify the filter was passed
            call_args = mock_client.request.call_args
            assert "filter[buyer_email]" in call_args.kwargs.get("params", {})

    @pytest.mark.asyncio
    async def test_fetch_all_single_page(self):
        """Should return all items when only one page."""
        client = KeyCRMClient(api_key="test-key")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"data": [{"id": 1}, {"id": 2}]}'
        mock_response.json.return_value = {"data": [{"id": 1}, {"id": 2}]}

        with patch.object(client, "_client") as mock_client:
            mock_client.request = AsyncMock(return_value=mock_response)
            client._client = mock_client

            result = await client.fetch_all("order", page_size=50)

        assert len(result) == 2
        assert result[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_paginate_generator(self):
        """Should yield batches as async generator."""
        client = KeyCRMClient(api_key="test-key")

        responses = [
            {"data": [{"id": 1}, {"id": 2}]},
            {"data": [{"id": 3}]},
            {"data": []},
        ]
        call_count = 0

        async def mock_request(*args, **kwargs):
            nonlocal call_count
            mock = MagicMock()
            mock.status_code = 200
            mock.content = b'data'
            mock.json.return_value = responses[min(call_count, len(responses) - 1)]
            call_count += 1
            return mock

        with patch.object(client, "_client") as mock_client:
            mock_client.request = mock_request
            client._client = mock_client

            batches = []
            async for batch in client.paginate("order", page_size=2):
                batches.append(batch)

        assert len(batches) == 2
        assert batches[0] == [{"id": 1}, {"id": 2}]
        assert batches[1] == [{"id": 3}]
