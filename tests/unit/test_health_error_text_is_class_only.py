"""The health routes publish a failure's class, never its text (L3-06).

With Postgres unreachable, `GET /api/health/data-quality` answered
`{"status": "error", "error": "ConnectionRefusedError: [Errno 61] Connect call
failed ('127.0.0.1', 1)"}`, and `/api/health/detailed` carried Meilisearch's
client exception text: a driver's message names hosts, ports and the
database user. Both publish the class alone now, as `/api/health` does, and
log the text. Who may open them is unchanged (owner decision O8).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture

SECRET = "Connect call failed ('10.1.2.3', 5432) for user ks_app"


def test_data_quality_says_the_class_alone(admin_client):  # noqa: F811
    """Mutation: f"{type(e).__name__}: {e}" again — the host is in the body."""
    with patch("core.dq_journal.health_runs",
               new=AsyncMock(side_effect=ConnectionRefusedError(SECRET))):
        response = admin_client.get("/api/health/data-quality")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body == {"status": "error", "error": "ConnectionRefusedError"}
    assert "10.1.2.3" not in response.text


def test_detailed_says_the_class_alone(admin_client, monkeypatch):  # noqa: F811
    import httpx

    class Refusing(httpx.AsyncClient):
        async def get(self, *args, **kwargs):
            raise httpx.ConnectError(SECRET)

    monkeypatch.setattr(httpx, "AsyncClient", Refusing)
    response = admin_client.get("/api/health/detailed")
    assert response.status_code == 200, response.text
    assert response.json()["components"]["meilisearch"] == {
        "status": "unavailable", "error": "ConnectError"}
    assert "10.1.2.3" not in response.text
