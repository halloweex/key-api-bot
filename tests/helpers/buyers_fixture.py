"""Chain 4's tables on the test Postgres, and KeyCRM's buyers listing faked.

`buyers_in_postgres()` yields a pool that `core.pg.get_pool` answers with for
the block, with `bronze.buyers`, `bronze.buyer_contacts`, `app.buyer_gender`
and every owner row empty before and after.

`keycrm_serving_buyers()` answers KeyCRM's `GET /buyer` from given pages at
the HTTP transport, under the real `core.keycrm.KeyCRMClient`: its paging,
retries and parse all run. The names and phone numbers are synthetic.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Sequence
from unittest.mock import AsyncMock, patch

TABLES = ("bronze.buyer_contacts", "app.buyer_gender", "bronze.buyers")


@asynccontextmanager
async def buyers_in_postgres(dsn: str | None = None):
    import asyncpg

    pool = await asyncpg.create_pool(dsn or os.environ["KS_PG_DSN"], min_size=1, max_size=3)

    async def clean() -> None:
        async with pool.acquire() as conn:
            for table in TABLES:
                await conn.execute(f"DELETE FROM {table}")
            # Left behind, an owner row reads in a later module as a chain
            # owning a table with no local marker.
            await conn.execute(
                "DELETE FROM meta.chain_watermarks WHERE key LIKE 'owner:%'")

    await clean()
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield pool
    finally:
        await clean()
        await pool.close()


def buyer_payload(buyer_id: int, name: str) -> Dict[str, Any]:
    """One buyer as KeyCRM lists it, with a phone that is obviously fake."""
    return {"id": buyer_id, "full_name": name, "phone": [f"38096{buyer_id:07d}"]}


def keycrm_serving_buyers(monkeypatch, pages: Sequence[List[Dict[str, Any]]]) -> list:
    """KeyCRM's buyers listing served from `pages`, one list of payloads a
    page, `next_page_url` set on every page but the last; any other request
    is a 404. Returns the requests KeyCRM received, as they arrive."""
    import httpx

    from core import keycrm

    received: list = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(request)
        if not request.url.path.endswith("/buyer"):
            return httpx.Response(404, json={"message": "not faked"})
        page = int(request.url.params.get("page", "1"))
        data = pages[page - 1] if page <= len(pages) else []
        following = f"{request.url.copy_set_param('page', page + 1)}" \
            if page < len(pages) else None
        return httpx.Response(200, json={"data": data, "next_page_url": following})

    transport = httpx.MockTransport(handler)
    real = httpx.AsyncClient

    class _OverTheFake(real):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(keycrm.httpx, "AsyncClient", _OverTheFake)
    return received
