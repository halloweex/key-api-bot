"""A fake GA4 Data API for the funnel: the service account, and the reports it
answers.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Dict, List

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from core import ga4
from web.services import ga4_funnel_service as svc


KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


PEM = KEY.private_bytes(
    serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
).decode()


INFO = {
    "type": "service_account",
    "client_email": "ga4-reader@example.iam.gserviceaccount.com",
    "private_key": PEM,
    "token_uri": "https://oauth2.example/token",
}


OCT = (date(2026, 10, 1), date(2026, 10, 31))


SEP = (date(2026, 8, 31), date(2026, 9, 30))


OCT_2025 = (date(2025, 10, 1), date(2025, 10, 31))


#: channel → people at each step, per window; RESERVED_TOTAL is the whole site.
CHANNELS = {
    OCT: {
        "RESERVED_TOTAL": [1000, 600, 80, 40, 30],
        "Paid Social": [500, 350, 20, 10, 6],
        "Organic Social": [400, 200, 50, 25, 20],
        "Unassigned": [100, 50, 10, 5, 4],
    },
    SEP: {
        "RESERVED_TOTAL": [900, 500, 60, 30, 20],
        "Paid Social": [450, 300, 15, 8, 5],
        "Organic Social": [450, 200, 45, 22, 15],
    },
    OCT_2025: {
        "RESERVED_TOTAL": [1500, 600, 60, 25, 15],
        "Paid Social": [1500, 600, 60, 25, 15],
    },
}


DEVICES = {
    window: {
        "RESERVED_TOTAL": counts["RESERVED_TOTAL"],
        "mobile": [round(n * 0.8) for n in counts["RESERVED_TOTAL"]],
        "desktop": [n - round(n * 0.8) for n in counts["RESERVED_TOTAL"]],
    }
    for window, counts in CHANNELS.items()
}


def _table(rows: Dict[str, List[int]], breakdown: str | None) -> dict:
    dims = [{"name": "funnelStepName"}] + ([{"name": breakdown}] if breakdown else [])
    return {"funnelTable": {
        "dimensionHeaders": dims,
        "metricHeaders": [{"name": "activeUsers"}, {"name": "funnelStepCompletionRate"}],
        "rows": [
            {
                "dimensionValues": [{"value": f"{i + 1}. {ga4.COLUMNS[i]}"}]
                + ([{"value": key}] if breakdown else []),
                "metricValues": [{"value": str(n)}, {"value": "0.5"}],
            }
            for key, counts in rows.items()
            for i, n in enumerate(counts)
        ],
    }}


class _Google:
    """The token endpoint and GA4, answering from the tables above."""

    def __init__(self, *, down: bool = False, status: int = 200) -> None:
        self.down = down
        self.status = status
        self.tokens = 0
        self.funnels: List[dict] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "oauth2.example":
            self.tokens += 1
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
        if self.down:
            raise httpx.ConnectError("down")
        if self.status != 200:
            return httpx.Response(self.status, json={"error": {"message": "Exhausted property tokens per hour"}})
        body = json.loads(request.content)
        self.funnels.append(body)
        span = body["dateRanges"][0]
        window = (date.fromisoformat(span["startDate"]), date.fromisoformat(span["endDate"]))
        breakdown = body["funnelBreakdown"]["breakdownDimension"]["name"]
        source = CHANNELS if breakdown == svc.CHANNEL else DEVICES
        return httpx.Response(200, json=_table(source.get(window, {}), breakdown))
