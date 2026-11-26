"""A fake GA4 for the capture: the key, the periods and the people per step."""
from __future__ import annotations

import json
from datetime import date
from typing import Dict, List, Optional, Set, Tuple

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from core import ga4
from web.services import ga4_funnel_service as svc


TODAY = date(2026, 11, 13)


_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


KEY_INFO = {
    "type": "service_account",
    "client_email": "ga4-reader@example.iam.gserviceaccount.com",
    "private_key": _KEY.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()).decode(),
    "token_uri": "https://oauth2.example/token",
}


CHANNELS = {"Paid Social": 0.5, "Organic Social": 0.3, "Direct": 0.15, "Unassigned": 0.05}


DEVICES = {"mobile": 0.8, "desktop": 0.2}


_STEP_SHARE = (1.0, 0.6, 0.1, 0.05, 0.03)


def people(scope: str, start: date, end: date, share: float = 1.0) -> List[int]:
    """What the fake answers: a function of the request alone, so a test can
    say what a stored row must hold."""
    entered = (1000 + start.toordinal() % 97 + 10 * (end - start).days) * (8 if scope == "ua" else 10)
    return [int(entered * share * s) for s in _STEP_SHARE]


def _table(rows: Dict[str, List[int]], breakdown: str) -> dict:
    return {"funnelTable": {
        "dimensionHeaders": [{"name": "funnelStepName"}, {"name": breakdown}],
        "metricHeaders": [{"name": "activeUsers"}, {"name": "funnelStepCompletionRate"}],
        "rows": [
            {"dimensionValues": [{"value": f"{i + 1}. {ga4.COLUMNS[i]}"}, {"value": key}],
             "metricValues": [{"value": str(n)}, {"value": "0.5"}]}
            for key, counts in rows.items() for i, n in enumerate(counts)
        ],
    }}


class FakeGA4:
    """The token endpoint and GA4's runFunnelReport. Records each funnel asked
    as (start, end, breakdown dimension, scope); `fail_at` answers 429 on that
    funnel request (1-based); `empty` periods are answered with no rows."""

    def __init__(self, *, fail_at: Optional[int] = None,
                 empty: Set[Tuple[date, date]] = frozenset()) -> None:
        self.fail_at = fail_at
        self.empty = set(empty)
        self.asked: List[Tuple[date, date, str, str]] = []

    def periods(self) -> List[Tuple[date, date]]:
        """The periods asked, in order, each once."""
        return list(dict.fromkeys((s, e) for s, e, _, _ in self.asked))

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "oauth2.example":
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
        body = json.loads(request.content)
        span = body["dateRanges"][0]
        start, end = date.fromisoformat(span["startDate"]), date.fromisoformat(span["endDate"])
        breakdown = body["funnelBreakdown"]["breakdownDimension"]["name"]
        scope = "ua" if "dimensionFilter" in body else "all"
        self.asked.append((start, end, breakdown, scope))
        if self.fail_at is not None and len(self.asked) == self.fail_at:
            return httpx.Response(429, json={"error": {"message": "Exhausted property tokens per hour"}})
        if (start, end) in self.empty:
            return httpx.Response(200, json=_table({}, breakdown))
        shares = CHANNELS if breakdown == svc.CHANNEL else DEVICES
        rows = {"RESERVED_TOTAL": people(scope, start, end)}
        rows.update({key: people(scope, start, end, share) for key, share in shares.items()})
        return httpx.Response(200, json=_table(rows, breakdown))

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self))


@pytest.fixture
def ga4_key(monkeypatch, tmp_path):
    """A configured GA4: a generated key file and a property id."""
    key = tmp_path / "ga4-reader.json"
    key.write_text(json.dumps(KEY_INFO))
    monkeypatch.setenv("KS_GA4_KEY_FILE", str(key))
    monkeypatch.setenv("KS_GA4_PROPERTY_ID", "123")
    ga4._reset_token()
    yield key
    ga4._reset_token()
