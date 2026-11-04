"""Google Analytics 4, read-only: the site's purchase funnel.

The shop's GA4 property is read by a service account that holds the Viewer
role on the property and no role in its Google Cloud project, so its key can
read reports and change nothing. The key is a JSON file kept on the host
outside the repository, which is public, and mounted read-only into web
(``KS_GA4_KEY_FILE``); ``KS_GA4_PROPERTY_ID`` names the property. Either one
missing is a supported state: nothing is asked of Google and the funnel route
answers "not configured".

**A funnel counts people, not hits.** It is closed: a person counts at a step
only after the steps before it, in order, and once. The Shopify sandbox
pixel's duplicate page views therefore do not inflate it.

**A funnel reaches back 14 months.** That is the longest event retention a
standard property has, and a funnel is built from events. An older range comes
back empty rather than as an error, while aggregated reports keep the whole
history. Measured on 2026-11-04: funnels began at 2025-09-01, and January to
August 2025 came back empty.

**GA4's trended funnel is not a daily funnel.** Over a range it counts a person
once, on their first day in it: for 2026-10-01..02 it gave 1 428 people
entering on the 2nd where the funnel of the 2nd alone gave 1 552. A per-day
series is therefore one funnel per day, never one trended call.
"""
from __future__ import annotations

import base64
import json
import os
import time
from datetime import date
from typing import Any, Dict, Optional

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

#: The funnel's steps as GA4 names their events, in order. The first item of
#: each pair is the column every reader of a funnel uses.
STEPS: tuple[tuple[str, str], ...] = (
    ("entered", "session_start"),
    ("viewed_product", "view_item"),
    ("added_to_cart", "add_to_cart"),
    ("began_checkout", "begin_checkout"),
    ("purchased", "purchase"),
)
COLUMNS: tuple[str, ...] = tuple(column for column, _ in STEPS)

#: The key a funnel table gives its whole-property row under.
TOTAL = ""

SCOPE = "https://www.googleapis.com/auth/analytics.readonly"
API = "https://analyticsdata.googleapis.com/v1alpha"
TIMEOUT_S = 20.0
#: GA4 caps a breakdown at 15 values; the channel groups number fewer.
BREAKDOWN_LIMIT = 15

# {"value": access token, "expires_at": epoch seconds}
_token: Dict[str, Any] = {}


class GA4Unavailable(Exception):
    """GA4 could not be asked, or refused what it was asked."""


def property_id() -> str:
    return os.getenv("KS_GA4_PROPERTY_ID", "").strip()


def key_file() -> str:
    return os.getenv("KS_GA4_KEY_FILE", "").strip()


def configured() -> bool:
    """Both the property and a key file are there. Readability is asked later."""
    path = key_file()
    return bool(property_id() and path and os.path.isfile(path))


def _reset_token() -> None:
    """For tests."""
    _token.clear()


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _load_key() -> Dict[str, Any]:
    try:
        with open(key_file(), encoding="utf-8") as fh:
            info = json.load(fh)
    except (OSError, ValueError) as exc:
        # The class only: a parse error can quote the file, and the file is a key.
        raise GA4Unavailable(f"GA4 key unreadable: {type(exc).__name__}") from None
    if not isinstance(info, dict) or not all(
        isinstance(info.get(k), str) for k in ("client_email", "private_key", "token_uri")
    ):
        raise GA4Unavailable("GA4 key lacks client_email, private_key or token_uri")
    return info


def assertion(info: Dict[str, Any], now: int) -> str:
    """A signed JWT asking ``token_uri`` for a read-only Analytics token."""
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    claims = _b64url(json.dumps({
        "iss": info["client_email"], "scope": SCOPE, "aud": info["token_uri"],
        "iat": now, "exp": now + 3600,
    }).encode())
    signing_input = f"{header}.{claims}".encode("ascii")
    try:
        key = serialization.load_pem_private_key(info["private_key"].encode(), password=None)
    except ValueError:
        raise GA4Unavailable("GA4 key holds no readable private key") from None
    if not isinstance(key, rsa.RSAPrivateKey):
        raise GA4Unavailable("GA4 key is not an RSA key")
    signature = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    return f"{header}.{claims}.{_b64url(signature)}"


async def access_token(client: httpx.AsyncClient, *, now: Optional[float] = None) -> str:
    """A read-only access token, reused until a minute before it expires."""
    moment = time.time() if now is None else now
    if _token.get("value") and moment < float(_token.get("expires_at", 0)) - 60:
        return str(_token["value"])
    info = _load_key()
    try:
        response = await client.post(
            info["token_uri"],
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion(info, int(moment)),
            },
            timeout=TIMEOUT_S,
        )
    except httpx.HTTPError as exc:
        raise GA4Unavailable(f"Google sign-in unreachable: {type(exc).__name__}") from exc
    if response.status_code != 200:
        raise GA4Unavailable(f"Google sign-in answered {response.status_code}")
    body = response.json()
    _token["value"] = body["access_token"]
    _token["expires_at"] = moment + float(body.get("expires_in", 3600))
    return str(body["access_token"])


def funnel_request(start: date, end: date, breakdown: Optional[str] = None) -> Dict[str, Any]:
    """The body of a closed funnel over ``start..end``, both included."""
    body: Dict[str, Any] = {
        "dateRanges": [{"startDate": start.isoformat(), "endDate": end.isoformat()}],
        "funnel": {
            "isOpenFunnel": False,
            "steps": [
                {"name": column, "filterExpression": {"funnelEventFilter": {"eventName": event}}}
                for column, event in STEPS
            ],
        },
    }
    if breakdown:
        body["funnelBreakdown"] = {
            "breakdownDimension": {"name": breakdown}, "limit": BREAKDOWN_LIMIT,
        }
    return body


def _step_column(name: str) -> Optional[str]:
    """GA4 names a step ``"<n>. <name>"``; its number is what is trusted."""
    number = name.split(".", 1)[0].strip()
    if number.isdigit() and 1 <= int(number) <= len(COLUMNS):
        return COLUMNS[int(number) - 1]
    return None


def parse_funnel(payload: Dict[str, Any], breakdown: Optional[str]) -> Dict[str, Dict[str, int]]:
    """``{breakdown value: {column: people}}``; the whole property is ``TOTAL``."""
    table = payload.get("funnelTable") or {}
    dims = [h.get("name") for h in table.get("dimensionHeaders", [])]
    metrics = [h.get("name") for h in table.get("metricHeaders", [])]
    users_at = metrics.index("activeUsers") if "activeUsers" in metrics else 0
    out: Dict[str, Dict[str, int]] = {}
    for row in table.get("rows", []):
        values = {name: v.get("value", "") for name, v in zip(dims, row.get("dimensionValues", []))}
        column = _step_column(str(values.get("funnelStepName", "")))
        if column is None:
            continue
        key = str(values.get(breakdown, TOTAL)) if breakdown else TOTAL
        if key == "RESERVED_TOTAL":
            key = TOTAL
        cells = row.get("metricValues", [])
        people = int(float(cells[users_at].get("value", 0))) if len(cells) > users_at else 0
        out.setdefault(key, {c: 0 for c in COLUMNS})[column] = people
    return out


async def run_funnel(
    client: httpx.AsyncClient,
    token: str,
    start: date,
    end: date,
    breakdown: Optional[str] = None,
) -> Dict[str, Dict[str, int]]:
    """One closed funnel over ``start..end``, parsed by ``parse_funnel``."""
    url = f"{API}/properties/{property_id()}:runFunnelReport"
    try:
        response = await client.post(
            url,
            json=funnel_request(start, end, breakdown),
            headers={"Authorization": f"Bearer {token}"},
            timeout=TIMEOUT_S,
        )
    except httpx.HTTPError as exc:
        raise GA4Unavailable(f"GA4 unreachable: {type(exc).__name__}") from exc
    if response.status_code != 200:
        # GA4's own message says which quota or permission; it names no secret.
        try:
            message = response.json()["error"]["message"]
        except (ValueError, KeyError, TypeError):
            message = ""
        raise GA4Unavailable(f"GA4 answered {response.status_code}: {message}"[:300])
    return parse_funnel(response.json(), breakdown)
