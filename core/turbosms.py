"""
TurboSMS gateway client.

Sending through the API rather than handing a file to the provider is what
makes a campaign measurable. A manual upload returns nothing: no per-recipient
message id, so no delivery status, and no way to learn who is on the provider's
stoplist. Both of those distort the measured effect —

* an undelivered number sits in the roster as "messaged", never buys, and drags
  the measured lift down;
* a stoplisted number does the same, and would be re-exported in every future
  campaign, because the segmentation only looks at purchases.

The send response carries both facts, so this client surfaces them as first
class results instead of a bare success flag.

API: https://turbosms.ua/en/api.html
DLR webhooks: https://turbosms.ua/en/callback.html
"""
from __future__ import annotations

import hashlib
import hmac
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx

from core.observability import get_logger

logger = get_logger(__name__)

BASE_URL = "https://api.turbosms.ua"

# Response codes that matter to us. The full list is larger; everything not
# named here is treated as a plain failure.
CODE_OK = 0
CODE_STOPLIST = 404          # NOT_ALLOWED_NUMBER_STOPLIST
CODE_ACCEPTED = 800          # SUCCESS_MESSAGE_ACCEPTED (queued)
CODE_PARTIAL = 802           # SUCCESS_MESSAGE_PARTIAL_ACCEPTED

_ACCEPTED_CODES = frozenset({CODE_OK, CODE_ACCEPTED, CODE_PARTIAL})

# DLR statuses from the webhook, mapped to the three outcomes we act on.
# Anything unknown stays as-is rather than being forced into a bucket.
DELIVERED_STATUSES = frozenset({"DELIVRD", "READ", "Delivered", "Read"})
FAILED_STATUSES = frozenset({
    "UNDELIV", "REJECTD", "EXPIRED",
    "Undelivered", "Rejected", "Expired", "Failed", "Cancelled",
})


class TurboSmsError(Exception):
    """A TurboSMS call failed outright (transport, auth, or malformed reply)."""


@dataclass(frozen=True)
class SendResult:
    """Per-recipient outcome of a send."""

    phone: str
    message_id: Optional[str]
    code: int
    status: str

    @property
    def accepted(self) -> bool:
        """The gateway took the message; a message_id exists to track it."""
        return self.code in _ACCEPTED_CODES and bool(self.message_id)

    @property
    def stoplisted(self) -> bool:
        """The recipient has opted out at the provider — do not message again."""
        return self.code == CODE_STOPLIST


@dataclass
class TurboSmsConfig:
    """Credentials and defaults, read from the environment."""

    token: str = field(default_factory=lambda: os.getenv("TURBOSMS_TOKEN", ""))
    sender: str = field(default_factory=lambda: os.getenv("TURBOSMS_SENDER", ""))
    # Shared secret configured alongside the callback URL in the TurboSMS
    # panel; the webhook signature is SHA1(secret + event id).
    webhook_secret: str = field(
        default_factory=lambda: os.getenv("TURBOSMS_WEBHOOK_SECRET", "")
    )
    timeout: float = 20.0

    @property
    def configured(self) -> bool:
        return bool(self.token and self.sender)


def verify_webhook_signature(event_id: str, signature: str, secret: str) -> bool:
    """
    Check a DLR callback's SHA1(secret + id) signature.

    Without this the endpoint is an open write into campaign results: anyone
    could post fabricated deliveries and move the measured lift.
    """
    if not secret or not signature or not event_id:
        return False
    expected = hashlib.sha1(f"{secret}{event_id}".encode()).hexdigest()
    # Constant-time compare — the signature is attacker-supplied.
    return hmac.compare_digest(expected, signature.lower().strip())


def classify_dlr(status: str) -> Optional[bool]:
    """
    Map a DLR status to delivered (True) / failed (False) / still open (None).

    'Sent', 'In Queue' and 'In Process' mean the operator has not reported yet,
    so they stay open rather than counting as a failure.
    """
    if status in DELIVERED_STATUSES:
        return True
    if status in FAILED_STATUSES:
        return False
    return None


class TurboSmsClient:
    """Async TurboSMS client. One instance per process; reuses its connection."""

    def __init__(
        self,
        config: Optional[TurboSmsConfig] = None,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        self.config = config or TurboSmsConfig()
        self._transport = transport
        self._client: Optional[httpx.AsyncClient] = None
        self._owns_client = False

    async def __aenter__(self) -> "TurboSmsClient":
        # Only build a connection if one was not supplied. Overwriting an
        # injected transport here would silently send tests at the live gateway.
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=BASE_URL,
                timeout=self.config.timeout,
                headers={"Authorization": f"Bearer {self.config.token}"},
                transport=self._transport,
            )
            self._owns_client = True
        return self

    async def __aexit__(self, *exc) -> None:
        if self._client and self._owns_client:
            await self._client.aclose()
            self._client = None
            self._owns_client = False

    async def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.config.configured:
            raise TurboSmsError(
                "TurboSMS is not configured — set TURBOSMS_TOKEN and TURBOSMS_SENDER"
            )
        if self._client is None:
            raise TurboSmsError("client used outside its context manager")

        try:
            response = await self._client.post(path, json=payload)
        except httpx.RequestError as e:
            raise TurboSmsError(f"TurboSMS request failed: {e}") from e

        if response.status_code >= 400:
            raise TurboSmsError(
                f"TurboSMS returned HTTP {response.status_code}: {response.text[:200]}"
            )

        try:
            body = response.json()
        except ValueError as e:
            raise TurboSmsError("TurboSMS returned a non-JSON body") from e

        # A non-zero top-level code means the whole request was rejected;
        # per-recipient codes live inside response_result and are not errors.
        code = body.get("response_code")
        if code not in _ACCEPTED_CODES:
            raise TurboSmsError(
                f"TurboSMS rejected the request: {code} {body.get('response_status')}"
            )
        return body

    async def send(self, phones: List[str], text: str) -> List[SendResult]:
        """
        Send one text to many recipients.

        Returns a result per recipient, including the ones the gateway refused.
        A stoplisted recipient comes back with message_id=None and code 404 —
        that is a signal to record, not an error to swallow.
        """
        if not phones:
            return []

        body = await self._post("/message/send.json", {
            "recipients": phones,
            "sms": {"sender": self.config.sender, "text": text},
        })

        results = [
            SendResult(
                phone=str(item.get("phone", "")),
                message_id=item.get("message_id"),
                code=int(item.get("response_code", -1)),
                status=str(item.get("response_status", "")),
            )
            for item in body.get("response_result") or []
        ]

        stoplisted = sum(1 for r in results if r.stoplisted)
        logger.info(
            "TurboSMS send: %d requested, %d accepted, %d stoplisted",
            len(phones), sum(1 for r in results if r.accepted), stoplisted,
        )
        return results

    async def statuses(self, message_ids: List[str]) -> Dict[str, str]:
        """Current delivery status per message id, for ids the gateway knows."""
        if not message_ids:
            return {}

        body = await self._post("/message/status.json", {"messages": message_ids})
        return {
            item["message_id"]: str(item.get("status", ""))
            for item in body.get("response_result") or []
            if item.get("message_id")
        }

    async def balance(self) -> Optional[float]:
        """Account balance, or None if the reply carries no usable figure."""
        body = await self._post("/user/balance.json", {})
        result = body.get("response_result") or {}
        raw = result.get("balance") if isinstance(result, dict) else None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
