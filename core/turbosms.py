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
CODE_SENT = 801              # SUCCESS_MESSAGE_SENT (already away)
CODE_PARTIAL = 802           # SUCCESS_MESSAGE_PARTIAL_ACCEPTED
CODE_PARTIAL_SENT = 803      # SUCCESS_MESSAGE_PARTIAL_SENT

# The gateway reports success as any of 800-803: queued or already sent,
# whole or partial. Treating one of them as a failure is the worst mistake
# available here — the messages have gone out, but the campaign is left
# unstamped, so the obvious next move is to send the whole roster again.
_ACCEPTED_CODES = frozenset({
    CODE_OK, CODE_ACCEPTED, CODE_SENT, CODE_PARTIAL, CODE_PARTIAL_SENT,
})

# DLR statuses from the webhook, mapped to the three outcomes we act on.
# Anything unknown stays as-is rather than being forced into a bucket.
DELIVERED_STATUSES = frozenset({"DELIVRD", "READ", "Delivered", "Read"})
FAILED_STATUSES = frozenset({
    "UNDELIV", "REJECTD", "EXPIRED",
    "Undelivered", "Rejected", "Expired", "Failed", "Cancelled",
})


# GSM 03.38, the 7-bit alphabet operators bill at 160 characters. Anything
# outside it forces the whole message to UCS-2 at 70 — so a single Cyrillic
# character in an otherwise Latin text more than halves what fits.
_GSM7_BASIC = set(
    "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?"
    "¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"
)
# These cost two GSM-7 characters each: they are sent as an escape plus a code.
_GSM7_EXTENDED = set("^{}\\[~]|€")


# The gateway refuses more than this in one call with 405
# NOT_ALLOWED_RECIPIENTS_LIMIT, so a campaign larger than it has to be split.
RECIPIENTS_PER_REQUEST = 5000


class TurboSmsError(Exception):
    """A TurboSMS call failed outright (transport, auth, or malformed reply)."""


class PartialSendError(TurboSmsError):
    """A batch failed after earlier ones had already gone out.

    Carries the results of the batches that succeeded, because discarding
    them is the one unrecoverable mistake here: those people have the message
    on their phones, and a retry that does not know it would send it twice —
    spending the budget again and destroying the campaign's measurement.
    """

    def __init__(self, results: List["SendResult"], sent: int, unsent: int, cause: Exception):
        super().__init__(
            f"sent to {sent} recipients, then the gateway refused: {cause}. "
            f"{unsent} were not messaged."
        )
        self.results = results
        self.sent = sent
        self.unsent = unsent


@dataclass(frozen=True)
class MessageCost:
    """What one text will actually be billed as."""

    encoding: str      # "gsm7" or "ucs2"
    characters: int    # billable units, not len(text) — GSM-7 escapes count twice
    parts: int

    @property
    def unicode(self) -> bool:
        return self.encoding == "ucs2"


def count_segments(text: str) -> MessageCost:
    """
    Work out the encoding and part count an operator will charge for.

    Worth surfacing because the cost cliff is invisible while writing: a
    Ukrainian message is UCS-2, so it fits 70 characters rather than 160, and
    a two-part send doubles the bill for every recipient in the campaign.
    """
    unicode_needed = any(
        ch not in _GSM7_BASIC and ch not in _GSM7_EXTENDED for ch in text
    )

    if unicode_needed:
        # UCS-2 is billed in 16-bit units, not characters. Anything outside the
        # Basic Multilingual Plane — every pictographic emoji — takes two of
        # them, so len() understates a message that carries one and can hide a
        # whole extra segment.
        chars = len(text.encode("utf-16-le")) // 2
        single, concatenated = 70, 67
        encoding = "ucs2"
    else:
        # Escaped characters occupy two septets each.
        chars = sum(2 if ch in _GSM7_EXTENDED else 1 for ch in text)
        single, concatenated = 160, 153
        encoding = "gsm7"

    if chars == 0:
        parts = 0
    elif chars <= single:
        parts = 1
    else:
        # Concatenation spends part of every segment on the joining header.
        parts = -(-chars // concatenated)

    return MessageCost(encoding=encoding, characters=chars, parts=parts)


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


#: Viber caps the button label; the gateway rejects anything longer.
VIBER_CAPTION_LIMIT = 30
VIBER_TEXT_LIMIT = 1000


@dataclass(frozen=True)
class ViberMessage:
    """
    The Viber half of a hybrid send.

    Worth having as its own type because Viber is not "SMS but nicer": it
    carries a real button, which is the only way a campaign link ever gets
    readable anchor text. An SMS can only ever show the bare URL.
    """

    text: str
    #: Button label. Requires `action` — a button with no destination is a
    #: dead control, and the gateway will not render one without a URL.
    caption: Optional[str] = None
    #: Where the button goes.
    action: Optional[str] = None
    image_url: Optional[str] = None
    #: Seconds the message stays deliverable before falling back to SMS.
    ttl: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.text.strip():
            raise ValueError("viber text is empty")
        if len(self.text) > VIBER_TEXT_LIMIT:
            raise ValueError(f"viber text exceeds {VIBER_TEXT_LIMIT} characters")
        if bool(self.caption) != bool(self.action):
            raise ValueError("viber button needs both a caption and an action URL")
        if self.caption and len(self.caption) > VIBER_CAPTION_LIMIT:
            raise ValueError(
                f"viber button caption exceeds {VIBER_CAPTION_LIMIT} characters"
            )
        if self.ttl is not None and not 60 <= self.ttl <= 86400:
            raise ValueError("viber ttl must be between 60 and 86400 seconds")

    def payload(self, sender: str) -> Dict[str, Any]:
        body: Dict[str, Any] = {"sender": sender, "text": self.text}
        if self.caption and self.action:
            body["caption"] = self.caption
            body["action"] = self.action
        if self.image_url:
            body["image_url"] = self.image_url
        if self.ttl is not None:
            body["ttl"] = self.ttl
        return body


@dataclass
class TurboSmsConfig:
    """Credentials and defaults, read from the environment."""

    # TURBOSMS_API_TOKEN is the name already in use in this project's .env;
    # TURBOSMS_TOKEN is accepted as an alias so either spelling works.
    token: str = field(
        default_factory=lambda: os.getenv("TURBOSMS_API_TOKEN")
        or os.getenv("TURBOSMS_TOKEN", "")
    )
    sender: str = field(default_factory=lambda: os.getenv("TURBOSMS_SENDER", ""))
    # Viber sender names are registered separately from SMS alpha names, so a
    # working SMS setup says nothing about whether Viber can be used.
    viber_sender: str = field(
        default_factory=lambda: os.getenv("TURBOSMS_VIBER_SENDER", "")
    )
    # Shared secret configured alongside the callback URL in the TurboSMS
    # panel; the webhook signature is SHA1(secret + event id).
    webhook_secret: str = field(
        default_factory=lambda: os.getenv("TURBOSMS_WEBHOOK_SECRET", "")
    )
    # What one billable segment costs, in UAH. The gateway does not report a
    # price with the send, so the tariff has to come from somewhere; this is
    # the contract rate, and it is copied onto each campaign when that campaign
    # is sent so a later rate change cannot restate an old result.
    price_per_part: float = field(
        default_factory=lambda: float(os.getenv("TURBOSMS_PRICE_PER_PART", "1.28"))
    )
    timeout: float = 20.0

    @property
    def configured(self) -> bool:
        return bool(self.token and self.sender)

    @property
    def viber_configured(self) -> bool:
        return bool(self.token and self.viber_sender)


def match_webhook_signature(
    event_id: str, signature: str, secret: str,
) -> "str | None":
    """
    Name the SHA1 concatenation that produced this signature, or None.

    The gateway's docs say only "SHA1 hash of a string consisting of the secret
    security key and id" and give no worked example, so the ORDER is undefined
    in writing. This code guessed `secret + id`, and the 2026-08-05 campaign had
    every one of its 3 655 callbacks rejected with 401 against a secret the
    owner has since confirmed matches the panel — which leaves the order as the
    live suspect.

    Both orders are accepted because both require knowing the secret: an
    attacker who can produce either has already lost us nothing extra. The
    matched name is returned so the logs record which one the gateway actually
    uses, and this ambiguity can be closed by evidence instead of guessing.
    """
    if not secret or not signature or not event_id:
        return None
    received = signature.lower().strip()
    candidates = {
        "sha1(secret+id)": f"{secret}{event_id}",
        "sha1(id+secret)": f"{event_id}{secret}",
    }
    for name, material in candidates.items():
        expected = hashlib.sha1(material.encode()).hexdigest()
        # Constant-time compare — the signature is attacker-supplied.
        if hmac.compare_digest(expected, received):
            return name
    return None


def verify_webhook_signature(event_id: str, signature: str, secret: str) -> bool:
    """Whether a DLR callback carries a signature made with our secret."""
    return match_webhook_signature(event_id, signature, secret) is not None


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

    async def send(
        self,
        phones: List[str],
        text: str,
        viber: Optional[ViberMessage] = None,
    ) -> List[SendResult]:
        """
        Send one message to many recipients, over Viber then SMS if asked.

        Passing `viber` makes it a hybrid send: the gateway tries Viber first
        and falls back to SMS only for recipients it could not reach. That is
        cheaper per delivery and carries a real button, so the SMS text should
        be written to stand on its own — the fallback has no button to press.

        Returns one result per recipient either way; the gateway does not say
        which channel it used at send time, only later via `statuses`.

        A stoplisted recipient comes back with message_id=None and code 404 —
        that is a signal to record, not an error to swallow.
        """
        if not phones:
            return []

        viber_payload = None
        if viber is not None:
            if not self.config.viber_configured:
                raise TurboSmsError(
                    "Viber is not configured — set TURBOSMS_VIBER_SENDER"
                )
            viber_payload = viber.payload(self.config.viber_sender)

        results: List[SendResult] = []
        for start in range(0, len(phones), RECIPIENTS_PER_REQUEST):
            batch = phones[start:start + RECIPIENTS_PER_REQUEST]
            payload: Dict[str, Any] = {
                "recipients": batch,
                "sms": {"sender": self.config.sender, "text": text},
            }
            if viber_payload is not None:
                payload["viber"] = viber_payload

            try:
                body = await self._post("/message/send.json", payload)
            except TurboSmsError as e:
                # Anything already away must survive the failure, or the retry
                # messages those people a second time.
                if results:
                    raise PartialSendError(
                        results, sent=start, unsent=len(phones) - start, cause=e,
                    ) from e
                raise

            results.extend(
                SendResult(
                    phone=str(item.get("phone", "")),
                    message_id=item.get("message_id"),
                    code=int(item.get("response_code", -1)),
                    status=str(item.get("response_status", "")),
                )
                for item in body.get("response_result") or []
            )

        stoplisted = sum(1 for r in results if r.stoplisted)
        logger.info(
            "TurboSMS send: %d requested in %d batch(es), %d accepted, "
            "%d stoplisted, channel=%s",
            len(phones), -(-len(phones) // RECIPIENTS_PER_REQUEST),
            sum(1 for r in results if r.accepted), stoplisted,
            "viber+sms" if viber is not None else "sms",
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
