"""TurboSMS faked at its transport, for the SMS send tests.

The routes get the real `TurboSmsClient` over an `httpx.MockTransport`, so the
balance is parsed, the roster batched and every gateway failure classified the
way production does it, and nothing here can reach the live gateway. What the
gateway was asked is the behaviour at this boundary: `calls` names every
request in order, `sent` holds each send's recipients.

    gw = install_gateway(monkeypatch)
    gw.send_failures = [None, 405]     # first batch answered, second refused
"""
from __future__ import annotations

import json

import httpx

from core.turbosms import CODE_STOPLIST, TurboSmsClient, TurboSmsConfig

BALANCE = "user/balance.json"
SEND = "message/send.json"


class Gateway:
    """A TurboSMS account: a balance, and an answer to every send."""

    def __init__(self):
        self.balance: object = 1_000_000.0
        self.balance_status = 200
        self.calls: list = []
        # One entry per send request, the recipients it carried.
        self.sent: list = []
        # Phones the gateway refuses as opted out at the provider.
        self.stoplisted: set = set()
        # How each send request fails, in order: None answers it, an int is
        # that HTTP status, "timeout" is no answer within the client's read
        # timeout. A send past the end of the list is answered.
        self.send_failures: list = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.strip("/")
        self.calls.append(path)
        if path == BALANCE:
            if self.balance_status != 200:
                return httpx.Response(self.balance_status, text="upstream error")
            result = {} if self.balance is None else {"balance": self.balance}
            return httpx.Response(200, json={
                "response_code": 0, "response_status": "OK", "response_result": result,
            })
        if path == SEND:
            recipients = json.loads(request.content)["recipients"]
            self.sent.append(list(recipients))
            failure = self.send_failures.pop(0) if self.send_failures else None
            if failure == "timeout":
                raise httpx.ReadTimeout("no answer within the read timeout",
                                        request=request)
            if failure is not None:
                return httpx.Response(failure, text="NOT_ALLOWED_RECIPIENTS_LIMIT")
            return httpx.Response(200, json={
                "response_code": 0, "response_status": "OK",
                "response_result": [self._answer(p) for p in recipients],
            })
        return httpx.Response(404, json={"response_code": 404})

    def _answer(self, phone: str) -> dict:
        if phone in self.stoplisted:
            return {"phone": phone, "response_code": CODE_STOPLIST, "message_id": None,
                    "response_status": "NOT_ALLOWED_NUMBER_STOPLIST"}
        return {"phone": phone, "response_code": 0, "message_id": f"m-{phone}",
                "response_status": "OK"}

    @property
    def sends(self) -> int:
        return self.calls.count(SEND)


def install_gateway(monkeypatch) -> Gateway:
    """Point the SMS routes at a fresh `Gateway`, configured for SMS and Viber
    at 1.28 ₴ a part, and return it."""
    gw = Gateway()
    config = TurboSmsConfig(token="test-token-not-real", sender="KoreanStory",
                            viber_sender="KoreanStory", price_per_part=1.28)

    def factory(*_a, **_kw):
        return TurboSmsClient(config, transport=httpx.MockTransport(gw.handler))

    monkeypatch.setattr("web.routes.api.customers.TurboSmsClient", factory)
    monkeypatch.setattr("web.routes.api.customers.TurboSmsConfig", lambda: config)
    return gw
