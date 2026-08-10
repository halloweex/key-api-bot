#!/usr/bin/env python3
"""Settle whether TURBOSMS_WEBHOOK_SECRET matches what the gateway signs with.

The webhook rejected 3 655 callbacks with 401 across the 2026-08-05 campaign and
nobody could tell why: a wrong shared secret and a payload in an unexpected
shape produce the same rejection and need opposite fixes. This script answers it
from one real (id, signature) pair.

Getting a pair
--------------
1. Set ``TURBOSMS_WEBHOOK_DEBUG=1`` on the web container and restart it.
2. Fire a test callback from the TurboSMS panel (or send one real SMS).
3. ``docker logs keycrm-web | grep "webhook debug payload"`` — it prints the
   whole rejected payload, ``id`` and ``signature`` included.
4. Run this script with them, then **turn the debug flag back off**: the
   signature is derived from the shared secret.

Usage
-----
    PYTHONPATH=. python scripts/check_turbosms_signature.py \
        --id 0e2f1a6c-... --signature 3f5b...

    # try a specific candidate instead of the configured one
    PYTHONPATH=. python scripts/check_turbosms_signature.py \
        --id ... --signature ... --secret 'candidate'

Exit code is 0 when something matched, 1 when nothing did.
"""
import argparse
import hashlib
import os
import sys


# The docs say only "SHA1 hash of a string consisting of the secret security
# key and id" and give no worked example, so the order is undefined in writing.
# Anything below that is not SHA1 is here to rule a scheme out, not because it
# is expected: if one of them matches, the handler is checking the wrong thing.
def schemes(secret: str, event_id: str) -> "list[tuple[str, str]]":
    import hmac
    return [
        ("sha1(secret+id)   [what the code checks]",
         hashlib.sha1(f"{secret}{event_id}".encode()).hexdigest()),
        ("sha1(id+secret)   [the other reading]",
         hashlib.sha1(f"{event_id}{secret}".encode()).hexdigest()),
        ("md5(secret+id)",
         hashlib.md5(f"{secret}{event_id}".encode()).hexdigest()),
        ("md5(id+secret)",
         hashlib.md5(f"{event_id}{secret}".encode()).hexdigest()),
        ("sha256(secret+id)",
         hashlib.sha256(f"{secret}{event_id}".encode()).hexdigest()),
        ("hmac-sha1(key=secret, msg=id)",
         hmac.new(secret.encode(), event_id.encode(), hashlib.sha1).hexdigest()),
        ("hmac-sha256(key=secret, msg=id)",
         hmac.new(secret.encode(), event_id.encode(), hashlib.sha256).hexdigest()),
    ]


def sha1_hex(secret: str, event_id: str) -> str:
    """The reading this code started with: SHA1(secret + id)."""
    return hashlib.sha1(f"{secret}{event_id}".encode()).hexdigest()


def _candidates(explicit: str | None) -> list[tuple[str, str]]:
    """Named secrets worth testing, most likely first.

    The near-miss variants exist because a secret pasted through a shell or a
    web form is the usual way this breaks: a trailing newline or a copied
    surrounding quote produces a wrong hash and an otherwise perfect setup.
    """
    if explicit is not None:
        return [("--secret", explicit)]

    named: list[tuple[str, str]] = []
    configured = os.getenv("TURBOSMS_WEBHOOK_SECRET", "")
    if configured:
        named.append(("TURBOSMS_WEBHOOK_SECRET", configured))
        stripped = configured.strip().strip('"').strip("'")
        if stripped != configured:
            named.append(("TURBOSMS_WEBHOOK_SECRET (trimmed)", stripped))
    # A plausible mix-up: the API token and the callback secret are different
    # values in the panel, and only one of them signs callbacks.
    token = os.getenv("TURBOSMS_API_TOKEN") or os.getenv("TURBOSMS_TOKEN", "")
    if token:
        named.append(("TURBOSMS_API_TOKEN (wrong value, tested to rule it out)", token))
    return named


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check a TurboSMS callback signature against candidate secrets.",
    )
    parser.add_argument("--id", required=True, help="the callback's top-level id")
    parser.add_argument("--signature", required=True, help="its signature field")
    parser.add_argument(
        "--secret",
        help="test this secret instead of the ones in the environment",
    )
    args = parser.parse_args()

    received = args.signature.lower().strip()
    candidates = _candidates(args.secret)

    print(f"event id  : {args.id}")
    print(f"signature : {received}  ({len(received)} chars)")
    if len(received) != 40 or not all(c in "0123456789abcdef" for c in received):
        print(
            "\n⚠️  That is not 40 hex characters, so it is not the documented\n"
            "    SHA1(secret + id). The gateway is signing with a different\n"
            "    scheme than this code implements — a new secret will not help."
        )
    print()

    if not candidates:
        print("No secret to test: TURBOSMS_WEBHOOK_SECRET is unset and --secret "
              "was not given.")
        return 1

    matched = None
    for name, secret in candidates:
        print(f"  secret: {name} (len {len(secret)})")
        for scheme_name, expected in schemes(secret, args.id):
            ok = expected == received
            if ok:
                matched = (name, scheme_name)
            mark = "MATCH " if ok else "      "
            print(f"    {mark} {scheme_name:42} {expected}")
        print()

    if matched:
        secret_name, scheme_name = matched
        print(f"✅ Matched: {secret_name} with {scheme_name}")
        if scheme_name.startswith("sha1(secret+id)") or scheme_name.startswith("sha1(id+secret)"):
            print("   The handler accepts both SHA1 orders, so this callback would\n"
                  "   be accepted. If they are still being rejected, the running\n"
                  "   container is on an older image — redeploy.")
        else:
            print("   That is NOT one of the two orders the handler checks. The\n"
                  "   handler needs to implement this scheme.")
        return 0

    print("❌ Nothing matched, under any secret or scheme tried. Either the\n"
          "   secret differs from the panel's security key, or the gateway\n"
          "   signs over something other than the id alone.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
