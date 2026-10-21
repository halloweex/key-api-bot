"""Regenerate the CPython facts the pilot's access layer reproduces.

Run from the repository root, in the Python production runs and with the
locked dependencies — itsdangerous, starlette, and the interpreter's own
base64, zlib, json, urllib and re answer these:

    docker run --rm -v "$PWD:/app" -w /app <python:3.14-slim + requirements-dev.lock> \
        python rust/crates/ks-api/gen_access_fixtures.py

Writes `tests/fixtures/access_semantics.jsonl` beside this file, deterministic
(fixed seed). Each line is one input and what CPython made of it:

- `b64`    itsdangerous's `base64_decode`: padding added by length, then the
           non-strict `binascii.a2b_base64`, which skips what is not in the
           alphabet and, in 3.14, does not stop at padding;
- `zlib`   `zlib.decompress`, which ignores what follows the stream;
- `json`   `json.loads`: NaN and Infinity, lone surrogates, a repeated key's
           last value in its first place, and nesting until the C stack runs out;
- `cookie` Starlette's Cookie parsing, every header merged into one dict;
- `unquote` uvicorn's path decoding; `url_path` what `request.url.path` says
           of the decoded path, which is what `api_gate` compares;
- `route`  `re.match` of every route pattern in `rust/contract/access.json`,
           Python's `$` — which matches before a final newline — included;
- `itsd`   `URLSafeTimedSerializer.loads` at a fixed instant, on cookies signed
           with the key and on forgeries.

Values are tagged so that nothing is lost in JSON: a str is its code points,
an int its decimal digits, a float its IEEE bits. `cargo test` checks the port
against every line without Python.
"""
from __future__ import annotations

import base64
import json
import random
import struct
import sys
import zlib
from pathlib import Path
from typing import Any, List
from urllib.parse import unquote

HERE = Path(__file__).resolve().parent
CONTRACT = HERE.parents[1] / "contract" / "access.json"

SECRET = b"fixture-secret-not-a-real-key"
NOW = 1_792_000_000                     # 2026-10-15, the instant `loads` runs at
MAX_AGE = 7 * 24 * 60 * 60


def _bits(x: float) -> str:
    return f"{struct.unpack('<Q', struct.pack('<d', x))[0]:016x}"


def tag(v: Any) -> Any:
    """A Python value, losslessly, in JSON."""
    if v is None:
        return ["n"]
    if v is True or v is False:
        return ["b", v]
    if isinstance(v, int):
        return ["i", str(v)]
    if isinstance(v, float):
        return ["f", _bits(v)]
    if isinstance(v, str):
        return ["s", [ord(c) for c in v]]
    if isinstance(v, list):
        return ["l", [tag(x) for x in v]]
    if isinstance(v, dict):
        return ["d", [[[ord(c) for c in k], tag(x)] for k, x in v.items()]]
    raise TypeError(type(v))


def _hex(b: bytes) -> str:
    return b.hex()


# ─── base64 ──────────────────────────────────────────────────────────────────

def _b64_cases(rng: random.Random):
    from itsdangerous.encoding import base64_decode

    alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_+/"
    junk = b"=!. \n\t*$\x00\x7f\x80\xc3\xa9\xff"
    inputs: List[bytes] = [b"", b"=", b"==", b"===", b"====", b"YQ", b"YQ=", b"YQ==", b"YQ==YQ==",
                           b"Y!Q==", b"YQ=x=", b"=YQ==", b"YWJj=", b"YWJjZA=x=", b"Y", b"YW\xffJj",
                           b"-_-_", b"+/+/", b"Zm9v", b"Zm9vYg", b"Zm9vYmE", b"Zm9vYmFy"]
    for _ in range(600):
        raw = bytes(rng.randrange(256) for _ in range(rng.randint(0, 24)))
        text = bytearray(base64.urlsafe_b64encode(raw).rstrip(b"="))
        for _ in range(rng.choice((0, 0, 1, 2, 3, 5))):
            text.insert(rng.randint(0, len(text)), rng.choice(junk))
        if rng.random() < 0.3:
            text += b"=" * rng.randint(1, 3)
        inputs.append(bytes(text))
    for _ in range(400):
        inputs.append(bytes(rng.choice(alphabet + junk) for _ in range(rng.randint(0, 14))))
    for s in inputs:
        try:
            out = _hex(base64_decode(s))
        except Exception:                       # noqa: BLE001 — every failure is one answer
            out = None
        yield {"kind": "b64", "in": _hex(s), "out": out}


# ─── zlib ────────────────────────────────────────────────────────────────────

def _zlib_cases(rng: random.Random):
    bodies = [b"", b"{}", b'{"user_id":5}', bytes(rng.randrange(256) for _ in range(300)),
              json.dumps({"k": "v" * 400}).encode()]
    inputs: List[bytes] = []
    for body in bodies:
        for level in (0, 1, 6, 9):
            inputs.append(zlib.compress(body, level))
        for wbits in (9, 12, 15):
            c = zlib.compressobj(6, zlib.DEFLATED, wbits)
            inputs.append(c.compress(body) + c.flush())
    variants: List[bytes] = []
    for good in inputs:
        variants += [good, good + b"trailing", good[:-1], good[:-4], good[:2], good[2:],
                     bytes([good[0] ^ 0x20]) + good[1:], good[:1] + bytes([good[1] | 0x20]) + good[2:]]
        if len(good) > 6:
            i = rng.randrange(2, len(good) - 4)
            variants.append(good[:i] + bytes([good[i] ^ 0x5A]) + good[i + 1:])
    variants += [b"", b"x", b"x\x9c", b"\x78\x9c\x03\x00\x00\x00\x00\x01", b"not zlib at all"]
    for data in variants:
        try:
            out = _hex(zlib.decompress(data))
        except Exception:                       # noqa: BLE001
            out = None
        yield {"kind": "zlib", "in": _hex(data), "out": out}


# ─── json ────────────────────────────────────────────────────────────────────

_JSON_FIXED = [
    "", " ", "null", "true", "false", "0", "-0", "-0.0", "1", "01", "-", "1.", ".5", "1e5", "1E+5",
    "1e-5", "1E400", "-1E400", "1e-400", "NaN", "-NaN", "Infinity", "-Infinity", "+1", "nan",
    '"a"', '"\\u00e9"', '"\\ud83d\\ude00"', '"\\ud800"', '"\\udc00\\ud800"', '"\\ud800x"',
    '"\\uD83D\\uDE00"', '"\\u12"', '"\\x41"', '"\\/"', '"\\b\\f\\n\\r\\t"', '"a\nb"', '"a\x1fb"',
    '"a\x7fb"', '" "', '"unterminated', "[1,]", "[,1]", "[1 2]", "[]", " [ ] ", "{}", '{"a":1,}',
    '{"a":1,"a":2}', '{"a":1,"b":2,"a":3}', '{"a" 1}', "{'a':1}", '{"a":}', "[1]x", "[1]\x0c",
    "﻿{}", "\t\n\r {}\t\n\r ", "1" * 4300, "1" * 4301, "-" + "1" * 4300, "1.5" + "0" * 5000,
    "[" * 50 + "]" * 50, '{"a":' * 30 + "1" + "}" * 30,
    '{"user_id": 183618567.0}', '{"user_id": true}', '[{"user_id": 1}]', "123456789012345678901234567890",
    "0.1", "1.0", "2.5e-324", "1.7976931348623157e308", "1.7976931348623159e308", "4.9e-324",
    "١", '"\\u0000"', '"\\uFFFF"', '"\\uffff"', '"𝄞"',
]


def _json_texts(rng: random.Random) -> List[str]:
    out = list(_JSON_FIXED)

    def value(depth: int):
        r = rng.random()
        if depth > 3 or r < 0.3:
            return rng.choice([None, True, False, 0, -7, 2 ** 70, 1.5, -0.0, 1e300, "x", "день",
                               "\ud800", "🙂", "", float("nan"), float("inf")])
        if r < 0.65:
            return [value(depth + 1) for _ in range(rng.randint(0, 3))]
        return {rng.choice(["a", "b", "user_id", "é"]): value(depth + 1) for _ in range(rng.randint(0, 3))}

    for _ in range(300):
        text = json.dumps(value(0), ensure_ascii=rng.random() < 0.5,
                          separators=rng.choice([(",", ":"), (", ", ": "), (" ,", " :")]))
        if rng.random() < 0.4 and text:
            i = rng.randrange(len(text))
            text = rng.choice([text[:i] + text[i + 1:], text[:i] + rng.choice('[]{},:"\\ 0e-') + text[i:]])
        out.append(text)
    return out


def _text(s: str):
    """A str as itself where UTF-8 can carry it, else as code points."""
    try:
        s.encode("utf-8")
        return s
    except UnicodeEncodeError:
        return [ord(c) for c in s]


def _json_cases(rng: random.Random):
    for text in _json_texts(rng):
        try:
            out = tag(json.loads(text))
        except (ValueError, RecursionError):
            out = None
        yield {"kind": "json", "in": _text(text), "out": out}
    # Nesting is the C stack's question, not a number's: 20 000 deep loads,
    # 100 000 does not. Spelled by depth so the file stays small.
    for depth in (100, 1000, 20000, 100000):
        try:
            json.loads("[" * depth + "]" * depth)
            ok = True
        except RecursionError:
            ok = False
        yield {"kind": "json_nest", "depth": depth, "ok": ok}


# ─── Starlette's Cookie parsing ──────────────────────────────────────────────

def _cookie_cases(rng: random.Random):
    from starlette.requests import Request

    names = ["dashboard_session", "a", "", " dashboard_session", "DASHBOARD_SESSION", "x=y"]
    values = ["v", "", '"quoted"', '"\\012\\"\\\\x"', '"', '""', '"a', 'a"', "a b", "\xa0v\xa0",
              "\x1cv\x1f", "é", "ÿ", '"\\377"', '"\\400"', "a=b=c", " spaced "]
    seps = [";", "; ", " ;", ";;", "; ;"]
    fixed = [[b""], [b";"], [b"="], [b"dashboard_session"], [b"dashboard_session="],
             [b"a=1; dashboard_session=x"], [b"dashboard_session=1", b"dashboard_session=2"],
             [b"dashboard_session=1; dashboard_session=2"], [b"a=1", b""], [b" = ; = "]]
    cases = list(fixed)
    for _ in range(400):
        headers = []
        for _ in range(rng.choice((1, 1, 1, 2, 3))):
            parts = []
            for _ in range(rng.randint(0, 4)):
                name, value = rng.choice(names), rng.choice(values)
                parts.append(rng.choice([f"{name}={value}", name, f"{name} = {value}", f"={value}"]))
            headers.append(rng.choice(seps).join(parts).encode("latin-1"))
        cases.append(headers)
    for headers in cases:
        scope = {"type": "http", "headers": [(b"cookie", h) for h in headers]}
        cookies = Request(scope).cookies
        yield {"kind": "cookie", "in": [h.decode("latin-1") for h in headers],
               "out": [[k, v] for k, v in cookies.items()]}


# ─── paths ───────────────────────────────────────────────────────────────────

def _unquote_cases(rng: random.Random):
    pieces = ["/api/", "health", "%2F", "%2f", "%68", "%zz", "%4", "%", "%%41", "%C3%A9", "%c3%a9",
              "%C3", "%A9", "%E2%82", "%E2%82%AC", "%ED%A0%80", "%F0%9F%99%82", "%F4%90%80%80",
              "%C0%AF", "%FF", "%0A", "%00", "%3F", "%23", "%20", "+", "~", "revenue", "/trend"]
    for _ in range(500):
        raw = "".join(rng.choice(pieces) for _ in range(rng.randint(1, 6)))
        # What uvicorn does with the request target's path, httptools or h11.
        decoded = unquote(raw) if "%" in raw else raw
        yield {"kind": "unquote", "in": raw, "out": _text(decoded)}


def _url_path_cases(rng: random.Random):
    from starlette.datastructures import URL

    pieces = ["/api/health", "/api/categories", "?", "#", "\n", "\r", "\t", "x", "/", "é", "%",
              "\x00", " ", "//", "?q=1", "\x7f", " "]
    paths = ["/api/health", "/api/health\n", "/api/he\nalth", "/api/health?x", "/api/health#y",
             "/api/\thealth", "\n/api/health", "/api/health\r\n"]
    paths += ["".join(rng.choice(pieces) for _ in range(rng.randint(1, 5))) for _ in range(400)]
    for path in paths:
        scope = {"type": "http", "scheme": "http", "server": ("testserver", 80),
                 "path": path, "query_string": b"", "headers": [(b"host", b"testserver")]}
        yield {"kind": "url_path", "in": _text(path),
               "out": _text(URL(scope=scope).path)}


def _route_cases(rng: random.Random):
    import re

    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    patterns = sorted({r["pattern"] for r in contract["routes"] if "pattern" in r})
    fills = ["1", "x", "день", "a b", "?", "\n", "a\nb", "%2F", "é"]
    for pattern in patterns:
        rx = re.compile(pattern)
        base = re.sub(r"\(\?P<[^>]+>[^)]*\)", "1", pattern[1:-1]).replace("\\", "")
        paths = {base, base + "\n", base + "\n\n", base + "/", base[:-1], "\n" + base,
                 base.replace("/", "/\n", 1), base + "x", base.upper()}
        if "(?P<" in pattern:
            for fill in fills:
                paths.add(re.sub(r"\(\?P<[^>]+>[^)]*\)", fill, pattern[1:-1]).replace("\\", ""))
        for path in sorted(paths):
            m = rx.match(path)
            yield {"kind": "route", "pattern": pattern, "in": _text(path),
                   "out": None if m is None else
                   {k: _text(v) for k, v in m.groupdict().items()}}


# ─── itsdangerous ────────────────────────────────────────────────────────────

def _serializer(ts: int, secret: bytes = SECRET, salt: bytes = b"itsdangerous"):
    from itsdangerous import TimestampSigner, URLSafeTimedSerializer

    class At(TimestampSigner):
        def get_timestamp(self) -> int:
            return ts

    return URLSafeTimedSerializer(secret, salt=salt, signer=At)


def _signed_raw(payload: bytes, ts: int, secret: bytes = SECRET) -> str:
    """A payload the app's serializer would never write, signed with the key."""
    return _serializer(ts, secret).make_signer().sign(payload).decode("latin-1")


def _itsd_inputs(rng: random.Random) -> List[str]:
    from itsdangerous import Signer

    session = {"user_id": 1234567890, "first_name": "Олена", "last_name": "Ковальчук 🌸",
               "username": "olena_k", "photo_url": "https://t.me/i/userpic/320/"
               + "".join(rng.choice("abcdefghij0123456789") for _ in range(40)) + ".jpg",
               "auth_date": NOW - 200, "logged_in_at": NOW - 150, "role": "viewer"}
    objects = [session, {"user_id": 5}, {"user_id": 5, "role": "admin"}, {}, [], "s", 7, None,
               {"user_id": 5.0}, {"user_id": True}, {"user_id": "5"}, {"user_id": None},
               {"user_id": [5]}, {"user_id": {"a": 1}}, {"user_id": 2 ** 70}, {"user_id": -3},
               {"user_id": float("nan")}, {"user_id": float("inf")}, {"user_id": 0},
               {"user_id": 5, "x": float("nan")}, {"user_id": 5, "user_id ": 6}]
    cookies: List[str] = []
    for obj in objects:
        for ts in (NOW - 100, NOW, NOW - MAX_AGE, NOW - MAX_AGE - 1, NOW + 1, NOW + 3600, 0,
                   253402300799, 253402300800, 2 ** 40, 2 ** 63 - 1, 2 ** 64 - 1):
            cookies.append(_serializer(ts).dumps(obj))
    cookies.append(_serializer(NOW - 100).dumps({"user_id": 5, "pad": "x" * 2000}))
    cookies.append(_serializer(NOW - 100, secret=b"another-secret").dumps({"user_id": 5}))
    cookies.append(_serializer(NOW - 100, salt=b"cookie-session").dumps({"user_id": 5}))
    for raw in (b"!!!", b"e30", b".eJzLSM3JyQcABiwCFQ", b".bm90IHpsaWI", b"WzEsMl0", b"bnVsbA",
                b"", b".", b"..", b"eyJ1c2VyX2lkIjo1fQ.", b"W1tbW1s", b"IlxyIg"):
        cookies.append(_signed_raw(raw, NOW - 100))
    # The serializer writes `ensure_ascii=False`, so it cannot write a lone
    # surrogate at all; JSON can still spell one, and `json.loads` keeps it.
    for text in (b'{"user_id":5,"first_name":"\\ud800"}', b'{"user_id":5,"x":Infinity}',
                 '{"user_id":5,"first_name":"Олена"}'.encode(), b'{"user_id":5}\xff'):
        cookies.append(_signed_raw(base64.urlsafe_b64encode(text).rstrip(b"="), NOW - 100))
    deep = base64.urlsafe_b64encode(zlib.compress(b"[" * 20000 + b"]" * 20000)).rstrip(b"=")
    deeper = base64.urlsafe_b64encode(zlib.compress(b"[" * 100000 + b"]" * 100000)).rstrip(b"=")
    cookies += [_signed_raw(b"." + deep, NOW - 100), _signed_raw(b"." + deeper, NOW - 100)]
    # Signed by the plain signer: over a nine-byte timestamp, and over none.
    signer = Signer(SECRET, salt=b"itsdangerous")
    nine = base64.urlsafe_b64encode((2 ** 64).to_bytes(9, "big")).rstrip(b"=")
    cookies.append(signer.sign(b"eyJ1c2VyX2lkIjo1fQ." + nine).decode())
    cookies.append(signer.sign(b"eyJ1c2VyX2lkIjo1fQ").decode())

    junk = "!*. =\n€\x80ÿ\"\\"
    base_cookies = list(cookies[:60])
    for _ in range(900):
        c = list(rng.choice(base_cookies))
        op = rng.randrange(7)
        i = rng.randrange(len(c) + 1)
        if op == 0 and c:
            c[rng.randrange(len(c))] = rng.choice("ABCabc019-_=.")
        elif op == 1:
            for _ in range(rng.randint(1, 4)):
                c.insert(rng.randint(0, len(c)), rng.choice(junk))
        elif op == 2 and c:
            del c[rng.randrange(len(c))]
        elif op == 3:
            c = c[:i]
        elif op == 4:
            c = ["."] + c
        elif op == 5:
            parts = "".join(c).split(".")
            rng.shuffle(parts)
            c = list(".".join(parts))
        else:
            c = list("".join(c) + rng.choice(["=", "==", "!!", "\n", "x"]))
        cookies.append("".join(c))
    return cookies


def _itsd_cases(rng: random.Random):
    import itsdangerous.timed as timed
    from itsdangerous import BadPayload, BadSignature, URLSafeTimedSerializer

    serializer = URLSafeTimedSerializer(SECRET)

    class Clock:
        @staticmethod
        def time() -> float:
            return float(NOW)

    real = timed.time
    timed.time = Clock
    try:
        for cookie in _itsd_inputs(rng):
            try:
                value = serializer.loads(cookie, max_age=MAX_AGE)
                depth = 0
                while isinstance(value, list) and len(value) == 1 and depth < 100:
                    value, depth = value[0], depth + 1
                out = ({"ok_nest": True} if depth == 100 else
                       {"ok": tag(serializer.loads(cookie, max_age=MAX_AGE))})
            except BadSignature:
                out = {"err": "refused"}
            except BadPayload:
                out = {"err": "payload"}
            except Exception:                   # noqa: BLE001 — `_resolve_session` catches none
                out = {"err": "raised"}
            yield {"kind": "itsd", "in": cookie, **out}
    finally:
        timed.time = real


def main() -> None:
    rng = random.Random(20261021)
    records = [{"kind": "made_by", "python": sys.version.split()[0], "now": NOW,
                "max_age": MAX_AGE, "secret": SECRET.decode()}]
    for gen in (_b64_cases(rng), _zlib_cases(rng), _json_cases(rng), _cookie_cases(rng),
                _unquote_cases(rng), _url_path_cases(rng), _route_cases(rng), _itsd_cases(rng)):
        records.extend(gen)
    out = HERE / "tests" / "fixtures" / "access_semantics.jsonl"
    out.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records),
                   encoding="utf-8")
    kinds = {}
    for r in records:
        kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    print(f"{len(records)} records, CPython {sys.version.split()[0]}: {kinds}")


if __name__ == "__main__":
    main()
