"""A deploy must not break the tabs that are already open.

WHAT HAPPENED

2026-09-06, three deploys inside two hours. Every build renames each chunk by
the hash of its content, so `ReportsPage-DRz6VXil.js` became
`ReportsPage-CzCOmilC.js`. A tab still holding the pre-deploy `index.html` was
still holding the old chunk map; the first navigation to a page it had not
already loaded asked for a file that no longer existed, the dynamic import
rejected, and the error boundary painted "Something went wrong" across a
healthy dashboard. One 404 in the server log, forty-six 200s around it.

TWO CAUSES, AND NEITHER FIX WORKS ALONE

The HTML shell carried **no `Cache-Control` at all**, so browsers applied
heuristic freshness and reused a stale document — while `/static/`, whose
filenames are *not* hashed, was served `immutable` for a year. Exactly
backwards. And nothing in the frontend recovered from a failed import, so the
one 404 became a full-page error rather than a reload.

Fixing only the headers leaves every tab open at deploy time to fail once.
Fixing only the frontend gives it a reload that fetches the same stale shell.
Hence both, and hence one test file for both.

WHY THESE ARE ASSERTED STRUCTURALLY

The nginx half cannot be exercised by the suite — there is no nginx here — so
what is checked is the *configuration text*, parsed rather than grepped. The
frontend half has real unit tests in
`web/frontend/src/utils/__tests__/lazyChunk.test.ts`; what is checked here is
the thing those tests cannot see, namely that every `lazy()` in the app
actually goes through the wrapper. A page nobody wrapped is precisely the page
somebody opens after the next deploy.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
NGINX = REPO / "nginx" / "nginx.conf"
FRONTEND = REPO / "web" / "frontend" / "src"
HELPER = FRONTEND / "utils" / "lazyChunk.ts"


# ── the frontend half ────────────────────────────────────────────────────────


def _sources() -> list[Path]:
    return [
        p for p in FRONTEND.rglob("*.ts*")
        if p != HELPER
        and "__tests__" not in p.parts
        and not p.name.endswith(".stories.tsx")
        and not p.name.endswith(".d.ts")
    ]


class TestEveryChunkIsWrapped:
    def test_the_scan_finds_the_lazy_ones_at_all(self):
        """A guard on the guard: a scan matching nothing passes vacuously."""
        wrapped = sum(
            len(re.findall(r"\blazyChunk\(", p.read_text(encoding="utf-8")))
            for p in _sources()
        )
        assert wrapped >= 20, f"only {wrapped} wrapped imports found — scan broken"

    def test_no_bare_react_lazy_survives(self):
        offenders = []
        for path in _sources():
            for lineno, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), 1,
            ):
                code = line.split("//")[0]
                # `lazyChunk(` also ends in `lazy(`-ish text, so match the call
                # on a word boundary and let `lazyChunk` fail the boundary.
                if re.search(r"(?<![A-Za-z0-9_])lazy\(", code):
                    offenders.append(f"{path.relative_to(REPO)}:{lineno}")
        assert not offenders, (
            f"React.lazy used directly at {offenders} — a chunk that goes "
            f"missing after a deploy will show the error boundary instead of "
            f"reloading. Use `lazyChunk` from src/utils/lazyChunk.ts."
        )

    def test_the_helper_cannot_loop(self):
        """The one way this fix could be worse than the defect."""
        body = HELPER.read_text(encoding="utf-8")
        assert "sessionStorage" in body, "no once-per-tab guard"
        assert "isMissingChunk" in body, "no narrowing on the failure kind"
        # A reload must be reachable only behind both conditions.
        guard = re.search(r"if \(isMissingChunk\(error\) && !readFlag\(\)\)", body)
        assert guard, "the reload is no longer behind both guards"


# ── the nginx half ───────────────────────────────────────────────────────────


def _location_block(path: str) -> str:
    """The body of `location <path> { … }`, braces balanced."""
    assert NGINX.exists(), (
        f"{NGINX} is not here. The runtime image does not carry `nginx/`, so "
        f"`deploy/gate_with_stores.sh` has to mount it — it does, and if this "
        f"fires the mount was dropped or a new runner needs it added."
    )
    conf = NGINX.read_text(encoding="utf-8")
    match = re.search(rf"location\s+{re.escape(path)}\s*\{{", conf)
    assert match, f"no `location {path}` in nginx.conf"
    start = match.end() - 1
    depth = 0
    for i in range(start, len(conf)):
        if conf[i] == "{":
            depth += 1
        elif conf[i] == "}":
            depth -= 1
            if depth == 0:
                return conf[start:i + 1]
    raise AssertionError(f"`location {path}` is not brace-balanced")


def _cache_control(block: str) -> str | None:
    match = re.search(r'add_header\s+Cache-Control\s+"([^"]*)"', block)
    return match.group(1) if match else None


class TestTheHtmlShellIsNotCached:
    def test_the_catch_all_says_no_cache(self):
        """`location /` serves `index.html` and every SPA route. It is the
        document that names the chunks, so a cached copy names dead ones."""
        value = _cache_control(_location_block("/"))
        assert value is not None, (
            "`location /` sends no Cache-Control — browsers then guess, and a "
            "guessed-fresh index.html is the stale chunk map that started this"
        )
        assert "no-cache" in value, value

    def test_the_hashed_assets_are_immutable(self):
        """The mirror image: these filenames contain a content hash, so they
        never change meaning and re-validating them every load is pure cost."""
        value = _cache_control(_location_block("/static-v2/assets/"))
        assert value is not None, "the hashed assets still send no Cache-Control"
        assert "immutable" in value and "max-age=" in value, value

    @pytest.mark.parametrize("path", ("/", "/static-v2/assets/"))
    def test_security_headers_are_re_included(self, path):
        """`add_header` in a location *replaces* the inherited set, which the
        config already documents for `/static/`. A cache header that silently
        drops the CSP would be a poor trade."""
        assert "security-headers.conf" in _location_block(path), (
            f"`location {path}` declares add_header without re-including the "
            f"server-level security headers"
        )
