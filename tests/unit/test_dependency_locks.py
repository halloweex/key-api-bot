"""What production runs must be a decision, not a resolution.

Nothing here was pinned above a floor, and transitive packages were pinned
nowhere at all — `starlette` arrives through `fastapi` and appeared in no
requirements file. Every deploy rebuilds the images with `pip install -r
requirements.txt`, so the version set production ran was chosen by whatever pip
resolved that minute.

It crossed a **major** boundary that way, starlette 0.50 → 1.6, and the first
anyone knew was CI failing on tests that reach into starlette's route tree.
That time the damage was confined to test introspection.

Two generated files close it: `requirements.lock` (what the images install) and
`requirements-dev.lock` (what CI installs). These tests pin the four properties
that make them worth having.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _pins(path: Path) -> dict[str, str]:
    out = {}
    for line in (REPO / path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, _, version = line.partition("==")
        out[name.lower().replace("_", "-")] = version
    return out


def _named(path: Path) -> set[str]:
    """Package names in an intent file, without their version specifiers."""
    names = set()
    for line in (REPO / path).read_text().splitlines():
        line = line.split("#")[0].strip()
        if not line or line.startswith("-"):
            continue
        name = re.split(r"[<>=~!\[]", line, maxsplit=1)[0].strip()
        if name:
            names.add(name.lower().replace("_", "-"))
    return names


RUNTIME = _pins(Path("requirements.lock"))
DEV = _pins(Path("requirements-dev.lock"))


class TestTheLocksArePins:
    def test_every_runtime_line_is_an_exact_version(self):
        """A floor in a lock file is not a lock."""
        assert RUNTIME
        assert all(re.fullmatch(r"[\w.+!-]+", v) for v in RUNTIME.values())

    def test_it_covers_everything_the_intent_file_asks_for(self):
        """The lock may hold more than `requirements.txt` names — that is the
        whole point, transitives — but never less."""
        missing = _named(Path("requirements.txt")) - set(RUNTIME)
        assert not missing, f"named in requirements.txt, absent from the lock: {missing}"

    def test_it_pins_the_transitive_package_that_started_this(self):
        """`starlette` is named in no requirements file and decides how the
        application's routes are assembled."""
        assert "starlette" in RUNTIME
        assert "fastapi" in RUNTIME


class TestCITestsWhatProductionRuns:
    def test_the_dev_lock_is_a_superset(self):
        missing = set(RUNTIME) - set(DEV)
        assert not missing, f"CI would not install: {missing}"

    def test_it_moves_no_runtime_version(self):
        """The property that makes a green check mean something. If installing
        pytest were allowed to nudge pydantic, CI would be exercising a set the
        deploy never builds."""
        moved = {
            name: (RUNTIME[name], DEV[name])
            for name in RUNTIME
            if DEV.get(name) != RUNTIME[name]
        }
        assert not moved, f"dev install moved runtime packages: {moved}"

    def test_it_adds_only_test_tooling(self):
        extra = set(DEV) - set(RUNTIME)
        assert "pytest" in extra
        assert "alembic" in extra
        # Nothing the runtime images must not carry has leaked the other way.
        assert "pytest" not in RUNTIME
        assert "alembic" not in RUNTIME


class TestNothingBuildsFromTheIntentFile:
    def test_both_images_install_the_lock(self):
        """Building from `requirements.txt` is what let pip choose."""
        for name in ("Dockerfile", "Dockerfile.web"):
            text = (REPO / name).read_text()
            assert "-r requirements.lock" in text
            assert "pip install --no-cache-dir -r requirements.txt" not in text

    def test_ci_installs_the_dev_lock(self):
        text = (REPO / ".github" / "workflows" / "ci.yml").read_text()
        assert "-r requirements-dev.lock" in text
        assert "pip install -r requirements-dev.txt" not in text


class TestTheBaseImagesArePinnedToo:
    """One layer up from the lock, and the same failure.

    `python:3.14-slim` is rebuilt on every CPython patch and every base-OS
    update, so building the same commit twice could produce two different
    runtimes. `requirements.lock` stopped pip deciding what ships; this stops
    Docker Hub deciding.
    """

    def _from_lines(self, name: str) -> list[str]:
        return [
            line for line in (REPO / name).read_text().splitlines()
            if line.startswith("FROM ")
        ]

    def test_every_build_stage_names_a_digest(self):
        for name in ("Dockerfile", "Dockerfile.web"):
            lines = self._from_lines(name)
            assert lines, f"{name} declares no FROM"
            for line in lines:
                assert "@sha256:" in line, f"{name}: unpinned base — {line}"

    def test_the_tag_is_kept_beside_the_digest(self):
        """A bare sha256 tells a reader nothing about what they are running."""
        for name in ("Dockerfile", "Dockerfile.web"):
            for line in self._from_lines(name):
                image = line.split()[1]
                assert ":" in image.split("@")[0], f"{name}: digest with no tag — {line}"

    def test_both_python_stages_use_the_same_base(self):
        """The bot and the web runtime must not drift apart by a patch."""
        digests = {
            line.split("@")[1].split()[0]
            for name in ("Dockerfile", "Dockerfile.web")
            for line in self._from_lines(name)
            if "python:" in line
        }
        assert len(digests) == 1, f"python stages disagree: {digests}"

    def test_the_service_images_are_deliberately_not_pinned(self):
        """Pinning those by digest makes the next `up -d` recreate the
        containers, and one of them is now the system of record for the mirror.
        Left on tags on purpose — this test says so out loud so that nobody
        'finishes the job' without meaning to."""
        compose = (REPO / "docker-compose.yml").read_text()
        assert "postgres:17.2-alpine@sha256" not in compose
