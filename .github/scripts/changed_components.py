"""Which halves of the system a range of commits touched, for deploy.yml.

    git diff --name-only --no-renames BASE HEAD | python3 .github/scripts/changed_components.py

prints three lines for `$GITHUB_OUTPUT`:

    python=true|false       build the Python images and run the Python deploy
    rust_image=true|false   build and push the Rust pilot's image
    rust_deploy=true|false  run the Rust deploy (pull, up, nginx reload)

A path is the Rust pilot's only when it says so (`RUST_IMAGE`,
`RUST_SERVICE`); a path nobody named counts as Python, which is what every
path counted as before the pilot had a deploy of its own. `NEUTRAL` paths
count as nothing. `docker-compose.yml` counts for both: the Python deploy's
plain `up -d` never touches the pilot's service — it sits in a profile — so a
change to that service needs the Rust deploy to reach it.

The globs are GitHub's, not fnmatch's: `*` stops at a slash, and `**/` may
match no directory at all, so `**/*.md` takes the root's README too.
"""
from __future__ import annotations

import re
import sys
from typing import Dict, Iterable

NEUTRAL = (
    # deploy.yml's own `paths-ignore`: a push made only of these never starts it.
    "**/*.md", ".claude/**", ".planning/**", "tests/**", ".idea/**", "LICENSE", ".gitignore",
    # scripts/bump_version.sh rewrites it after every Python deploy, so it is
    # in every diff measured from one. A Python build reads it; it never
    # needs one.
    "VERSION",
    # The pilot's CI: nothing in it runs on the host.
    ".github/workflows/rust-pilot.yml",
)
RUST_IMAGE = ("rust/**", "Dockerfile.rust")
# What the Rust deploy applies without building anything.
RUST_SERVICE = ("nginx/rust/**", "docker-compose.yml")
# Of RUST_SERVICE, what is the pilot's alone: no Python deploy for it.
RUST_ONLY = RUST_IMAGE + ("nginx/rust/**",)


def _regex(glob: str) -> "re.Pattern[str]":
    out, i = [], 0
    while i < len(glob):
        if glob.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif glob.startswith("**", i):
            out.append(".*")
            i += 2
        elif glob[i] == "*":
            out.append("[^/]*")
            i += 1
        elif glob[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(glob[i]))
            i += 1
    return re.compile("".join(out))


def matches(path: str, globs: Iterable[str]) -> bool:
    return any(_regex(g).fullmatch(path) for g in globs)


def classify(paths: Iterable[str]) -> Dict[str, bool]:
    flags = {"python": False, "rust_image": False, "rust_deploy": False}
    for path in (p.strip() for p in paths):
        if not path or matches(path, NEUTRAL):
            continue
        if matches(path, RUST_IMAGE):
            flags["rust_image"] = flags["rust_deploy"] = True
        if matches(path, RUST_SERVICE):
            flags["rust_deploy"] = True
        if not matches(path, RUST_ONLY):
            flags["python"] = True
    return flags


def main() -> None:
    for name, value in classify(sys.stdin).items():
        print(f"{name}={'true' if value else 'false'}")


if __name__ == "__main__":
    main()
