"""Which half a change deploys: deploy/changed_components.py.

The rule the deploy depends on is asymmetric on purpose. A path is the Rust
pilot's only when the script names it; everything else counts as Python,
which is what every path counted as before the pilot had a deploy of its own.
So the failure a forgotten path can cause is a Python deploy nobody needed —
never a Python change that does not ship.
"""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "deploy" / "changed_components.py"
_spec = importlib.util.spec_from_file_location("changed_components", SCRIPT)
components = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(components)

NOTHING = {"python": False, "rust_image": False, "rust_deploy": False}


def _flags(**on):
    return {**NOTHING, **{k: True for k in on}}


@pytest.mark.parametrize(
    "paths, expected",
    [
        # The pilot's code: its image and its deploy, nothing of Python's.
        (["rust/crates/ks-api/src/http.rs"], _flags(rust_image=True, rust_deploy=True)),
        (["Dockerfile.rust"], _flags(rust_image=True, rust_deploy=True)),
        (["rust/contract/access.json"], _flags(rust_image=True, rust_deploy=True)),
        # A switch: a reload, no build at all.
        (["nginx/rust/routes.conf"], _flags(rust_deploy=True)),
        # The pilot's service lives in compose, which the Python half reads too.
        (["docker-compose.yml"], _flags(python=True, rust_deploy=True)),
        # Python, and anything nobody named.
        (["core/filters.py"], _flags(python=True)),
        (["nginx/nginx.conf"], _flags(python=True)),
        (["Dockerfile.web"], _flags(python=True)),
        (["rustacean.py"], _flags(python=True)),
        (["a/rust/b.py"], _flags(python=True)),
        # Neither: docs, tests, the bump, the pilot's CI.
        (["VERSION", "CHANGELOG.md"], NOTHING),
        (["README.md", "rust/README.md", "tests/unit/test_x.py"], NOTHING),
        ([".github/workflows/rust-pilot.yml"], NOTHING),
        ([], NOTHING),
        # Both halves at once.
        (["rust/crates/ks-api/src/http.rs", "web/main.py"],
         _flags(python=True, rust_image=True, rust_deploy=True)),
    ],
)
def test_a_change_deploys_the_half_it_belongs_to(paths, expected):
    assert components.classify(paths) == expected


def test_the_globs_are_githubs():
    """`**/` may match no directory at all, and `*` stops at a slash."""
    assert components.matches("README.md", ["**/*.md"])
    assert components.matches("a/b/c.md", ["**/*.md"])
    assert components.matches("rust/x/y/z.rs", ["rust/**"])
    assert not components.matches("rust", ["rust/**"])
    assert not components.matches("a/b.txt", ["*.txt"])


def test_what_never_starts_a_deploy_counts_as_nothing():
    """deploy.yml's own `paths-ignore` stops a run made only of those paths;
    the classifier must say the same about them inside a run."""
    workflow = yaml.safe_load((REPO / ".github" / "workflows" / "deploy.yml").read_text())
    trigger = workflow[True] if True in workflow else workflow["on"]
    ignored = trigger["push"]["paths-ignore"]
    assert set(ignored) <= set(components.NEUTRAL), set(ignored) - set(components.NEUTRAL)


def test_it_prints_what_github_output_reads():
    out = subprocess.run(
        [sys.executable, str(SCRIPT)], input="rust/Cargo.toml\nnginx/rust/routes.conf\n",
        capture_output=True, text=True, check=True,
    ).stdout
    assert out.splitlines() == ["python=false", "rust_image=true", "rust_deploy=true"]
