"""The SQL a revision runs, rendered by Alembic offline."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]


def _alembic_sql(*args: str) -> str:
    """What `alembic <args> --sql` would execute, without its BEGIN/COMMIT, so
    it can run inside the test's own transaction."""
    out = subprocess.run(
        [sys.executable, "-m", "alembic", *args, "--sql"],
        cwd=REPO, capture_output=True, text=True, timeout=120,
        env={"PATH": "/usr/bin:/bin", "HOME": "/tmp"},
    )
    assert out.returncode == 0, out.stderr[-2000:]
    lines = [line for line in out.stdout.splitlines()
             if line.strip() not in ("BEGIN;", "COMMIT;")]
    return "\n".join(lines)
