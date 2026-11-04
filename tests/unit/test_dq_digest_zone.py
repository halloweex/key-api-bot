"""The digest's run labels, pinned across step 14's S14-10.

Chain 9 hands every Postgres row of the journal to `pg_dq_journal_write._local`,
which converts each timestamp into one zone before the shapers render it, and
the 09:00 digest prints the first sixteen characters of `started_at`
(`core.data_quality.build_digest`). Until S14-10 that zone was DuckDB's
session `TimeZone`, asked of an in-memory DuckDB through the one opener
whenever the process ran `KS_DUCKDB=on` (`_duckdb_zone`), and the host's zone
read without DuckDB under `off` (`_host_zone`) — production's since
2026-10-16. S14-10 collapses the two into the second.

These literals were taken by running this very file on the base `e3112a7d`,
where a fresh interpreter with no `KS_DUCKDB` asked DuckDB, and they hold on
the head, where nothing asks DuckDB: for a process started with `TZ` set,
DuckDB's session zone and the host zone are the same zone. Each case runs in
an interpreter of its own, started with the zone set, because DuckDB took
its zone once, when it loaded.
"""
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]

# One run row as asyncpg hands it back — every instant in UTC — rendered
# through the chain's own reader and the digest's own header line.
_PROGRAM = r"""
import asyncio, json
from datetime import date, datetime, timezone

from core import pg_dq_journal_write as chain
from core.data_quality import DigestSection, build_digest

UTC = timezone.utc
ROW = (41, datetime(2026, 10, 1, 4, 0, 5, tzinfo=UTC),
       datetime(2026, 10, 1, 4, 0, 9, tzinfo=UTC),
       datetime(2026, 10, 1, 4, 0, 5, tzinfo=UTC), date(2026, 10, 1),
       date(2026, 10, 1), "integrity", "WARN", 1, 0, 0, 1, 0, 4000, None)
WINTER = (42, datetime(2026, 12, 1, 4, 0, 5, tzinfo=UTC),
          datetime(2026, 12, 1, 4, 0, 9, tzinfo=UTC),
          datetime(2026, 12, 1, 4, 0, 5, tzinfo=UTC), date(2026, 12, 1),
          date(2026, 12, 1), "mirror_landing", "WARN", 1, 0, 0, 1, 0, 4000, None)


class Conn:
    def __init__(self, row):
        self.row = row

    async def fetchrow(self, sql, *args):
        return self.row


async def main():
    runs = [await chain.latest_run(Conn(ROW), "integrity"),
            await chain.latest_run(Conn(WINTER), "mirror_landing")]
    issue = [{"check_name": "pk_uniqueness_orders", "table_name": "orders",
              "severity": "WARN", "count": 2, "sample_ids": [], "description": "d"}]
    digest = build_digest(
        [DigestSection(layer=r["layer"], run=r, issues=issue) for r in runs])
    print(json.dumps({"started_at": [r["started_at"] for r in runs],
                      "digest": digest}, ensure_ascii=False))


asyncio.run(main())
"""

PINNED = {
    "Europe/Kyiv": {
        "started_at": ["2026-10-01T07:00:05+03:00", "2026-12-01T06:00:05+02:00"],
        "heads": ["<b>integrity</b> · WARN · 2026-10-01 07:00",
                  "<b>mirror_landing</b> · WARN · 2026-12-01 06:00"],
    },
    "UTC": {
        "started_at": ["2026-10-01T04:00:05+00:00", "2026-12-01T04:00:05+00:00"],
        "heads": ["<b>integrity</b> · WARN · 2026-10-01 04:00",
                  "<b>mirror_landing</b> · WARN · 2026-12-01 04:00"],
    },
}


def _render(tz: str, tmp_path) -> dict:
    """The program in a fresh interpreter: the repository on its path, a tmp
    working directory, and an environment holding nothing but the zone —
    no `.env` reaches it, and no `KS_DUCKDB`, which is how the base asked
    DuckDB for its zone."""
    env = {"PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(ROOT),
           "PYTHONDONTWRITEBYTECODE": "1", "TZ": tz, "HOME": str(tmp_path)}
    done = subprocess.run([sys.executable, "-c", _PROGRAM], cwd=tmp_path, env=env,
                          capture_output=True, text=True, timeout=120)
    assert done.returncode == 0, done.stderr[-2000:]
    return json.loads(done.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize("tz", sorted(PINNED))
def test_the_digest_labels_are_pinned(tz, tmp_path):
    """Mutation: render in UTC whatever the process's zone (drop `_local`'s
    conversion), or read the zone from anything but `TZ` and the host — the
    Kyiv case prints 04:00 for a run the reader saw start at 07:00."""
    out = _render(tz, tmp_path)
    assert out["started_at"] == PINNED[tz]["started_at"]
    heads = [line for line in out["digest"].splitlines() if line.startswith("<b>")]
    assert heads == PINNED[tz]["heads"]
