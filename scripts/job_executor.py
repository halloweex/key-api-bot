#!/usr/bin/env python3
"""Who executes a scheduler job: show every switch, or set one.

K0 of the Rust plan (`core/job_lease.py`): a job runs in Python unless its
switch — the row `executor:<job_id>` of `meta.chain_watermarks` — says
exactly `rust`. This script is the switch's one writer. A change takes
effect at the job's next tick in both processes; nothing is deployed or
restarted. Every run of the job, in either process, also takes the job's
lease, so a run still in flight finishes before the other side starts one.

Usage, inside the web container (it has the DSN):

    docker exec keycrm-web python /app/scripts/job_executor.py show
    docker exec keycrm-web python /app/scripts/job_executor.py set ch_sync rust
    docker exec keycrm-web python /app/scripts/job_executor.py set ch_sync python

`show` lists every job the scheduler registers: who executes it, the
switch's value and since when, its lease key and who holds the lease now.
`set` refuses a job the scheduler does not register and any executor but
`python` or `rust`; `python` is the way back, and the row is kept so that
`updated_at` says when it was taken.

Exit 0 done, 2 refused, 1 Postgres could not be asked.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core import job_lease  # noqa: E402 — after the path above


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="job_executor.py",
        description="Who executes a scheduler job: Python (default) or Rust.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("show", help="every job, its executor and its lease")
    setter = commands.add_parser("set", help="hand one job to python or rust")
    setter.add_argument("job_id")
    setter.add_argument("executor", help="python or rust")
    return parser


async def _holder(conn, key: int) -> str:
    row = await conn.fetchrow(job_lease.HOLDER_SQL, key >> 32, key & 0xFFFFFFFF)
    if row is None:
        return "-"
    return f"pid {row['pid']} ({row['application_name'] or 'no application_name'})"


async def _show(conn, jobs: List[str]) -> int:
    rows = await conn.fetch(job_lease.READ_ALL_SWITCHES_SQL, job_lease.SWITCH_PREFIX)
    switches = {r["key"][len(job_lease.SWITCH_PREFIX):]: r for r in rows}
    print(f"{'job':<26} {'executor':<8} {'switch':<8} {'since':<26} "
          f"{'lease key':<20} held by")
    for job_id in jobs:
        row = switches.get(job_id)
        value = row["value"] if row is not None else None
        since = row["updated_at"].isoformat(timespec="seconds") if row is not None else "-"
        key = job_lease.lease_key(job_id)
        print(f"{job_id:<26} {job_lease.executor_of(value):<8} "
              f"{value if value is not None else '-':<8} {since:<26} "
              f"{key:<20} {await _holder(conn, key)}")
    for name in sorted(set(switches) - set(jobs)):
        print(f"warning: {job_lease.switch_key(name)} names no job the "
              f"scheduler registers; it switches nothing", file=sys.stderr)
    return 0


def _refusal(jobs: List[str], job_id: str, executor: str) -> Optional[str]:
    if job_id not in jobs:
        return (f"{job_id!r} is not a job the scheduler registers; "
                f"one of: {', '.join(jobs)}")
    if executor not in job_lease.EXECUTORS:
        return (f"executor {executor!r} is neither "
                f"{job_lease.PYTHON!r} nor {job_lease.RUST!r}")
    return None


async def run(argv: List[str]) -> int:
    args = _parser().parse_args(argv)
    from core.scheduler import registered_job_ids

    jobs = await registered_job_ids()
    if args.command == "set":
        refused = _refusal(jobs, args.job_id, args.executor)
        if refused is not None:
            print(f"refused: {refused}", file=sys.stderr)
            return 2

    import asyncpg

    from core.pg import dsn

    try:
        conn = await asyncpg.connect(dsn(), timeout=10)
    except Exception as exc:  # noqa: BLE001 — said, and the exit code is 1
        print(f"Postgres could not be asked: {type(exc).__name__}: {exc}",
              file=sys.stderr)
        return 1
    try:
        if args.command == "show":
            return await _show(conn, jobs)
        before = await job_lease.set_executor(conn, args.job_id, args.executor)
        print(f"{job_lease.switch_key(args.job_id)}: {before!r} -> "
              f"{args.executor!r}. {args.job_id} is executed by "
              f"{job_lease.executor_of(args.executor)} from its next tick; a run "
              f"in flight finishes first, holding the lease "
              f"{job_lease.lease_key(args.job_id)}.")
        return 0
    finally:
        await conn.close()


def main(argv: Optional[List[str]] = None) -> int:
    return asyncio.run(run(sys.argv[1:] if argv is None else argv))


if __name__ == "__main__":
    sys.exit(main())
