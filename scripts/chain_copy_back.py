#!/usr/bin/env python3
"""Release a latched write chain: copy its tables back to DuckDB, then unlatch.

    cd /opt/key-api-bot && docker compose stop web bot
    docker run --rm --name chain-copy-back \
        -v /opt/key-api-bot/data:/app/data \
        --env-file /opt/key-api-bot/.env \
        --network key-api-bot_default \
        halloweex/keycrm-web:latest \
        python /app/scripts/chain_copy_back.py inventory --handover   # 1
    ... the same command without --handover (or with --dry-run)        # 2
    ... the same command with --execute                                # 3
    # exit 0 released · 1 rolled back · 2 refused · 3 committed, not released

`--handover` is the read-only question both directions need answered, and it
comes first in both. **Before flipping a chain** it is the gate: exit 0 says
everything DuckDB holds has reached Postgres, and anything CRITICAL is a row the
flip would strand, because the shipper stands down the moment the chain routes
to Postgres and these tables have no backfill. **Before rolling one back** it is
the preview: its CRITICALs are exactly what `--execute` refuses on, and its INFO
lines are the size of the copy. `--execute` asks the same question again itself
and refuses before writing anything, so skipping step 1 cannot destroy a row —
it only moves the discovery to step 3.

It needs no latch and writes nothing, but it does need the database file to
itself: DuckDB will not let a second process open a file a writer holds, not
even read-only. So it runs in the same stopped window as the copy-back.

The one-off-container shape is `scripts/weekly_compact.sh`'s: the image already
carries every module this needs, `./data` is the one directory both containers
mount, and `--env-file` supplies `KS_PG_DSN`. The network is the one thing that
script does not need and this one does. Postgres has no `ports:` key by charter
rule 12, so it is reachable only from a network it is on, and the DSN in `.env`
names the host `postgres` — the compose service alias. `key-api-bot_default` is
observed, not derived: on 2026-09-18 both the web container and ks-postgres
were on it (and on `ks-data`).

WHY A DRY RUN IS THE DEFAULT

This is the only thing that releases a latch, and a latch is the only thing
standing between two stores and two writers. The dry run (`--dry-run`, or no
flag at all) reads both sides, asks the handover question and prints what it
would carry; nothing else here is reversible by re-running it.

WHAT `--execute` DOES, AND WHAT EACH EXIT CODE LEAVES BEHIND

Every table, the watermarks and the id allocator are written in one DuckDB
transaction, and the zero-tolerance comparison runs inside it: it commits only
on zero findings. After the COMMIT come a checkpoint and the release of both
copies of the latch.

    0  released — or, for a dry run and --handover, nothing to refuse.
    1  NOT committed. The comparison inside the transaction found a
       difference and rolled back, or something failed before the COMMIT and
       left a traceback. DuckDB holds what it held (the id allocator at or
       above where it was), the latch is where it was, and `up -d` returns
       to the state before the run. For --handover: a CRITICAL.
    2  refused before anything was written: a precondition, the handover's
       CRITICALs, the DuckDB lock, or two flags that ask opposite things.
    3  COMMITTED, then the checkpoint or the release failed. DuckDB HAS the
       copy, so `up -d` does not return to the state before the run. The
       message names which copies of the latch survived and the next step for
       that state — usually `--execute` again, which finds DuckDB already
       equal and releases.

Exit 3 has its own code because exit 1 used to be both: an exception after the
COMMIT left through the interpreter's own exit 1, the code that means "rolled
back", with the copy in DuckDB. In none of the four is DuckDB left
half-copied — the transaction sees to that — but only 1 and 2 leave it as it
was.

HOW IT KNOWS WEB IS STOPPED, AND WHY IT IS NOT A CONTAINER CHECK

DuckDB's file lock is exclusive for writers, so opening the database read-write
IS the check: while `keycrm-web` is up it holds that lock and `duckdb.connect`
raises. A check that asked the docker socket instead could be wrong in both
directions — a container that is `Up` but has already closed the database, a
container the socket does not show because this one was started without it —
and it would have to be right about which container names count. The lock
cannot be wrong about the only thing that matters here, which is whether
something else is about to write these tables. What this script adds is the
sentence: the driver's message names a file and a pid and says nothing about
`docker compose stop`.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import duckdb  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("chain_copy_back")


def _parse(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy a latched write chain's tables back into DuckDB.",
    )
    parser.add_argument(
        "chain",
        help="the chain: 'expenses', 'inventory' or 'goals' (or its module name)",
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="write the rows and release the latch; without it nothing is written",
    )
    # The default already, and accepted by name because the plan, the runbook
    # and this module's own docstring all call it that. An operator who types
    # the word they were given must not get argparse's exit 2 — the same code
    # this script uses for a refusal — on the one command whose two modes must
    # never be confused.
    parser.add_argument(
        "--dry-run", action="store_true",
        help="read both stores and say what --execute would do (the default)",
    )
    parser.add_argument(
        "--handover", action="store_true",
        help="only report whether everything DuckDB holds has reached Postgres",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="print the result as JSON instead of as a report",
    )
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    from core.chain_transfer import (
        CommittedNotReleased, CopyBackRefused, copy_back, handover_check,
        resolve_chain,
    )
    from core.duckdb_store import DuckDBStore
    from core.runtime_modes import configure_modes

    # Two different asks, and honouring one of them silently is how somebody
    # types the pre-flight's flag and releases a latch.
    if args.dry_run and args.execute:
        print("REFUSED: --dry-run reads and --execute writes; pick one.",
              file=sys.stderr)
        return 2
    if args.handover and (args.execute or args.dry_run):
        print("REFUSED: --handover is its own report; run it on its own.",
              file=sys.stderr)
        return 2

    # The latch is read from the marker files, and `configure_modes` is the one
    # call every entry point makes to load it. Called here for the same reason
    # the writers' entry points call it: a process that decided for itself
    # which chains are latched would be a second opinion about the only fact
    # this script acts on.
    configure_modes()

    try:
        chain = resolve_chain(args.chain)
    except LookupError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2

    store = DuckDBStore()
    try:
        await store.connect()
    except duckdb.IOException as exc:
        # The driver says "Could not set lock on file ... held by PID n". True,
        # and it does not say what to do about it.
        print(
            "REFUSED: the DuckDB database is locked by another process, which "
            "on this host is the web or bot container. Nothing was read and "
            "nothing was written.\n"
            "  docker compose stop web bot      # then run this again\n"
            f"  (driver: {exc})",
            file=sys.stderr,
        )
        return 2

    try:
        if args.handover:
            return _handover(await handover_check(store, chain), args)
        result = await copy_back(store, chain, dry_run=not args.execute)
    except CopyBackRefused as exc:
        if args.json and exc.issues:
            print(json.dumps({"refused": str(exc).splitlines()[0],
                              "findings": [_row(i) for i in exc.issues]},
                             indent=2, default=str))
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2
    except CommittedNotReleased as exc:
        if args.json:
            print(json.dumps({**exc.plan, "error": str(exc).splitlines()[0]},
                             indent=2, default=str))
        print(f"COMMITTED, NOT RELEASED: {exc}", file=sys.stderr)
        return 3
    finally:
        # Never allowed to replace the code above. The checkpoint's likeliest
        # failure is a full disk, and closing a DuckDB file checkpoints it
        # again: an exception here would leave through the interpreter's exit
        # 1 — "rolled back" — on the one run that committed.
        try:
            await store.close()
        except Exception:
            logger.exception("closing the DuckDB store failed")

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        _report(result)
    # 0 only when there is nothing left to decide: the dry run that read
    # cleanly, or the execution that compared at zero and released. A copy
    # that did not compare is exit 1: it was rolled back and the latch is
    # still held. A copy that committed and then failed never reaches here —
    # that is exit 3, above.
    if not result.get("executed"):
        return 0
    return 0 if result.get("released") else 1


def _handover(issues, args: argparse.Namespace) -> int:
    """The pre-flight's verdict. Reads only, and needs no latch.

    It still needs the DuckDB file to itself, because DuckDB will not open a
    file a writer holds — so in practice it runs in the same stopped window as
    the copy-back, either before deciding to flip a chain or before deciding
    to bring one back. The value is the same in both directions: a key DuckDB
    holds and Postgres does not is stranded by a flip, and nothing after the
    flip can carry it across.
    """
    rows = [_row(i) for i in issues]
    if args.json:
        print(json.dumps(rows, indent=2, default=str))
    elif not rows:
        print("\nevery row DuckDB holds is in Postgres, and the values agree.\n")
    else:
        print()
        for found in rows:
            print(f"  [{found['severity']}] {found['check']} on {found['table']}"
                  f" ({found['count']}) samples={found['samples']}")
            print(f"      {found['description']}\n")
    return 1 if any(r["severity"] == "CRITICAL" for r in rows) else 0


def _row(issue) -> dict:
    return {"check": issue.check_name, "table": issue.table_name,
            "severity": issue.severity.value, "count": issue.count,
            "samples": list(issue.sample_ids), "description": issue.description}


def _report(result: dict) -> None:
    print(f"\nchain: {result['chain']}   marker {result['latched_at']}"
          f"   owner rows {result.get('owned_since')}")
    print(f"executed: {result.get('executed')}")
    print("\nrows read from Postgres:")
    for table, count in sorted(result["rows"].items()):
        print(f"  {table:<32} {count:>9,}")
    if result.get("sync_keys"):
        print("\nwatermarks carried back into sync_metadata:")
        for key, value in sorted(result["sync_keys"].items()):
            print(f"  {key:<32} {value}")
    if result.get("sequences"):
        print("\nids Postgres has already handed out:")
        for seq, issued in sorted(result["sequences"].items()):
            burned = (result.get("burned") or {}).get(seq)
            tail = "" if burned is None else f"   (burned {burned:,} in DuckDB)"
            print(f"  {seq:<32} {issued}{tail}")
    handover = result.get("handover") or []
    if handover:
        print("\nwhat the copy overwrites or carries (none of it blocking):")
        for found in handover:
            print(f"  [{found['severity']}] {found['check']} on {found['table']}"
                  f" ({found['count']}) samples={found['samples']}")
    findings = result.get("findings") or []
    if findings:
        print(f"\n{len(findings)} finding(s) — rolled back, the latch is NOT "
              "released:")
        for found in findings:
            print(f"  [{found['severity']}] {found['check']} on {found['table']}"
                  f" ({found['count']}) samples={found['samples']}")
            print(f"      {found['description']}")
    elif result.get("executed"):
        print("\nevery table compares equal at zero.")
    print("\nnext:")
    for line in result.get("runbook", ()):
        print(f"  {line}")
    print()


def main(argv=None) -> int:
    return asyncio.run(_run(_parse(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
