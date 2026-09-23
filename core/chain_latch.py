"""Once a chain has written Postgres, the flag alone cannot bring it back.

WHAT GOES WRONG WITHOUT THIS

Every consumer of a write chain routes on `KS_WRITE_*` at the moment of the
call — the three expense writers, the five inventory ones, the store's two
watermark methods, the hourly shipper and the daily comparison. So setting a
flag back to `duckdb` after rows have landed in Postgres does not undo
anything; it starts a second writer beside the first:

- a typed expense lands in DuckDB and never appears on the page, which reads
  Postgres;
- DuckDB's `seq_stock_movements_id` reissues ids the Postgres sequence has
  already handed out, and revision 0030 forbids exactly that — one store
  writes `stock_movements` and that store allocates the id;
- `replicate_operational` resumes its full replace out of a DuckDB that never
  saw the Postgres rows, so the hour after the flip rolls them back;
- a later copy-back then overwrites whichever half it did not come from.

None of that is loud. The measured shape of this family of faults is an hourly
rollback that looks healthy in between (`replicate_sms`' recorded failure).

THE ANSWER THE OWNER CHOSE (OD-19 (a), 2026-09-17): THE LATCH WINS

The first Postgres write a chain performs *latches* it. From then on
`writes_postgres()` answers True whatever the environment says, every consumer
reads that one answer, and the only way back is `scripts/chain_copy_back.py`
(DN-08), which copies the rows to DuckDB, compares them at zero and then
releases both copies of the latch. An environment that disagrees with the latch
is not silently obeyed and not silently ignored: it is published in
`/api/health`, paged as WARN by the canary, and stamped on the chain's tables
in `meta.mirror_state` by the shipper.

The flip side is stated plainly because it is the cost: a latched chain whose
Postgres is unreachable **fails its writes** rather than writing DuckDB. That
is the conservative direction — a refused expense is retyped, a row in the
store nobody reads is found weeks later.

TWO COPIES, AND THEY ANSWER DIFFERENT QUESTIONS

- A **local marker file** under the store's data directory, one per chain,
  written before the chain's first Postgres write in the process. It is the
  routing copy: it can be read synchronously, with no event loop and no
  database, which is what `writes_postgres()` needs and what makes the latch
  survive a boot with Postgres down.
- A row in `meta.chain_watermarks` under the `owner:<table>` prefix, inserted
  inside the writing transaction. It is the audit copy: it lives in the store
  that received the rows, it cannot be lost with the container's filesystem,
  and the daily comparison reads it against the markers.

They are written in that order on purpose. A marker without an owner row means
a first write that failed after the marker — the chain stays latched, which is
the safe direction, and the daily comparison says the two copies disagree. An
owner row without a marker means the marker was lost, and that is the dangerous
direction, so it is also reported.

IN PRODUCTION TODAY THERE IS NO LATCH, AND THAT IS CORRECT

`KS_WRITE_EXPENSES=postgres` has been on since 2026-09-17 08:33 UTC and
`app.manual_expenses` holds zero rows: no Postgres write has happened, so no
chain is latched and the flag can still be flipped back freely. The latch is
taken by the first expense somebody types, not by the deploy.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, FrozenSet, Mapping, Optional, Sequence

from core.duckdb_constants import DB_DIR

logger = logging.getLogger(__name__)

# Beside the DuckDB file, resolved the same way it is — `./data` is the one
# directory both containers mount and the only one the web container (uid 999)
# is known to be able to write, because it writes the database there.
MARKER_DIR: Path = DB_DIR / "write-chain-owners"

# The prefix the audit copy lives under in `meta.chain_watermarks`. Revision
# 0032's table is reused rather than a new one migrated: it already exists to
# hold what a chain owns once it writes Postgres, it is never copied out of
# DuckDB, and DN-06 ships no migration.
OWNER_PREFIX = "owner:"

# `{chain: latched_at}` — loaded from the markers once and cached, because
# `writes_postgres()` is asked on every write and every stand-down question.
# None means "not loaded yet", which is different from "nothing is latched".
_latched: Optional[Dict[str, str]] = None


def marker_path(chain: str) -> Path:
    return MARKER_DIR / chain


def load() -> Dict[str, str]:
    """Read every marker into the cache and return it. Never raises.

    Called by `core.runtime_modes.configure_modes()` before anything can write,
    and lazily by the first question if some entry point forgot to. Lazy rather
    than required, because a process that never configured the modes must not
    be the one that decides a latched chain may write DuckDB again.
    """
    global _latched
    found: Dict[str, str] = {}
    try:
        for path in sorted(MARKER_DIR.glob("*")):
            if path.is_file():
                found[path.name] = _stamp_of(path)
    except OSError as exc:
        # An unreadable directory is not an unlatched chain, and there is
        # nothing here that can tell the difference — so say so loudly and
        # keep whatever was already known rather than answering "free".
        logger.error("chain latch: cannot read %s: %s", MARKER_DIR, exc)
        if _latched is not None:
            return dict(_latched)
    _latched = found
    return dict(found)


def _stamp_of(path: Path) -> str:
    """When the chain was latched, from the marker or from the file itself.

    A marker that cannot be parsed still latches: the existence of the file is
    the latch, and its contents are only the audit trail. Falling back to the
    mtime keeps a truncated write — a container killed between `write` and
    `rename` cannot produce one, but a filesystem can — from reading as a chain
    that never took ownership.
    """
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        stamp = data.get("latched_at") if isinstance(data, dict) else None
        if isinstance(stamp, str) and stamp:
            return stamp
    except (OSError, ValueError) as exc:
        logger.warning("chain latch: %s is unreadable (%s); using its mtime", path, exc)
    return datetime.fromtimestamp(
        path.stat().st_mtime, timezone.utc).isoformat()


def _cache() -> Dict[str, str]:
    if _latched is None:
        load()
    return _latched if _latched is not None else {}


def latched(chain: str) -> bool:
    """Has this chain already written Postgres? Synchronous, no database."""
    return chain in _cache()


def latched_at(chain: str) -> Optional[str]:
    """When it was latched, as an ISO-8601 UTC string, or None."""
    return _cache().get(chain)


def latch(chain: str, env: str = "") -> str:
    """Take the local half of the latch, and return when it was taken.

    Idempotent and cheap after the first call in a process: an already-latched
    chain returns the stamp it was latched with, so the owner rows the writers
    insert carry the moment ownership actually passed rather than the moment of
    the latest write.

    **It raises if the marker cannot be written**, and the caller must not write
    Postgres anyway. A Postgres write with no local marker is precisely the
    state this module exists to prevent: the next boot would read the flag,
    find `duckdb`, and start the second writer. A refused write is visible; a
    silent second writer is not.
    """
    existing = _cache().get(chain)
    if existing:
        return existing
    stamp = datetime.now(timezone.utc).isoformat()
    _write_marker(marker_path(chain), json.dumps(
        {"chain": chain, "env": env, "latched_at": stamp}, ensure_ascii=False))
    _cache()[chain] = stamp
    logger.warning(
        "chain latch: %s now owns its tables in Postgres (%s). Only "
        "scripts/chain_copy_back.py releases this.", chain, stamp)
    return stamp


def release(chain: str) -> bool:
    """Drop the local marker. DN-08's copy-back is the only caller there should
    ever be; the tests use it to prove the unlatched behaviour is still real.

    **The removal is as durable as the creation**, and it has to be for the
    same reason one step later. `_write_marker` fsyncs the directory so a
    rename that never reached the disk cannot unlatch a chain on the next
    power cut; the copy-back releases the latch *after* it has copied the rows
    into DuckDB and put `KS_WRITE_*` back, so an unlink that never reached the
    disk brings the marker back to a chain whose rows are now in both stores —
    two writers allocating `stock_movements` ids, the state revision 0030
    forbids and the one this module exists to prevent.
    """
    try:
        marker_path(chain).unlink()
        removed = True
    except FileNotFoundError:
        removed = False
    if removed:
        _fsync_dir(MARKER_DIR)
    _cache().pop(chain, None)
    return removed


def _fsync_dir(path: Path) -> None:
    """Push a directory entry — a create or an unlink — to the disk itself."""
    dir_fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _write_marker(path: Path, payload: str) -> None:
    """Temp file, fsync, rename, fsync the directory.

    The marker decides where a chain's writes go for the rest of the machine's
    life, so a half-written one is worse than none: `_stamp_of` would have to
    guess, and a rename that never reached the disk would unlatch the chain on
    the next power cut. This is the arrangement the rest of the repository uses
    where a file is a decision rather than a cache.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    _fsync_dir(path.parent)


def owner_key(table: str) -> str:
    return f"{OWNER_PREFIX}{table}"


async def claim(conn, tables: Sequence[str], stamp: str) -> None:
    """The audit copy, written inside the caller's writing transaction.

    `ON CONFLICT DO NOTHING`: the first write is what took ownership and every
    later one only confirms it, so the stored moment must not drift forward —
    the comparison reads it against `meta.mirror_state.last_ok_at` to tell a
    shipment that predates the handover from one that overwrote it.

    Inside the transaction rather than beside it, so a write that rolls back
    does not claim what it did not write. The local marker is already on disk
    by then, which is the deliberate asymmetry: the routing copy fails safe,
    the audit copy fails honest.
    """
    if not tables:
        return
    await conn.executemany(
        "INSERT INTO meta.chain_watermarks (key, value, updated_at) "
        "VALUES ($1, $2, now()) ON CONFLICT (key) DO NOTHING",
        [(owner_key(table), stamp) for table in tables],
    )


async def read_owners(pool) -> Dict[str, str]:
    """`{table: latched_at}` — the audit copy, for the daily comparison."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT key, value FROM meta.chain_watermarks WHERE key LIKE $1",
            f"{OWNER_PREFIX}%",
        )
    return {r["key"][len(OWNER_PREFIX):]: r["value"] for r in rows}


def claimed_chains(owners: Mapping[str, str]) -> Dict[str, str]:
    """`{chain: earliest owner stamp}` for every chain holding an owner row.

    **What the local marker cannot answer.** The marker decides where writes
    go and it has to, because it is the only copy readable with Postgres down.
    But it lives on a bind mount: an older `./data` snapshot, a host move, a
    rebuilt data directory and the marker is gone while the rows it was taken
    for are still in Postgres. The routing then follows `KS_WRITE_*` again —
    and the hourly full replace, which stands down on that same answer, would
    put the table back the way a frozen DuckDB remembers it.

    So anything that is **already holding a Postgres connection** asks this as
    well, and stands down on either copy. It costs one query per run of the
    hourly and daily jobs that ask it — the operational pair here, the order
    paths through `pg_landing.order_tables_stood_down_or_owned` (DN-22a) — and
    it cannot be done on the write path itself: that is the read
    `writes_postgres()` exists to avoid.

    A chain, not a table: ownership passes for a chain as a unit, so one owner
    row holds all of its tables down. DN-08's copy-back deletes both copies, so
    a chain released deliberately is not held by this.
    """
    from core.write_chains import WRITE_CHAINS, chain_name

    out: Dict[str, str] = {}
    for chain in WRITE_CHAINS:
        stamps = [owners[t] for t in chain.CHAIN_TABLES if t in owners]
        if stamps:
            out[chain_name(chain)] = min(stamps)
    return out


def claimed_tables(owners: Mapping[str, str]) -> FrozenSet[str]:
    """Every table of every chain `claimed_chains` names."""
    from core.write_chains import WRITE_CHAINS, chain_name

    claimed = claimed_chains(owners)
    return frozenset(
        table
        for chain in WRITE_CHAINS if chain_name(chain) in claimed
        for table in chain.CHAIN_TABLES
    )
