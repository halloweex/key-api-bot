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
`writes_postgres()` answered True whatever the environment said, every
consumer read that one answer (until step 14's S14-11 deleted it with the
flags: a step-14 image writes Postgres, a constant), and the only way back was `scripts/chain_copy_back.py`
(DN-08), which copied the rows to DuckDB, compared them at zero and then
released both copies of the latch. Step 14 deleted it (S14-04): a step-14 image
routes by neither copy (S14-00) and has no way back but an image from before
step 14, which routes by both — `core.rollback_lever`, the line every page
about a chain's ownership names. An environment that disagrees with the latch
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, FrozenSet, Mapping, Optional, Sequence

# The two file primitives moved to `core.atomic_file` in step 14's S14-01,
# unchanged. The private names stay bound to them here: `latch` and `release`
# call them, the suite patches them here, and they keep any caller of the old
# names working for one release.
from core.atomic_file import fsync_dir as _fsync_dir
from core.atomic_file import write_atomic as _write_marker
from core.paths import DATA_DIR
from core.rollback_lever import ROLLBACK_RUNBOOK

logger = logging.getLogger(__name__)

# Beside the DuckDB file, resolved the same way it is — `./data` is the one
# directory both containers mount and the only one the web container (uid 999)
# is known to be able to write, because it writes the database there.
MARKER_DIR: Path = DATA_DIR / "write-chain-owners"

# The prefix the audit copy lives under in `meta.chain_watermarks`. Revision
# 0032's table is reused rather than a new one migrated: it already exists to
# hold what a chain owns once it writes Postgres, it is never copied out of
# DuckDB, and DN-06 ships no migration.
OWNER_PREFIX = "owner:"

# `{chain: latched_at}` — loaded from the markers once and cached, because
# `latch()` is asked on every write (and until step 14's S14-11 every
# chain's `writes_postgres()` and every stand-down question asked it too).
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


def markers_on_disk() -> Dict[str, str]:
    """`{chain: latched_at}` for every marker on disk now, read afresh and
    never into the cache: the standing watch asks whether a marker is still
    there (`chain_latch_disagrees` in `core.pg_chain_invariants`), and the
    cache would answer for the moment the process started. A directory that
    does not exist holds no marker; one that cannot be read raises `OSError`
    — an unreadable directory is not a lost marker, and the caller says it
    could not look.

    Listed with `os.scandir`, never `Path.glob` or `Path.exists`: both
    swallow the `PermissionError` of a directory that cannot be listed (3.12
    and 3.14 alike) and answer "nothing here", which turned every chain with
    owner rows into a false CRITICAL lost marker. A marker removed between the
    listing and its stamp is gone, which is what the caller asks."""
    try:
        with os.scandir(MARKER_DIR) as entries:
            names = sorted(entry.name for entry in entries if entry.is_file())
    except FileNotFoundError:
        return {}
    found: Dict[str, str] = {}
    for name in names:
        try:
            found[name] = _stamp_of(MARKER_DIR / name)
        except FileNotFoundError:
            continue
    return found


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
        "chain latch: %s now owns its tables in Postgres (%s). Nothing in "
        "this build releases it; an image from before step 14 routes by this "
        "marker, so keep it (%s).", chain, stamp, ROLLBACK_RUNBOOK)
    return stamp


def release(chain: str) -> bool:
    """Drop the local marker. DN-08's copy-back was the only production caller
    until step 14's S14-04 deleted it; the tests use it to put a marker back
    the way they found it.

    **The removal is as durable as the creation**, and it has to be for the
    same reason one step later. `_write_marker` fsyncs the directory so a
    rename that never reached the disk cannot unlatch a chain on the next
    power cut; the copy-back released the latch *after* it had copied the
    rows into DuckDB and put `KS_WRITE_*` back, so an unlink that never
    reached the disk brought the marker back to a chain whose rows were then
    in both stores — two writers allocating `stock_movements` ids, the state
    revision 0030 forbids and the one this module exists to prevent.
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
    it cannot be done on the write path itself: that is the read the
    chains' `writes_postgres()` existed to avoid (step 14's S14-11 deleted it
    with the flags).

    A chain, not a table: ownership passes for a chain as a unit, so one owner
    row holds all of its tables down. DN-08's copy-back deleted both copies,
    so a chain it released was not held by this; nothing releases one since
    step 14's S14-04.
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


def owned_tables(owners: Mapping[str, str]) -> FrozenSet[str]:
    """Every table the owner rows hold down: the whole of each registered
    chain one of them names (`claimed_tables`), and every owner row as itself.

    **The second half is what an image older than the chain needs.** Such a
    build declares no chain for the table, so `claimed_tables` alone hands it
    back to DuckDB, and the hourly full replace then puts a frozen DuckDB's
    copy over rows only Postgres holds. Reproduced for `app.buyer_gender`
    (DN-22b review): a verdict a human overrode in Postgres was replaced within
    the hour, and the two copies then agreed, so the daily comparison had
    nothing to say. `chain_owner_unregistered` is what says it instead.

    Every path that held a pool and stood down on the owner rows read this
    one answer — the operational pair, until step 14's S14-12 and S14-13
    deleted it, and `pg_landing.owned_among` — so no two of them could
    disagree about which tables had changed hands.
    """
    return claimed_tables(owners) | frozenset(owners)


def unregistered_owned_tables(owners: Mapping[str, str]) -> Dict[str, str]:
    """`{table: latched_at}` for every owner row that names a table no chain in
    this build declares — an image older than the chain, or a chain dropped
    from `WRITE_CHAINS`. Held down by `owned_tables`,
    reported by the daily comparison as `chain_owner_unregistered`."""
    from core.write_chains import WRITE_CHAINS

    declared = {table for chain in WRITE_CHAINS for table in chain.CHAIN_TABLES}
    return {table: at for table, at in owners.items() if table not in declared}


# ─── A chain that stamped the mirror's watermark itself ─────────────────────
#
# The daily comparison read `meta.mirror_state.last_ok_at` after the owner row
# as a shipment out of DuckDB over the chain's rows (`chain_shipper_overwrote`,
# the stage-4 plan's E1), and a chain whose writer stamps the watermark itself
# handed it the watermark and its record of its own write instants together
# (`MirrorStamp`, chain 6's `read_mirror_stamps`). Step 14's S14-12 deleted the
# comparison and both.


# ─── The lock-out a flip waited for — until step 14's S14-11 ──────────────────
#
# Every chain registered from #280 on named the revision that refuses the
# images built before it (`LOCKOUT_REVISION`, 0035 for the eight #280
# registered), and its flag moved the writes only in a build that required
# that revision or a later one (`lockout_unmet`, read off the four digits
# every revision id starts with). Since S14-00 no flag moves anything, and
# S14-11 deleted the constants and `lockout_unmet` with the flags; which
# revision knows each chain's tables is the literal `LOCKOUT` in
# `tests/unit/test_chain_lockout.py`, which still holds every chain to a
# revision that comments its tables.
