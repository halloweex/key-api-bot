"""The modes a process reads once and caches — configured before anything writes.

A cached mode is read at one moment and trusted afterwards, so that moment has
to come before the first code path that consults it. `KS_PG_DERIVE` used to be
read in `BackgroundScheduler.start`, and web's startup runs the boot sync
(`init_and_sync`) before it starts the scheduler. The boot incremental sync
therefore landed orders in Postgres while `owns()` still answered False, and
`mark_if_owned` skipped in silence (both went in step 14's S14-15: Postgres
derives on its own signal unconditionally). The first derivation rebuilt over those
rows anyway, so no number was ever wrong — but any detector pairing writes with
marks would have read the boot as marks lost, and every cached mode added after
this one would have inherited the same hole.

So every entry point that can reach a landing writer calls `configure_modes()`
first: web's startup, before the boot sync; the scheduler, again, which is
harmless; and the scripts that reach `write_orders`. A test walks all three.

**A later cached mode is added here, and nowhere else.** The entry points know
this function, not the modes behind it, so none of them has to learn about a
new one — which is the whole reason this is a module rather than one more line
at each call site.

The write chains' ownership latch is loaded here too, for the same reason and
one more: it is read from local marker files, so loading it at the one moment
every entry point already calls keeps it synchronous and keeps Postgres off
the path. A chain that has written Postgres must route there from the first
question this process asks, including a boot where Postgres is unreachable —
see `core/chain_latch.py`.
"""
from __future__ import annotations

from typing import Dict


def configure_modes() -> Dict[str, str]:
    """Read every cached mode from the environment, and say what was read.

    Idempotent: a second call reads the same environment into the same caches,
    so web's startup and the scheduler can both call it without either having
    to know whether the other already has. The latch is re-read rather than
    assumed unchanged.

    **No mode is left to read** (step 14). S14-00 made every one a constant
    — the value production published on 2026-10-21
    (`.planning/DUCKDB_EXIT_STEP14_PLAN.md` H0) — and the PRs after it
    deleted each with its last reader: `KS_PG_DERIVE`, `KS_UTM_PARSE` and
    `KS_WRITE_WAREHOUSE` (S14-15), `KS_READ_FALLBACK` (S14-16) and
    `KS_DUCKDB` (S14-18, with `core/duckdb_switch.py`: nothing in any
    process opens the DuckDB file, so there is nothing for it to switch).
    The mapping is therefore empty; it stays the return value so that a mode
    added later is added here and nowhere else. A value left in `.env` for
    an image from before step 14 is read by nothing here, whatever it is.
    `web=True`, which made web run `KS_DUCKDB=off` whatever the variable
    said, went with the switch.
    """
    from core import chain_latch

    # Not in the returned mapping: that maps an environment variable to the
    # value read from it, and the latch is read from disk. Since S14-00 it
    # routes nothing — every chain writes Postgres — but it is still loaded,
    # because a first write still takes a marker and an owner row (decision
    # 3: what an image from before step 14 routes by, while one is the
    # rollback target), and
    # `/api/health` publishes `latched` from it.
    chain_latch.load()
    return {}
