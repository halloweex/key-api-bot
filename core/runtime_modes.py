"""The modes a process reads once and caches — configured before anything writes.

A cached mode is read at one moment and trusted afterwards, so that moment has
to come before the first code path that consults it. `KS_PG_DERIVE` used to be
read in `BackgroundScheduler.start`, and web's startup runs the boot sync
(`init_and_sync`) before it starts the scheduler. The boot incremental sync
therefore landed orders in Postgres while `owns()` still answered False, and
`mark_if_owned` skipped in silence. The first derivation rebuilt over those
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
    assumed unchanged, because a copy-back between two calls releases it.
    """
    from core import chain_latch, pg_derivation, read_fallback, warehouse_cutover

    # Not in the returned mapping: that maps an environment variable to the
    # value read from it, and the latch is read from disk and answers over the
    # variable rather than out of it. It is empty in production today —
    # `KS_WRITE_EXPENSES=postgres` has been on since 2026-09-17 08:33 UTC and
    # `app.manual_expenses` still holds zero rows, so no chain has written
    # Postgres and none is latched. The first typed expense takes it.
    chain_latch.load()
    # `KS_READ_FALLBACK` (DN-20a), and with it the read switches that name an
    # engine this process has no address for. Never raises: web is the only
    # syncer, and a crash loop over how a read degrades would stop order
    # intake — see `core/read_fallback.py`.
    # `KS_WRITE_WAREHOUSE` (DN-28): read and published, never acted on in this
    # build — the switch is DN-29, and it must find the mode cached before the
    # boot sync, whose empty-DuckDB path runs a full warehouse rebuild. Never
    # raises, for `KS_READ_FALLBACK`'s reason — see `core/warehouse_cutover.py`.
    return {
        pg_derivation.ENV: pg_derivation.configure_mode(),
        read_fallback.ENV: read_fallback.configure_mode(),
        warehouse_cutover.ENV: warehouse_cutover.configure_mode(),
    }
