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
"""
from __future__ import annotations

from typing import Dict


def configure_modes() -> Dict[str, str]:
    """Read every cached mode from the environment, and say what was read.

    Idempotent: a second call reads the same environment into the same caches,
    so web's startup and the scheduler can both call it without either having
    to know whether the other already has.
    """
    from core import pg_derivation

    return {pg_derivation.ENV: pg_derivation.configure_mode()}
