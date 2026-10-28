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


def configure_modes(*, web: bool = False) -> Dict[str, str]:
    """Read every cached mode from the environment, and say what was read.

    Idempotent: a second call reads the same environment into the same caches,
    so web's startup and the scheduler can both call it without either having
    to know whether the other already has. The latch is re-read rather than
    assumed unchanged, because a copy-back between two calls releases it.

    `web` is web's own two calls, its startup and its scheduler. Since step
    14's S14-00 web runs `KS_DUCKDB=off` whatever the variable says
    (`duckdb_switch.configure_mode(web=True)`); every other entry point reads
    the variable as before.

    **Since S14-00 every mode configured here is a constant** — the value
    production published on 2026-10-21 (`.planning/DUCKDB_EXIT_STEP14_PLAN.md`
    H0): `KS_PG_DERIVE` own, `KS_READ_FALLBACK` off, `KS_UTM_PARSE` postgres,
    `KS_WRITE_WAREHOUSE` postgres, and every write chain on Postgres. Each
    `configure_mode()` still runs, so the health blocks keep their shape, and
    each module's line goes with the module (S14-15, S14-16, S14-18).
    `tests/unit/test_step14_prod_parity.py` holds every answer to what the
    code before S14-00 gave under production's environment and markers.
    """
    from core import (chain_latch, duckdb_switch, pg_derivation, pg_utm_parse,
                      read_fallback, warehouse_cutover)

    # Not in the returned mapping: that maps an environment variable to the
    # value read from it, and the latch is read from disk. Since S14-00 it
    # routes nothing — every chain writes Postgres — but it is still loaded,
    # because a first write still takes a marker and an owner row (decision
    # 3: what an image from before step 14 routes by, while one is the
    # rollback target), and
    # `/api/health` publishes `latched` from it.
    chain_latch.load()
    # `KS_DUCKDB` first: whether this process may open the DuckDB file at all.
    # In web, never (S14-00). Never raises — see `core/duckdb_switch.py`.
    #
    # The other four are constants since S14-00, configured only so their
    # health blocks keep their shape: `own`, `off`, `postgres`, `postgres`.
    # A dict display evaluates in order; none of them depends on another now.
    modes = {
        duckdb_switch.ENV: duckdb_switch.configure_mode(web=web),
        pg_derivation.ENV: pg_derivation.configure_mode(),
        read_fallback.ENV: read_fallback.configure_mode(report=False),
        pg_utm_parse.ENV: pg_utm_parse.configure_mode(),
        warehouse_cutover.ENV: warehouse_cutover.configure_mode(),
    }
    # The read switches with no address. Never raises — see
    # `read_fallback.report_misconfigured`.
    read_fallback.report_misconfigured()
    return modes
