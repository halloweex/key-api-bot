"""The H3 staging-merge subsystem is gone, and must not half-come-back.

`bronze_order_events` was built 2026-04-19 as the foundation of a
staging-merge ingest: sync would write only an append-only event log, and a
promotion job would be the single writer of `orders`. It reached Phase 5 and
`SYNC_MODE=staging` was never switched on in production for a single day. The
shadow write in legacy mode was made opt-in on 2026-05-19 and defaulted off,
and the table has held **0 rows** ever since — verified on the 2026-09-14
production backup and, live, by the invariant check passing that morning.

It is also the standing counter-example this codebase cites when it explains
why `app.order_versions` records a *change* rather than an *observation*: this
table keyed on the observation, wrote ~150 000 rows a day and took the database
to 43 GB. That argument outlives the table and is deliberately left in place in
`test_order_versions.py`.

WHAT THESE TESTS ACTUALLY PROTECT

Not the deletion — a deletion does not rot. The two things that can:

1. **The compaction exclusion.** It is what physically removes the table. The
   weekly compact exports every table it finds in the *source* database and
   imports it into a fresh one built from `_init_schema()`. With the DDL gone
   and the name absent from `DERIVED_TABLES`, the import runs
   `INSERT INTO "bronze_order_events"` against a schema that no longer defines
   it — an error that is not a duplicate-key error, so phase 2 exits 1 and the
   whole weekly compact aborts, taking the off-site export with it. That is
   `orders_v2`' exact recorded failure, and this is the guard it never had.

2. **The symbols.** A reintroduced `should_write_bronze` or `append_bronze_events`
   would be a write path to a table that no longer exists.
"""
from __future__ import annotations

import pathlib

REPO = pathlib.Path(__file__).resolve().parents[2]


class TestTheLessonOutlivesTheTable:
    def test_order_versions_still_cites_the_43gb_incident(self):
        """The reason `app.order_versions` records a change and not an
        observation. Deleting the table does not delete the evidence for the
        rule it taught."""
        src = (REPO / "tests" / "unit" / "test_order_versions.py").read_text(
            encoding="utf-8")
        assert "43 GB" in src
