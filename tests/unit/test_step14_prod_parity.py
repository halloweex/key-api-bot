"""Step 14's S14-00 changes no answer production gives (rule 1).

S14-00 turned every cached mode, start verdict and routing predicate into a
constant: the value production published on 2026-10-21 (H0). This holds the
code to that, two ways.

**The literal below was captured by running the code before S14-00**
(`origin/main` at 57fd7c22, VERSION 3.0.293 — image 3.0.292's code but for
the version) under production's environment (`PROD_ENV`, the effective
environment of the web container, names and values only, no secrets) and
production's eleven chain markers (`PROD_MARKERS`). Only the Postgres facts
that code's start asked were stubbed, with what production holds
(`_stub_the_old_start`); everything else ran as production runs it. The
capture is this module run as a script from a checkout of that commit:

    git worktree add --detach /tmp/s14-old 57fd7c22
    cd /tmp/s14-old && env -i PATH=/usr/bin:/bin HOME=$HOME TZ=UTC \\
        <venv>/bin/python -I <this checkout>/tests/unit/test_step14_prod_parity.py 11 old-11.json

and again with `none` for no markers: the two differ in `latched` and
`latched_at` alone, which `EXPECTED` leaves out and the tests assert per state.
The same capture from `origin/main` at d621618a (3.0.295's code but for the
version, rechecked 2026-10-22) and at 6ac33044 (3.0.298's code but for the
version, the image production ran on 2026-10-25, rechecked that day) equals
`EXPECTED` too, with the markers and without: every image from 3.0.292 to
the one production ran when this was last rechecked answers alike.

**Then the code after S14-00 must answer the same**, with the eleven markers
and with none — it no longer routes by them — and with any value, or no
value, in each variable it stopped reading (rule 3): import, `configure_modes`,
`/api/health` 200 and the same constants. Beside it the live snapshot (`H0`):
the health blocks S14-00 touches, as production published them before it.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# The web container's effective environment on 2026-10-21, the mode switches
# only (`KS_MIRROR_LANDING` is unset there, and so absent here).
PROD_ENV = {
    'KS_DUCKDB': 'off',
    'KS_DQ_PG_WAREHOUSE': 'on',
    'KS_GOALS_HISTORY': 'silver',
    'KS_PG_DERIVE': 'own',
    'KS_UTM_PARSE': 'postgres',
    'KS_WRITE_WAREHOUSE': 'postgres',
    'KS_READ_FALLBACK': 'off',
    'KS_USER_STORE': 'postgres',
    'KS_SMS_STORE': 'postgres',
    'KS_BOT_STORE': 'postgres',
    'KS_READ_BUYER_SYNC': 'postgres',
    'KS_READ_CHAT': 'postgres',
    'KS_READ_COHORTS': 'clickhouse',
    'KS_READ_DASHBOARD': 'postgres',
    'KS_READ_EXPENSES': 'postgres',
    'KS_READ_FORECAST_INPUT': 'postgres',
    'KS_READ_GOALS': 'postgres',
    'KS_READ_GOLD': 'postgres',
    'KS_READ_INVENTORY': 'postgres',
    'KS_READ_LOOKUPS': 'postgres',
    'KS_READ_MARGIN': 'postgres',
    'KS_READ_MARKETING': 'postgres',
    'KS_READ_PRODUCTS_INTEL': 'postgres',
    'KS_READ_REPORTS': 'postgres',
    'KS_READ_SEARCH_INDEX': 'postgres',
    'KS_READ_SILVER': 'postgres',
    'KS_READ_TRAFFIC': 'postgres',
    'KS_READ_WEEKLY': 'postgres',
    'KS_WRITE_BUYERS': 'postgres',
    'KS_WRITE_CATALOGUE': 'postgres',
    'KS_WRITE_DQ_JOURNAL': 'postgres',
    'KS_WRITE_EXPENSES': 'postgres',
    'KS_WRITE_EXPENSE_TYPES': 'postgres',
    'KS_WRITE_FORECAST': 'postgres',
    'KS_WRITE_GOALS': 'postgres',
    'KS_WRITE_INVENTORY': 'postgres',
    'KS_WRITE_MANAGERS': 'postgres',
    'KS_WRITE_ORDERS': 'postgres',
    'KS_WRITE_TRAFFIC_LEDGER': 'postgres',
    'KS_WRITE_WATCHDOGS': 'postgres',
    'KS_WRITE_WEEKLY_LEDGER': 'postgres',
}
# Addresses production has and the snapshot does not list. Never contacted:
# what asks Postgres is refused at a port nothing listens on.
ADDRESSES = {
    "KS_PG_DSN": "postgresql://s14:s14@127.0.0.1:1/none",
    "KS_CH_URL": "http://s14-clickhouse:8123",
}
# `ls data/write-chain-owners/` in production, with each marker's stamp.
PROD_MARKERS = {
    'pg_inventory_write': '2026-09-30T13:33:23.853917+00:00',
    'pg_expense_types_write': '2026-10-03T23:00:02.720882+00:00',
    'pg_buyers_write': '2026-10-09T11:20:16.600912+00:00',
    'pg_orders_write': '2026-10-15T08:04:08.775536+00:00',
    'pg_managers_write': '2026-10-15T08:38:46.164760+00:00',
    'pg_catalogue_write': '2026-10-09T10:45:13.114362+00:00',
    'pg_forecast_write': '2026-10-12T00:30:03.911109+00:00',
    'pg_dq_journal_write': '2026-10-09T16:00:01.476098+00:00',
    'pg_watchdog_write': '2026-10-09T10:00:00.036342+00:00',
    'pg_weekly_ledger_write': '2026-10-12T06:30:02.781538+00:00',
    'pg_traffic_ledger_write': '2026-10-12T06:45:01.347735+00:00',
}
# `/api/health.backups` in H0: hours since each backup marker.
BACKUP_AGES_H = {".pg_pitr_drill_last_ok": 45.7, ".pg_restore_drill_remote_last_ok": 46.0,
                 ".pg_offsite_last_ok": 22.7}

# Every variable S14-00 stopped reading for a decision (the plan's list).
STOPPED_READING = (
    "KS_DUCKDB", "KS_WRITE_WAREHOUSE", "KS_PG_DERIVE", "KS_UTM_PARSE",
    "KS_READ_FALLBACK", "KS_GOALS_HISTORY", "KS_DQ_PG_WAREHOUSE",
    "KS_MIRROR_LANDING", "KS_USER_STORE", "KS_SMS_STORE",
    "KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES", "KS_WRITE_GOALS",
    "KS_WRITE_EXPENSE_TYPES", "KS_WRITE_BUYERS", "KS_WRITE_ORDERS",
    "KS_WRITE_MANAGERS", "KS_WRITE_CATALOGUE", "KS_WRITE_FORECAST",
    "KS_WRITE_DQ_JOURNAL", "KS_WRITE_WATCHDOGS", "KS_WRITE_WEEKLY_LEDGER",
    "KS_WRITE_TRAFFIC_LEDGER",
)
JUNK = "junk-s14"

# What a later step-14 PR took away on purpose, by the plan, and which PR:
# `EXPECTED` and `H0` stay as captured, every comparison leaves these out, and
# `/api/health` is asserted to publish none of them. S14-07 deleted the
# `KS_GOALS_HISTORY` refusal with its health block and canary check.
RETIRED = {"goals_history": "S14-07",
           # S14-11 deleted the registry's per-chain answer with the flags.
           "chain_modes": "S14-11",
           # S14-14 deleted KS_DQ_PG_WAREHOUSE's reader, a constant `on`.
           "pg_twins_flag": "S14-14",
           # S14-15 deleted step 13's switch (`core/warehouse_cutover.py`),
           # KS_PG_DERIVE's and KS_UTM_PARSE's modes, and the landing mirror's
           # `enabled`/`mirror_on` — and from `/api/health` the
           # `warehouse_writer_mode` and `utm_parse` blocks and `derivation`'s
           # `mode` and `error`, the only fields of it captured here.
           "warehouse_cutover": "S14-15",
           "pg_derivation": "S14-15",
           "pg_utm_parse": "S14-15",
           "mirror": "S14-15",
           "warehouse_writer_mode": "S14-15",
           "utm_parse": "S14-15",
           "derivation": "S14-15",
           # S14-16 deleted the read fallback's mode and the configurations it
           # published (`mode`, `mode_error`, `refusing`, `misconfigured`,
           # `no_engine_routes`); the block's fields on `/api/health` are
           # `RETIRED_FIELDS`', since the block itself stays, as `{refused}`.
           "read_fallback": "S14-16",
           # S14-18 deleted the DuckDB switch (`core/duckdb_switch.py`) with
           # its block on `/api/health`, and the two store predicates the
           # table-fate manifest was the last to ask
           # (`user_store_is_postgres`, `sms_store_is_postgres`).
           "duckdb_switch": "S14-18",
           "stores": "S14-18"}

# The same for single fields, by their path in a capture. S14-09 deleted chain
# 3's, 6's and 6a's routing answers the capture asks of the modules themselves
# (`mode`, `unmet_precondition`, chain 3's hold) and chain 3's preflight from
# `/api/health`. The registry's own `mode` and `unmet_precondition` for those
# chains (`chain_modes`) stay until S14-11, and are still compared.
RETIRED_FIELDS = {
    ("chains", "pg_orders_write", "mode"): "S14-09",
    ("chains", "pg_orders_write", "unmet_precondition"): "S14-09",
    ("chains", "pg_orders_write", "unmet_reasons"): "S14-09",
    ("chains", "pg_orders_write", "settle_hold"): "S14-09",
    ("chains", "pg_orders_write", "held"): "S14-09",
    ("chains", "pg_catalogue_write", "mode"): "S14-09",
    ("chains", "pg_catalogue_write", "unmet_precondition"): "S14-09",
    ("chains", "pg_expense_types_write", "unmet_precondition"): "S14-09",
    ("health", "write_chains", "pg_orders_write", "preflight"): "S14-09",
    # S14-10 deleted the shadow chains' comparison and the registry's answer
    # to which of their tables it compared (empty since S14-00).
    ("registry", "compared_in_shadow"): "S14-10",
    # S14-11 deleted every chain's routing answer with the flags, the
    # registry's mismatch list, and the `write_chains` entries' flag fields;
    # `stood_down_tables`/`stood_down_sync_keys` are asked of
    # `chain_tables()`/`sync_keys()` there, and compared.
    ("registry", "mismatched_chains"): "S14-11",
    # S14-12 deleted the DuckDB comparisons and the 05:30 job's DuckDB arm,
    # and with them the predicate the comparisons stood down on and chain 3's
    # answer to which layer that arm wrote.
    ("warehouse_cutover", "warehouse_checks_stand_down"): "S14-12",
    ("chains", "pg_orders_write", "stood_down_layers"): "S14-12",
    # S14-14 deleted the DuckDB integrity scan, and with it the sets its
    # checks stood down by: step 13's (published by `status()` too) and
    # chain 3's.
    ("warehouse_cutover", "stood_down_duckdb_checks"): "S14-14",
    ("warehouse_cutover", "status", "stood_down_duckdb_checks"): "S14-14",
    ("chains", "pg_orders_write", "landing_checks_stood_down"): "S14-14",
    # S14-15 stopped configuring the three modes it deleted, so
    # `configure_modes()` names them no more.
    **{(call, name): "S14-15"
       for call in ("configure_modes", "configure_modes_again")
       for name in ("KS_PG_DERIVE", "KS_UTM_PARSE", "KS_WRITE_WAREHOUSE")},
    # S14-16 stopped configuring KS_READ_FALLBACK, and `/api/health`'s
    # `read_fallback_mode` keeps only `refused`, which the capture never
    # asked (a count, not a constant).
    **{(call, "KS_READ_FALLBACK"): "S14-16"
       for call in ("configure_modes", "configure_modes_again")},
    **{("health", "read_fallback_mode", name): "S14-16"
       for name in ("mode", "error", "misconfigured", "no_engine")},
    # S14-18 stopped configuring KS_DUCKDB: `configure_modes()` names no mode
    # any more.
    **{(call, "KS_DUCKDB"): "S14-18"
       for call in ("configure_modes", "configure_modes_again")},
}
_S14_11_HEALTH_FIELDS = ("env", "mode", "error", "latched", "mismatch",
                         "unmet_precondition", "shadow", "preflight")


def _without_retired(result: dict) -> dict:
    """`result`, a capture or a literal, without the blocks `RETIRED` names
    and the fields `RETIRED_FIELDS` names."""
    result = json.loads(json.dumps(result))
    for key in RETIRED:
        result.pop(key, None)
        result.get("health", {}).pop(key, None)
    for path in RETIRED_FIELDS:
        node = result
        for step in path[:-1]:
            node = node.get(step, {}) if isinstance(node, dict) else {}
        if isinstance(node, dict):
            node.pop(path[-1], None)
    return result

# Captured on 57fd7c22 — see the module's docstring. Not to be edited by hand:
# a difference here is a change of behaviour, and the answer is a recapture
# with the command above and a reason in the commit.
EXPECTED = json.loads(r"""
{
 "chain_modes": {
  "pg_buyers_write": {
   "env": "KS_WRITE_BUYERS",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_catalogue_write": {
   "env": "KS_WRITE_CATALOGUE",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_dq_journal_write": {
   "env": "KS_WRITE_DQ_JOURNAL",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": true,
   "unmet_precondition": null
  },
  "pg_expense_types_write": {
   "env": "KS_WRITE_EXPENSE_TYPES",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_expenses_write": {
   "env": "KS_WRITE_EXPENSES",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_forecast_write": {
   "env": "KS_WRITE_FORECAST",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_goals_write": {
   "env": "KS_WRITE_GOALS",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_inventory_write": {
   "env": "KS_WRITE_INVENTORY",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_managers_write": {
   "env": "KS_WRITE_MANAGERS",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_orders_write": {
   "env": "KS_WRITE_ORDERS",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_traffic_ledger_write": {
   "env": "KS_WRITE_TRAFFIC_LEDGER",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": true,
   "unmet_precondition": null
  },
  "pg_watchdog_write": {
   "env": "KS_WRITE_WATCHDOGS",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": true,
   "unmet_precondition": null
  },
  "pg_weekly_ledger_write": {
   "env": "KS_WRITE_WEEKLY_LEDGER",
   "error": null,
   "mismatch": false,
   "mode": "postgres",
   "shadow": true,
   "unmet_precondition": null
  }
 },
 "chains": {
  "pg_buyers_write": {
   "mode": "postgres",
   "unmet_precondition": null,
   "writes_postgres": true
  },
  "pg_catalogue_write": {
   "mode": "postgres",
   "unmet_precondition": null,
   "writes_postgres": true
  },
  "pg_dq_journal_write": {
   "reads_postgres": true,
   "unmet_precondition": null,
   "writes_postgres": true
  },
  "pg_expense_types_write": {
   "unmet_precondition": null,
   "writes_postgres": true
  },
  "pg_expenses_write": {
   "unmet_precondition": "<none>",
   "writes_postgres": true
  },
  "pg_forecast_write": {
   "_routes_postgres": true,
   "reads_postgres": true,
   "reads_the_chain": [
    true,
    false
   ],
   "unmet_precondition": null,
   "writes_postgres": true
  },
  "pg_goals_write": {
   "reads_postgres": true,
   "reads_the_chain": [
    true,
    false
   ],
   "unmet_precondition": "<none>",
   "writes_postgres": true
  },
  "pg_inventory_write": {
   "unmet_precondition": "<none>",
   "writes_postgres": true
  },
  "pg_managers_write": {
   "mode": "postgres",
   "reads_postgres": true,
   "reads_the_chain": [
    true,
    false
   ],
   "unmet_precondition": null,
   "unmet_reasons": [],
   "writes_postgres": true
  },
  "pg_orders_write": {
   "held": null,
   "landing_checks_stood_down": [
    "fk_orphan_order_products_order_id",
    "not_null_orders_ordered_at",
    "not_null_orders_source_id",
    "not_null_orders_status_id",
    "orders_without_line_items",
    "pk_uniqueness_order_products",
    "pk_uniqueness_orders",
    "status_group_agreement",
    "value_domain_orders_source_id",
    "value_domain_orders_status_id"
   ],
   "mode": "postgres",
   "settle_hold": null,
   "stood_down_layers": [
    "reconciliation"
   ],
   "unmet_precondition": null,
   "unmet_reasons": [],
   "writes_postgres": true
  },
  "pg_traffic_ledger_write": {
   "unmet_precondition": null,
   "writes_postgres": true
  },
  "pg_watchdog_write": {
   "unmet_precondition": null,
   "writes_postgres": true
  },
  "pg_weekly_ledger_write": {
   "unmet_precondition": null,
   "writes_postgres": true
  }
 },
 "configure_modes": {
  "KS_DUCKDB": "off",
  "KS_PG_DERIVE": "own",
  "KS_READ_FALLBACK": "off",
  "KS_UTM_PARSE": "postgres",
  "KS_WRITE_WAREHOUSE": "postgres"
 },
 "configure_modes_again": {
  "KS_DUCKDB": "off",
  "KS_PG_DERIVE": "own",
  "KS_READ_FALLBACK": "off",
  "KS_UTM_PARSE": "postgres",
  "KS_WRITE_WAREHOUSE": "postgres"
 },
 "duckdb_switch": {
  "error": null,
  "health_block": {
   "error": null,
   "held_back": [],
   "mode": "off",
   "opened_while_off": {},
   "value": "off"
  },
  "held_back": [],
  "is_off": true,
  "mode": "off",
  "value": "off"
 },
 "goals_history": {
  "mode": "silver",
  "state": {
   "error": null,
   "mode": "silver"
  }
 },
 "health": {
  "derivation": {
   "error": null,
   "mode": "own"
  },
  "duckdb_switch": {
   "error": null,
   "held_back": [],
   "mode": "off",
   "opened_while_off": {},
   "value": "off"
  },
  "goals_history": {
   "error": null,
   "mode": "silver"
  },
  "read_fallback_mode": {
   "error": null,
   "misconfigured": [],
   "mode": "off",
   "no_engine": []
  },
  "status_code": 200,
  "utm_parse": {
   "error": null,
   "mode": "postgres"
  },
  "warehouse_writer_mode": {
   "error": null,
   "held": false,
   "held_for_s": null,
   "mode": "postgres",
   "preconditions_unmet": [],
   "reclassify_needed": false,
   "value": "postgres",
   "way_back_refused": []
  },
  "write_chains": {
   "pg_buyers_write": {
    "env": "KS_WRITE_BUYERS",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": null,
    "shadow": false,
    "unmet_precondition": null
   },
   "pg_catalogue_write": {
    "env": "KS_WRITE_CATALOGUE",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": null,
    "shadow": false,
    "unmet_precondition": null
   },
   "pg_dq_journal_write": {
    "env": "KS_WRITE_DQ_JOURNAL",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": null,
    "shadow": true,
    "unmet_precondition": null
   },
   "pg_expense_types_write": {
    "env": "KS_WRITE_EXPENSE_TYPES",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": null,
    "shadow": false,
    "unmet_precondition": null
   },
   "pg_expenses_write": {
    "env": "KS_WRITE_EXPENSES",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": null,
    "shadow": false,
    "unmet_precondition": null
   },
   "pg_forecast_write": {
    "env": "KS_WRITE_FORECAST",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": null,
    "shadow": false,
    "unmet_precondition": null
   },
   "pg_goals_write": {
    "env": "KS_WRITE_GOALS",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": null,
    "shadow": false,
    "unmet_precondition": null
   },
   "pg_inventory_write": {
    "env": "KS_WRITE_INVENTORY",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": {
     "ok": null,
     "reasons": [
      "chain 1 already writes Postgres; the preflight is the question asked before the flip"
     ]
    },
    "shadow": false,
    "unmet_precondition": null
   },
   "pg_managers_write": {
    "env": "KS_WRITE_MANAGERS",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": {
     "ok": null,
     "reasons": []
    },
    "shadow": false,
    "unmet_precondition": null
   },
   "pg_orders_write": {
    "env": "KS_WRITE_ORDERS",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": {
     "ok": null,
     "reasons": []
    },
    "shadow": false,
    "unmet_precondition": null
   },
   "pg_traffic_ledger_write": {
    "env": "KS_WRITE_TRAFFIC_LEDGER",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": null,
    "shadow": true,
    "unmet_precondition": null
   },
   "pg_watchdog_write": {
    "env": "KS_WRITE_WATCHDOGS",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": null,
    "shadow": true,
    "unmet_precondition": null
   },
   "pg_weekly_ledger_write": {
    "env": "KS_WRITE_WEEKLY_LEDGER",
    "error": null,
    "mismatch": false,
    "mode": "postgres",
    "preflight": null,
    "shadow": true,
    "unmet_precondition": null
   }
  }
 },
 "mirror": {
  "enabled": true,
  "mirror_on": true
 },
 "pg_derivation": {
  "error": null,
  "mode": "own",
  "owns": true
 },
 "pg_twins_flag": [
  true,
  null
 ],
 "pg_utm_parse": {
  "error": null,
  "mode": "postgres",
  "parses_in_postgres": true
 },
 "read_fallback": {
  "error": null,
  "misconfigured": [],
  "mode": "off",
  "no_engine": [],
  "refusing": true
 },
 "registry": {
  "compared_in_shadow": [],
  "mismatched_chains": [],
  "stood_down_sync_keys": [
   "last_sync_buyers",
   "last_sync_categories",
   "last_sync_expense_types",
   "last_sync_managers",
   "last_sync_meilisearch_pg",
   "last_sync_offers",
   "last_sync_orders",
   "last_sync_products",
   "last_sync_stocks"
  ],
  "stood_down_tables": [
   "app.buyer_gender",
   "app.data_dir_samples",
   "app.data_quality_diffs",
   "app.data_quality_issues",
   "app.data_quality_runs",
   "app.disk_samples",
   "app.growth_metrics",
   "app.inventory_history",
   "app.inventory_sku_history",
   "app.manager_classifications",
   "app.manual_expenses",
   "app.memory_samples",
   "app.order_backfill_misses",
   "app.revenue_goals",
   "app.revenue_predictions",
   "app.seasonal_indices",
   "app.sku_inventory_status",
   "app.stock_movements",
   "app.traffic_report_sends",
   "app.weekly_patterns",
   "app.weekly_report_sends",
   "bronze.buyer_contacts",
   "bronze.buyers",
   "bronze.categories",
   "bronze.expense_types",
   "bronze.expenses",
   "bronze.managers",
   "bronze.offer_stocks",
   "bronze.offers",
   "bronze.order_products",
   "bronze.orders",
   "bronze.products"
  ]
 },
 "stores": {
  "sms_store_is_postgres": true,
  "user_store_is_postgres": true
 },
 "warehouse_cutover": {
  "duckdb_derives": false,
  "error": null,
  "held": false,
  "held_for_s": null,
  "mode": "postgres",
  "preconditions_unmet": [],
  "reclassify_needed": false,
  "status": {
   "error": null,
   "held": false,
   "held_since": null,
   "mode": "postgres",
   "preconditions_unmet": [],
   "reclassify_needed": false,
   "stood_down_duckdb_checks": [
    "attribution_coverage",
    "fk_orphan_order_products_order_id",
    "gold_cell_values",
    "goods_shipped_without_sale",
    "headline_vs_line_items",
    "not_null_orders_ordered_at",
    "not_null_orders_source_id",
    "not_null_orders_status_id",
    "orders_without_line_items",
    "pk_uniqueness_order_products",
    "pk_uniqueness_orders",
    "silver_arc",
    "status_group_agreement",
    "value_domain_orders_source_id",
    "value_domain_orders_status_id"
   ],
   "switch_built": true,
   "value": "postgres",
   "variable": "KS_WRITE_WAREHOUSE",
   "way_back_refused": [],
   "writer": null
  },
  "stood_down_duckdb_checks": [
   "attribution_coverage",
   "fk_orphan_order_products_order_id",
   "gold_cell_values",
   "goods_shipped_without_sale",
   "headline_vs_line_items",
   "not_null_orders_ordered_at",
   "not_null_orders_source_id",
   "not_null_orders_status_id",
   "orders_without_line_items",
   "pk_uniqueness_order_products",
   "pk_uniqueness_orders",
   "silver_arc",
   "status_group_agreement",
   "value_domain_orders_source_id",
   "value_domain_orders_status_id"
  ],
  "value": "postgres",
  "verdict_reached": true,
  "warehouse_checks_stand_down": true,
  "way_back_refused": [],
  "writes_postgres": true
 }
}
""")

# S14-11's per-chain retirements, read off the literal: every routing
# answer under `chains` (chain 3's two stand-down sets went in S14-12 and
# S14-14) and every flag field of each `write_chains` entry.
for _name, _entry in EXPECTED["chains"].items():
    for _field in _entry:
        if _field not in ("stood_down_layers", "landing_checks_stood_down"):
            RETIRED_FIELDS.setdefault(("chains", _name, _field), "S14-11")
    for _field in _S14_11_HEALTH_FIELDS:
        RETIRED_FIELDS[("health", "write_chains", _name, _field)] = "S14-11"

# H0: what production's `/api/health` published on 2026-10-21, before S14-00.
H0 = json.loads(r"""
{
 "derivation": {
  "error": null,
  "mode": "own"
 },
 "duckdb_switch": {
  "error": null,
  "held_back": [],
  "mode": "off",
  "opened_while_off": {},
  "value": "off"
 },
 "goals_history": {
  "error": null,
  "mode": "silver"
 },
 "read_fallback_mode": {
  "error": null,
  "misconfigured": [],
  "mode": "off",
  "no_engine": []
 },
 "utm_parse": {
  "error": null,
  "mode": "postgres"
 },
 "warehouse_writer_mode": {
  "error": null,
  "held": false,
  "held_for_s": null,
  "mode": "postgres",
  "preconditions_unmet": [],
  "reclassify_needed": false,
  "value": "postgres",
  "way_back_refused": []
 },
 "write_chains": {
  "pg_buyers_write": {
   "env": "KS_WRITE_BUYERS",
   "error": null,
   "latched": true,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_catalogue_write": {
   "env": "KS_WRITE_CATALOGUE",
   "error": null,
   "latched": true,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_dq_journal_write": {
   "env": "KS_WRITE_DQ_JOURNAL",
   "error": null,
   "latched": true,
   "mismatch": false,
   "mode": "postgres",
   "shadow": true,
   "unmet_precondition": null
  },
  "pg_expense_types_write": {
   "env": "KS_WRITE_EXPENSE_TYPES",
   "error": null,
   "latched": true,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_expenses_write": {
   "env": "KS_WRITE_EXPENSES",
   "error": null,
   "latched": false,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_forecast_write": {
   "env": "KS_WRITE_FORECAST",
   "error": null,
   "latched": true,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_goals_write": {
   "env": "KS_WRITE_GOALS",
   "error": null,
   "latched": false,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_inventory_write": {
   "env": "KS_WRITE_INVENTORY",
   "error": null,
   "latched": true,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_managers_write": {
   "env": "KS_WRITE_MANAGERS",
   "error": null,
   "latched": true,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_orders_write": {
   "env": "KS_WRITE_ORDERS",
   "error": null,
   "latched": true,
   "mismatch": false,
   "mode": "postgres",
   "shadow": false,
   "unmet_precondition": null
  },
  "pg_traffic_ledger_write": {
   "env": "KS_WRITE_TRAFFIC_LEDGER",
   "error": null,
   "latched": true,
   "mismatch": false,
   "mode": "postgres",
   "shadow": true,
   "unmet_precondition": null
  },
  "pg_watchdog_write": {
   "env": "KS_WRITE_WATCHDOGS",
   "error": null,
   "latched": true,
   "mismatch": false,
   "mode": "postgres",
   "shadow": true,
   "unmet_precondition": null
  },
  "pg_weekly_ledger_write": {
   "env": "KS_WRITE_WEEKLY_LEDGER",
   "error": null,
   "latched": true,
   "mismatch": false,
   "mode": "postgres",
   "shadow": true,
   "unmet_precondition": null
  }
 }
}
""")


# ─── Preparing a process as production is ────────────────────────────────────

def _environment(junk=(), absent=()) -> dict:
    env = {**PROD_ENV, **ADDRESSES}
    for name in junk:
        env[name] = JUNK
    for name in absent:
        env.pop(name, None)
    return env


def _place_files(data: Path, markers: bool) -> None:
    """Fresh backup markers in `data`, and the chain markers when asked."""
    from core import backup_evidence, chain_latch
    from core.write_chains import WRITE_CHAINS, chain_name

    now = datetime.now(timezone.utc)
    for name, hours in BACKUP_AGES_H.items():
        (data / name).write_text((now - timedelta(hours=hours)).isoformat() + "\n")
    # `DATA_DIR` since S14-01 (`core.paths`); the capture also runs this
    # against trees from before it, where the name was `DB_DIR`.
    setattr(backup_evidence,
            "DATA_DIR" if hasattr(backup_evidence, "DATA_DIR") else "DB_DIR", data)
    chain_latch.MARKER_DIR.mkdir(parents=True, exist_ok=True)
    chain_latch._latched = None
    if markers:
        envs = {chain_name(c): c.WRITE_ENV for c in WRITE_CHAINS}
        for chain, stamp in PROD_MARKERS.items():
            (chain_latch.MARKER_DIR / chain).write_text(json.dumps(
                {"chain": chain, "env": envs[chain], "latched_at": stamp}))


def _stub_the_old_start() -> None:
    """The Postgres facts the start before S14-00 asked — step 13's revision
    and expenses history, off's frozen record, its copy and the owner rows —
    as production holds them. The code after S14-00 asks none of them, and
    has none of these to stub."""
    from core.pg import REQUIRED_REVISION

    try:
        from core import warehouse_cutover as wc   # deleted in S14-15
    except ImportError:
        wc = None
    if wc is not None and hasattr(wc, "_read_postgres_in_a_worker"):
        wc._read_postgres_in_a_worker = lambda env: {
            "revision": REQUIRED_REVISION, "revision_error": None,
            "expenses_backfilled": True, "expenses_backfill_error": None,
            "expenses_history_row": True, "unread": False}
    try:
        from core import duckdb_off
    except ImportError:
        return
    from core.write_chains import WRITE_CHAINS, chain_name

    owners = {}
    for chain in WRITE_CHAINS:
        stamp = PROD_MARKERS.get(chain_name(chain))
        if stamp:
            owners.update((table, datetime.fromisoformat(stamp))
                          for table in chain.CHAIN_TABLES)
    duckdb_off._read_postgres_in_a_worker = lambda dsn: {
        "writer": {"writer": "postgres", "since": "2026-10-09T08:18:00+00:00",
                   "resolved": True, "resolved_at": "2026-10-09T08:18:30+00:00"},
        "copied_at": datetime(2026, 10, 21, 6, 0, tzinfo=timezone.utc),
        "owners": owners, "pg_error": None, "unread": False}


# ─── What is captured ────────────────────────────────────────────────────────

def capture() -> dict:
    """Every mode and predicate the plan's S14-00 lists, as this process
    answers it after web's `configure_modes`. Only names both sides of
    S14-00 define."""
    import importlib
    import importlib.util
    import inspect

    from core import (pg_dashboard_users, pg_derivation,
                      pg_landing, pg_utm_parse, pg_warehouse_dq, read_fallback,
                      runtime_modes, write_chains)
    from core import pg_forecast_write, pg_managers_write, pg_orders_write

    try:
        from core import warehouse_cutover as wc   # deleted in S14-15
    except ImportError:
        wc = None

    out: dict = {}
    # `web=True` until S14-18, which deleted the switch it configured.
    web = ({"web": True} if "web" in inspect.signature(
        runtime_modes.configure_modes).parameters else {})
    out["configure_modes"] = runtime_modes.configure_modes(**web)
    # The scheduler configures again, in the same process.
    out["configure_modes_again"] = runtime_modes.configure_modes(**web)
    # Deleted in S14-18 (`RETIRED`), with their modules: asked by name where
    # they still exist — the import walk reads a literal module name, and
    # `module.<name>`, as a claim it exists.
    def _old(module: str):
        name = f"core.{module}"
        return (importlib.import_module(name)
                if importlib.util.find_spec(name) is not None else None)

    switch = _old("duckdb_switch")
    if switch is not None:
        out["duckdb_switch"] = {
            name: getattr(switch, fn)() for name, fn in (
                ("mode", "mode"), ("value", "value"), ("error", "mode_error"),
                ("is_off", "is_off"), ("health_block", "health_block"))}
        out["duckdb_switch"]["held_back"] = list(getattr(switch, "held_back")() or ())
    pg_sms = _old("pg_sms")
    # Deleted in S14-15 (`RETIRED`) with their modules or names; asked where
    # they still exist, by name, so the capture still runs against the trees
    # before it — the import walk reads `module.<name>` as a claim it exists.
    if wc is not None:
        out.update(_step13(wc))
    if hasattr(pg_derivation, "owns"):
        out["pg_derivation"] = {name: getattr(pg_derivation, fn)() for name, fn in (
            ("mode", "mode"), ("owns", "owns"), ("error", "mode_error"))}
    if hasattr(pg_utm_parse, "parses_in_postgres"):
        out["pg_utm_parse"] = {name: getattr(pg_utm_parse, fn)() for name, fn in (
            ("mode", "mode"), ("parses_in_postgres", "parses_in_postgres"),
            ("error", "mode_error"))}
    # Deleted in S14-16 (`RETIRED`), asked by name where it still exists.
    if hasattr(read_fallback, "misconfigured"):
        out["read_fallback"] = {name: getattr(read_fallback, fn)() for name, fn in (
            ("mode", "mode"), ("error", "mode_error"), ("refusing", "refusing"),
            ("misconfigured", "misconfigured"), ("no_engine", "no_engine_routes"))}
    twins_flag = getattr(pg_warehouse_dq, "flag", None)
    if twins_flag is not None:
        out["pg_twins_flag"] = list(twins_flag())
    if hasattr(pg_landing, "mirror_on"):
        out["mirror"] = {"enabled": getattr(pg_landing, "enabled")(),
                         "mirror_on": getattr(pg_landing, "mirror_on")(
                             os.environ.get("KS_MIRROR_LANDING"))}
    out.update(_after_step13(pg_dashboard_users, pg_sms, write_chains,
                             pg_forecast_write, pg_managers_write, pg_orders_write))
    return json.loads(json.dumps(out, default=str))


def _step13(wc) -> dict:
    """What `core.warehouse_cutover` answered, while it existed (S14-15)."""
    out: dict = {}
    status = wc.status()
    out["warehouse_cutover"] = {
        "mode": wc.mode(), "value": wc.value(), "error": wc.mode_error(),
        "writes_postgres": wc.writes_postgres(), "duckdb_derives": wc.duckdb_derives(),
        "preconditions_unmet": [u.key for u in wc.preconditions_unmet()],
        "way_back_refused": list(wc.way_back_refused()),
        "held": wc.held(), "held_for_s": wc.held_for_s(),
        "reclassify_needed": wc.reclassify_needed(),
        "verdict_reached": wc.verdict_reached(),
        "status": {k: status[k] for k in (
            "variable", "value", "mode", "error", "switch_built", "preconditions_unmet",
            "writer", "held", "held_since", "reclassify_needed",
            "stood_down_duckdb_checks", "way_back_refused") if k in status},
    }
    # Deleted in S14-14 (`RETIRED_FIELDS`, `RETIRED`); asked where they still
    # exist, by name, so the capture still runs against the trees before it.
    stood = getattr(wc, "stood_down_duckdb_checks", None)
    if stood is not None:
        out["warehouse_cutover"]["stood_down_duckdb_checks"] = sorted(stood())
    # Deleted in S14-12 (`RETIRED_FIELDS`); asked where it still exists, by
    # name, so the capture still runs against the trees before it.
    stand_down = getattr(wc, "warehouse_checks_stand_down", None)
    if stand_down is not None:
        out["warehouse_cutover"]["warehouse_checks_stand_down"] = stand_down()
    return out


def _after_step13(pg_dashboard_users, pg_sms, write_chains, pg_forecast_write,
                  pg_managers_write, pg_orders_write) -> dict:
    """The rest of `capture`: the store switches, the chains, the registry
    and `/api/health`."""
    out: dict = {}
    # Deleted in S14-18 (`RETIRED`); asked by name where they still exist.
    if pg_sms is not None and hasattr(pg_dashboard_users, "user_store_is_postgres"):
        out["stores"] = {
            "user_store_is_postgres": getattr(pg_dashboard_users, "user_store_is_postgres")(),
            "sms_store_is_postgres": getattr(pg_sms, "sms_store_is_postgres")()}
    chains = {}
    # Every routing answer went in S14-11 (`RETIRED_FIELDS`); asked where they
    # still exist, by name, so the capture still runs against the trees
    # before it — the import walk reads `module.<name>` as a claim it exists.
    for chain in write_chains.WRITE_CHAINS:
        name = write_chains.chain_name(chain)
        entry = {}
        if hasattr(chain, "writes_postgres"):
            entry["writes_postgres"] = getattr(chain, "writes_postgres")()
            entry["unmet_precondition"] = (getattr(chain, "unmet_precondition")()
                                           if hasattr(chain, "unmet_precondition")
                                           else "<none>")
        if hasattr(chain, "mode"):
            entry["mode"] = getattr(chain, "mode")()
        if hasattr(chain, "reads_postgres"):
            entry["reads_postgres"] = getattr(chain, "reads_postgres")()
        if hasattr(chain, "reads_the_chain"):
            holes = getattr(chain, "TABLE_HOLES", None) or (getattr(chain, "TABLE_HOLE"),)
            reads = getattr(chain, "reads_the_chain")
            entry["reads_the_chain"] = [reads(" ".join(holes)), reads("SELECT 1")]
        chains[name] = entry
    if hasattr(pg_forecast_write, "_routes_postgres"):
        chains["pg_forecast_write"]["_routes_postgres"] = getattr(
            pg_forecast_write, "_routes_postgres")(log=False)
    landing = getattr(pg_orders_write, "landing_checks_stood_down", None)
    if landing is not None:
        chains["pg_orders_write"]["landing_checks_stood_down"] = sorted(landing())
    # Deleted in S14-12 (`RETIRED_FIELDS`); asked where it still exists, so
    # the capture still runs against the trees before it.
    layers = getattr(pg_orders_write, "stood_down_layers", None)
    if layers is not None:
        chains["pg_orders_write"]["stood_down_layers"] = sorted(layers())
    # Deleted in S14-09 (`RETIRED_FIELDS`); asked where they still exist, so
    # the capture still runs against the trees before it.
    for name, field in (("unmet_reasons", "unmet_reasons"), ("settle_hold", "settle_hold"),
                        ("_held", "held")):
        if hasattr(pg_orders_write, name):
            value = getattr(pg_orders_write, name)
            chains["pg_orders_write"][field] = value() if callable(value) else value
    if hasattr(pg_managers_write, "unmet_reasons"):
        chains["pg_managers_write"]["unmet_reasons"] = getattr(
            pg_managers_write, "unmet_reasons")()
    out["chains"] = chains
    modes = getattr(write_chains, "chain_modes", None)
    if modes is not None:
        out["chain_modes"] = {
            name: {k: state[k] for k in ("env", "mode", "error", "latched", "latched_at",
                                         "mismatch", "unmet_precondition", "shadow")}
            for name, state in modes().items()}
    # Since S14-11 the stand-down sets are every chain's tables and keys,
    # asked by name — `stood_down_*` before it, `chain_tables`/`sync_keys`
    # after — and compared against the same literal.
    tables = (getattr(write_chains, "stood_down_tables", None)
              or getattr(write_chains, "chain_tables"))
    keys = (getattr(write_chains, "stood_down_sync_keys", None)
            or getattr(write_chains, "sync_keys"))
    out["registry"] = {"stood_down_tables": sorted(tables()),
                       "stood_down_sync_keys": sorted(keys())}
    mismatched = getattr(write_chains, "mismatched_chains", None)
    if mismatched is not None:
        out["registry"]["mismatched_chains"] = sorted(mismatched())
    # Deleted in S14-10 (`RETIRED_FIELDS`); asked where it still exists, so the
    # capture still runs against the trees before it. By name, as S14-09's
    # are: the import walk reads `write_chains.<name>` as a claim it exists.
    compared = getattr(write_chains, "compared_in_shadow", None)
    if compared is not None:
        out["registry"]["compared_in_shadow"] = sorted(compared())
    out["health"] = _health()
    return out


def _health() -> dict:
    """`/api/health`'s blocks S14-00 touches, from the route itself, and its
    status code."""
    from fastapi.testclient import TestClient

    from web.main import app
    from web.routes.api import health

    for value in vars(health).values():
        if isinstance(value, dict) and "expires_at" in value:
            value["expires_at"] = 0
    response = TestClient(app).get("/api/health")
    body = response.json()
    return {
        "status_code": response.status_code,
        "duckdb_switch": body.get("duckdb_switch"),
        "warehouse_writer_mode": body.get("warehouse_writer_mode"),
        # `mode` and `error` alone, and absent once they are (S14-15).
        "derivation": ({k: (body.get("derivation") or {}).get(k) for k in ("mode", "error")}
                       if "mode" in (body.get("derivation") or {}) else None),
        "utm_parse": body.get("utm_parse"),
        "read_fallback_mode": {k: (body.get("read_fallback_mode") or {}).get(k)
                               for k in ("mode", "error", "misconfigured", "no_engine")},
        "goals_history": body.get("goals_history"),
        "write_chains": {
            name: {k: entry.get(k) for k in ("env", "mode", "error", "latched", "mismatch",
                                             "unmet_precondition", "shadow", "preflight")}
            for name, entry in (body.get("write_chains") or {}).items()},
    }


def _latched_out(result: dict) -> dict:
    """`result` without what depends on the markers, and that, apart. Empty
    from S14-11 on: nothing published depends on them any more."""
    latched = {}
    for name, state in result.get("chain_modes", {}).items():
        latched[name] = (state.pop("latched"), state.pop("latched_at"),
                         result["health"]["write_chains"][name].pop("latched"))
    return latched


def _constants(result: dict) -> dict:
    """`result` without what is published as read — a variable's value, a
    chain flag's error and the mismatch it makes with a latch — which a junk
    value changes by design (rule 3: the answer must not)."""
    result = _without_retired(result)
    # The switch's blocks published `value` as read until S14-18 retired them.
    switch = result.get("duckdb_switch") or {}
    for block in (switch, switch.get("health_block") or {},
                  result["health"].get("duckdb_switch") or {}):
        block.pop("value", None)
    for state in (*result.get("chain_modes", {}).values(),
                  *result["health"]["write_chains"].values()):
        state.pop("error", None)
        state.pop("mismatch", None)
    result["registry"].pop("mismatched_chains", None)
    return result


# ─── As a script: the capture ────────────────────────────────────────────────

def main(argv) -> int:
    """`<11|none> <out.json> [--junk|--absent]`, run from a checkout's root.
    `--junk` sets every variable S14-00 stopped reading to junk at once,
    `--absent` removes them all."""
    markers, out_path, *flags = argv[1:]
    sys.path.insert(0, os.getcwd())
    junk = STOPPED_READING if "--junk" in flags else ()
    absent = STOPPED_READING if "--absent" in flags else ()
    for name in (*PROD_ENV, *STOPPED_READING):
        os.environ.pop(name, None)
    os.environ.update({"DASHBOARD_SECRET_KEY": "s14-capture", "BOT_TOKEN": "1:s14-capture",
                       "KEYCRM_API_KEY": "s14-capture", "KS_ALERTS_DISABLED": "1",
                       "KS_ALERT_GATE_STATE_DIR": "", "KS_INSTANCE": "s14-capture"})
    os.environ.update(_environment(junk=junk, absent=absent))
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp) / "data"
        data.mkdir()
        from core import chain_latch, duckdb_store

        # The DuckDB store's file, where the tree still has one (until S14-18).
        duckdb_store.DB_DIR, duckdb_store.DB_PATH = data, data / "analytics.duckdb"
        chain_latch.MARKER_DIR = data / "write-chain-owners"
        _place_files(data, markers == "11")
        _stub_the_old_start()
        result = capture()
    Path(out_path).write_text(json.dumps(result, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))


# ─── The tests ───────────────────────────────────────────────────────────────

import pytest  # noqa: E402 — the script above runs without the suite


@pytest.fixture
def production(monkeypatch, tmp_path):
    """This process as production's web: its environment and the backup
    markers; the chain markers are each test's to place."""
    from core import backup_evidence, duckdb_store

    def prepare(*, markers: bool, junk=(), absent=()):
        for name in (*PROD_ENV, *STOPPED_READING):
            monkeypatch.delenv(name, raising=False)
        for name, value in _environment(junk=junk, absent=absent).items():
            monkeypatch.setenv(name, value)
        data = tmp_path / "data"
        data.mkdir(exist_ok=True)
        # The DuckDB store held a file until S14-18; set where it still does.
        monkeypatch.setattr(duckdb_store, "DB_DIR", data, raising=False)
        monkeypatch.setattr(duckdb_store, "DB_PATH", data / "analytics.duckdb",
                            raising=False)
        monkeypatch.setattr(backup_evidence, "DATA_DIR", data)
        _place_files(data, markers)

    return prepare


@pytest.mark.parametrize("markers", [True, False], ids=["eleven-markers", "no-markers"])
def test_every_answer_is_what_the_code_before_s14_00_gave(production, markers):
    """Mutation: any constant S14-00 introduced answering other than
    production did — a chain `duckdb`, step 13 not in force, the fallback
    on, web `on`, a precondition named — and, with no markers, any answer
    that still follows them."""
    production(markers=markers)
    result = capture()
    latched = _latched_out(result)
    assert _without_retired(result) == _without_retired(EXPECTED)
    # Since S14-11 nothing published depends on the markers.
    assert latched == {}


def test_the_literal_is_what_production_published(production):
    """The capture against the live snapshot taken before S14-00 (H0): the
    health blocks S14-00 touches, field by field. Mutation: a stub in the
    capture that made the old code answer what production did not."""
    production(markers=True)
    health = capture()["health"]
    assert health["status_code"] == 200
    kept = _without_retired({"health": health})["health"]
    live = _without_retired({"health": H0})["health"]
    for block in ("duckdb_switch", "read_fallback_mode"):
        if block not in RETIRED:
            assert kept[block] == live[block], block
    for block in RETIRED:
        assert health.get(block) is None, f"{block} went with {RETIRED[block]}"
    for path, pr in RETIRED_FIELDS.items():
        if path[0] == "health":
            node = health
            for step in path[1:-1]:
                node = node[step]
            assert node.get(path[-1]) is None, f"{'.'.join(path)} went with {pr}"
    for name, entry in H0["write_chains"].items():
        published = health["write_chains"][name]
        kept = {k: v for k, v in entry.items()
                if ("health", "write_chains", name, k) not in RETIRED_FIELDS}
        assert {k: published[k] for k in kept} == kept, name


@pytest.mark.parametrize("how", ["junk", "absent"])
@pytest.mark.parametrize("name", STOPPED_READING)
def test_a_variable_it_stopped_reading_changes_no_answer(production, name, how):
    """Rule 3: junk, or nothing, in a variable S14-00 stopped reading — the
    modes configure without a raise, `/api/health` answers 200, and every
    constant is what production's values give. An absent value never means
    `duckdb` (the images before step 14 read an absent KS_USER_STORE so: a
    401 for everybody). Mutation: read any of them for a decision again."""
    production(markers=True, **{how: (name,)})
    result = capture()
    _latched_out(result)
    assert result["health"]["status_code"] == 200
    assert _constants(result) == _constants(EXPECTED)


@pytest.mark.parametrize("flag", ["--junk", "--absent"])
def test_a_fresh_interpreter_imports_and_starts_on_every_one_at_once(tmp_path, flag):
    """The same, every variable at once, in an interpreter of its own: what
    is read at import time is read there."""
    out = tmp_path / "out.json"
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path),
           "TZ": "UTC"}
    run = subprocess.run([sys.executable, "-I", str(Path(__file__)), "11", str(out), flag],
                         cwd=ROOT, env=env, capture_output=True, text=True, timeout=180)
    assert run.returncode == 0, run.stderr[-3000:]
    result = json.loads(out.read_text())
    _latched_out(result)
    assert result["health"]["status_code"] == 200
    assert _constants(result) == _constants(EXPECTED)
