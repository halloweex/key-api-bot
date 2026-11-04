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
RETIRED = {"goals_history": "S14-07"}


def _without_retired(result: dict) -> dict:
    """`result`, a capture or a literal, without the blocks `RETIRED` names."""
    result = json.loads(json.dumps(result))
    for key in RETIRED:
        result.pop(key, None)
        result.get("health", {}).pop(key, None)
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
    from core import warehouse_cutover as wc
    from core.pg import REQUIRED_REVISION

    if hasattr(wc, "_read_postgres_in_a_worker"):
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
    from core import (duckdb_switch, pg_dashboard_users, pg_derivation,
                      pg_landing, pg_sms, pg_utm_parse, pg_warehouse_dq, read_fallback,
                      runtime_modes, warehouse_cutover as wc, write_chains)
    from core import pg_forecast_write, pg_managers_write, pg_orders_write

    out: dict = {}
    out["configure_modes"] = runtime_modes.configure_modes(web=True)
    # The scheduler configures again, in the same process.
    out["configure_modes_again"] = runtime_modes.configure_modes(web=True)
    out["duckdb_switch"] = {
        "mode": duckdb_switch.mode(), "value": duckdb_switch.value(),
        "error": duckdb_switch.mode_error(), "is_off": duckdb_switch.is_off(),
        "held_back": list(duckdb_switch.held_back() or ()),
        "health_block": duckdb_switch.health_block(),
    }
    status = wc.status()
    out["warehouse_cutover"] = {
        "mode": wc.mode(), "value": wc.value(), "error": wc.mode_error(),
        "writes_postgres": wc.writes_postgres(), "duckdb_derives": wc.duckdb_derives(),
        "warehouse_checks_stand_down": wc.warehouse_checks_stand_down(),
        "stood_down_duckdb_checks": sorted(wc.stood_down_duckdb_checks()),
        "preconditions_unmet": [u.key for u in wc.preconditions_unmet()],
        "way_back_refused": list(wc.way_back_refused()),
        "held": wc.held(), "held_for_s": wc.held_for_s(),
        "reclassify_needed": wc.reclassify_needed(),
        "verdict_reached": wc.verdict_reached(),
        "status": {k: status[k] for k in (
            "variable", "value", "mode", "error", "switch_built", "preconditions_unmet",
            "writer", "held", "held_since", "reclassify_needed",
            "stood_down_duckdb_checks", "way_back_refused")},
    }
    out["pg_derivation"] = {"mode": pg_derivation.mode(), "owns": pg_derivation.owns(),
                            "error": pg_derivation.mode_error()}
    out["pg_utm_parse"] = {"mode": pg_utm_parse.mode(),
                           "parses_in_postgres": pg_utm_parse.parses_in_postgres(),
                           "error": pg_utm_parse.mode_error()}
    out["read_fallback"] = {"mode": read_fallback.mode(), "error": read_fallback.mode_error(),
                            "refusing": read_fallback.refusing(),
                            "misconfigured": read_fallback.misconfigured(),
                            "no_engine": read_fallback.no_engine_routes()}
    out["pg_twins_flag"] = list(pg_warehouse_dq.flag())
    out["mirror"] = {"enabled": pg_landing.enabled(),
                     "mirror_on": pg_landing.mirror_on(os.environ.get(pg_landing.MIRROR_ENV))}
    out["stores"] = {"user_store_is_postgres": pg_dashboard_users.user_store_is_postgres(),
                     "sms_store_is_postgres": pg_sms.sms_store_is_postgres()}
    chains = {}
    for chain in write_chains.WRITE_CHAINS:
        name = write_chains.chain_name(chain)
        entry = {"writes_postgres": chain.writes_postgres(),
                 "unmet_precondition": chain.unmet_precondition()
                 if hasattr(chain, "unmet_precondition") else "<none>"}
        if hasattr(chain, "mode"):
            entry["mode"] = chain.mode()
        if hasattr(chain, "reads_postgres"):
            entry["reads_postgres"] = chain.reads_postgres()
        if hasattr(chain, "reads_the_chain"):
            holes = getattr(chain, "TABLE_HOLES", None) or (getattr(chain, "TABLE_HOLE"),)
            entry["reads_the_chain"] = [chain.reads_the_chain(" ".join(holes)),
                                        chain.reads_the_chain("SELECT 1")]
        chains[name] = entry
    chains["pg_forecast_write"]["_routes_postgres"] = pg_forecast_write._routes_postgres(log=False)
    chains["pg_orders_write"].update(
        stood_down_layers=sorted(pg_orders_write.stood_down_layers()),
        landing_checks_stood_down=sorted(pg_orders_write.landing_checks_stood_down()),
        unmet_reasons=pg_orders_write.unmet_reasons(),
        settle_hold=pg_orders_write.settle_hold(),
        held=pg_orders_write._held)
    chains["pg_managers_write"]["unmet_reasons"] = pg_managers_write.unmet_reasons()
    out["chains"] = chains
    out["chain_modes"] = {
        name: {k: state[k] for k in ("env", "mode", "error", "latched", "latched_at",
                                     "mismatch", "unmet_precondition", "shadow")}
        for name, state in write_chains.chain_modes().items()}
    out["registry"] = {
        "compared_in_shadow": sorted(write_chains.compared_in_shadow()),
        "stood_down_tables": sorted(write_chains.stood_down_tables()),
        "stood_down_sync_keys": sorted(write_chains.stood_down_sync_keys()),
        "mismatched_chains": sorted(write_chains.mismatched_chains()),
    }
    out["health"] = _health()
    return json.loads(json.dumps(out, default=str))


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
        "derivation": {k: (body.get("derivation") or {}).get(k) for k in ("mode", "error")},
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
    """`result` without what depends on the markers, and that, apart."""
    latched = {}
    for name, state in result["chain_modes"].items():
        latched[name] = (state.pop("latched"), state.pop("latched_at"),
                         result["health"]["write_chains"][name].pop("latched"))
    return latched


def _constants(result: dict) -> dict:
    """`result` without what is published as read — a variable's value, a
    chain flag's error and the mismatch it makes with a latch — which a junk
    value changes by design (rule 3: the answer must not)."""
    result = _without_retired(result)
    for block in (result["duckdb_switch"], result["duckdb_switch"]["health_block"],
                  result["health"]["duckdb_switch"], result["warehouse_cutover"],
                  result["warehouse_cutover"]["status"],
                  result["health"]["warehouse_writer_mode"]):
        block.pop("value", None)
    for state in (*result["chain_modes"].values(),
                  *result["health"]["write_chains"].values()):
        state.pop("error", None)
        state.pop("mismatch", None)
    result["registry"].pop("mismatched_chains")
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
        monkeypatch.setattr(duckdb_store, "DB_DIR", data)
        monkeypatch.setattr(duckdb_store, "DB_PATH", data / "analytics.duckdb")
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
    for name, (flag, at, published) in latched.items():
        stamp = PROD_MARKERS.get(name) if markers else None
        assert (flag, at, published) == (stamp is not None, stamp, stamp is not None), name


def test_the_literal_is_what_production_published(production):
    """The capture against the live snapshot taken before S14-00 (H0): the
    health blocks S14-00 touches, field by field. Mutation: a stub in the
    capture that made the old code answer what production did not."""
    production(markers=True)
    health = capture()["health"]
    assert health["status_code"] == 200
    for block in ("duckdb_switch", "warehouse_writer_mode", "derivation", "utm_parse",
                  "read_fallback_mode"):
        assert health[block] == H0[block], block
    for block in RETIRED:
        assert health[block] is None, f"{block} went with {RETIRED[block]}"
    for name, entry in H0["write_chains"].items():
        published = health["write_chains"][name]
        assert {k: published[k] for k in entry} == entry, name


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
