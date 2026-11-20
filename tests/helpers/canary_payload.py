"""A healthy /api/health payload and a transport that serves it: what the
canary's tests start from and break one key of.
"""
from __future__ import annotations

import httpx


DASHBOARD = "https://ksanalytics.duckdns.org"


def _mock_transport(handler):
    """Build an httpx.AsyncClient backed by a MockTransport for handler."""
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _healthy_payload():
    return {
        "status": "healthy",
        "version": "1.2.3",
        "uptime_seconds": 100,
        # No `duckdb` block and no ledger since step 14's S14-17.
        "degraded_by": [],
        "sync": {"status": "active", "seconds_since_sync": 30},
        "data_quality": {
            "integrity": {"last_success_at": "2026-08-08T19:00:00+03:00", "age_seconds": 1800},
            "mirror_landing": {"last_success_at": "2026-08-08T07:30:00+03:00", "age_seconds": 43200},
            # The 05:30 job's Postgres half (DN-21); its DuckDB half,
            # `reconciliation`, went in step 14's S14-12.
            "reconciliation_pg": {"last_success_at": "2026-08-08T05:30:00+03:00", "age_seconds": 50400},
            # …and its ClickHouse half (OD-08 (a)).
            "reconciliation_ch": {"last_success_at": "2026-08-08T05:30:00+03:00", "age_seconds": 50400},
        },
        # The Postgres copy of landing. Absent, `run_canary` reports
        # `mirror_block_missing` — that is the point of the block, and it is
        # exercised in tests/unit/test_mirror_freshness.py.
        "mirrors": {
            "bronze.orders": {
                "last_ok_at": "2026-08-08T19:25:00+03:00",
                "age_seconds": 300,
                "failures_since_ok": 0,
                "failing": False,
            },
        },
        # The alerting watching itself (step 03). Absent → failure, rule 3.
        "alerting": {
            "consecutive_transport_failures": 0,
            "last_delivery_at": 1723900000.0,
        },
    }
