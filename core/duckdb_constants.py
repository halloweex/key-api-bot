"""Shared constants and helpers for DuckDB store and repository mixins."""
from pathlib import Path
from zoneinfo import ZoneInfo

from bot.config import DEFAULT_TIMEZONE

# Database configuration
DB_DIR = Path(__file__).parent.parent / "data"
DB_PATH = DB_DIR / "analytics.duckdb"
DEFAULT_TZ = ZoneInfo(DEFAULT_TIMEZONE)

# Query timeout settings
DEFAULT_QUERY_TIMEOUT = 30.0  # seconds
LONG_QUERY_TIMEOUT = 120.0   # for sync operations

# B2B (wholesale) manager ID - Olga D
B2B_MANAGER_ID = 15

# Retail manager IDs (including historical managers who left: 8, 11, 17, 19).
# Seeds `managers.is_retail` for managers the warehouse has never seen; the
# stored value wins from then on, so a human's classification survives a sync.
RETAIL_MANAGER_IDS = [4, 8, 11, 16, 17, 19, 22]

# Every value the sales_type partition may take. The Silver CASE ends in
# ELSE 'internal', so the partition is exhaustive by construction — and that is
# exactly what has to be asserted, because nothing else would notice if a
# future edit added a fourth value. Such a value would land in Gold, keep the
# revenue checksum balanced, and appear on no page: every endpoint defaults to
# Query("retail"). It would vanish from the views and the checksums at once.
#
# `internal` was called `other` until 2026-08-09. Retail is a fixed list of
# retail managers and b2b is the wholesale manager; nobody else may be mixed
# into either. What was left was a junk drawer with a junk name, holding two
# unrelated things at once — genuine sales by staff outside those two roles,
# and goods shipped to bloggers with no sale at all. Naming it does not
# separate them, but it stops the bucket pretending to be an error.
KNOWN_SALES_TYPES = ("retail", "b2b", "internal")

# Timezone for date extraction - KeyCRM stores timestamps in +04:00 (server time)
# but UI displays in Kyiv timezone, so we convert for consistency
DISPLAY_TIMEZONE = 'Europe/Kyiv'


def _date_in_kyiv(column: str) -> str:
    """Generate SQL for extracting date in Kyiv timezone."""
    return f"DATE(timezone('{DISPLAY_TIMEZONE}', {column}))"
