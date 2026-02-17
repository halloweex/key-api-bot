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

# Retail manager IDs (including historical managers who left: 8, 11, 17, 19)
RETAIL_MANAGER_IDS = [4, 8, 11, 16, 17, 19, 22]

# Timezone for date extraction - KeyCRM stores timestamps in +04:00 (server time)
# but UI displays in Kyiv timezone, so we convert for consistency
DISPLAY_TIMEZONE = 'Europe/Kyiv'


def _date_in_kyiv(column: str) -> str:
    """Generate SQL for extracting date in Kyiv timezone."""
    return f"DATE(timezone('{DISPLAY_TIMEZONE}', {column}))"
