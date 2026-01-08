"""
Configuration module for KeyCRM Telegram Bot.

Contains all constants, environment variables, and configuration settings.
"""
import os
from enum import IntEnum
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ─── Environment Variables ──────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN")
KEYCRM_API_KEY = os.getenv("KEYCRM_API_KEY")

# ─── Authorization ─────────────────────────────────────────────────────────
# Admin user IDs who can approve/deny access requests
# Example: ADMIN_USER_IDS=123456789,987654321
_admin_users_str = os.getenv("ADMIN_USER_IDS", "")
ADMIN_USER_IDS: set[int] = set(
    int(uid.strip()) for uid in _admin_users_str.split(",") if uid.strip().isdigit()
)

def is_admin(user_id: int) -> bool:
    """Check if user is an admin."""
    return user_id in ADMIN_USER_IDS

# ─── API Configuration ──────────────────────────────────────────────────────
KEYCRM_BASE_URL = "https://openapi.keycrm.app/v1"

# ─── Timezone Configuration ─────────────────────────────────────────────────
DEFAULT_TIMEZONE = "Europe/Kyiv"

# ─── Manager IDs ────────────────────────────────────────────────────────────
TELEGRAM_MANAGER_IDS = ['19', '22', '4', '16']

# ─── Status IDs ─────────────────────────────────────────────────────────────
RETURN_STATUS_IDS = [19, 22, 21, 23]  # Returned/Canceled orders

# ─── Source Mapping ─────────────────────────────────────────────────────────
SOURCE_MAPPING = {
    1: 'Instagram',
    2: 'Telegram',
    3: 'Opencart',
    4: 'Shopify'
}

# ─── Conversation States ────────────────────────────────────────────────────
class ConversationState(IntEnum):
    """Telegram bot conversation states."""
    SELECTING_REPORT_TYPE = 0
    SELECTING_DATE_RANGE = 1
    SELECTING_CUSTOM_START_YEAR = 2
    SELECTING_CUSTOM_START_MONTH = 3
    SELECTING_CUSTOM_START_DAY = 4
    SELECTING_CUSTOM_END_YEAR = 5
    SELECTING_CUSTOM_END_MONTH = 6
    SELECTING_CUSTOM_END_DAY = 7
    GENERATING_REPORT = 8
    SELECTING_TOP10_SOURCE = 9

# ─── Report Types ───────────────────────────────────────────────────────────
REPORT_TYPES = {
    "summary": "📊 Summary Report",
    "excel": "📑 Excel Report",
    "top10": "🏆 TOP-10 Products"
}

# ─── Date Ranges ────────────────────────────────────────────────────────────
DATE_RANGES = {
    "today": "📅 Today",
    "yesterday": "📅 Yesterday",
    "thisweek": "📆 This Week",
    "thismonth": "📆 This Month",
    "custom": "🗓️ Custom Date Range"
}

# ─── Source Names with Emojis ───────────────────────────────────────────────
SOURCE_NAMES = {
    "1": "Instagram",
    "2": "Telegram",
    "4": "Shopify",
    "all": "All Sources"
}

SOURCE_EMOJIS = {
    1: "📸",
    2: "✈️",
    3: "🌐",
    4: "🛍️"
}

# Sources available for TOP-10 reports (id, name, emoji)
TOP10_SOURCES = [
    (1, "Instagram", "📸"),
    (4, "Shopify", "🛍️"),
    (2, "Telegram", "✈️")
]

# ─── Pagination Settings ────────────────────────────────────────────────────
API_PAGE_LIMIT = 50  # KeyCRM API page limit
API_REQUEST_DELAY = 0.3  # Delay between API calls in seconds

# ─── Order Sync Buffer ─────────────────────────────────────────────────────
# Buffer time (hours) added to API queries to catch orders with delayed sync.
# Some integrations (Shopify, Opencart) may have delays between when an order
# is placed (ordered_at) and when it syncs to KeyCRM (created_at).
# We filter by created_at in API but then filter by ordered_at locally.
ORDER_SYNC_BUFFER_HOURS = 24

# ─── Date Picker Settings ──────────────────────────────────────────────────
YEAR_RANGE_PAST = 2  # How many years back to show in date picker


def get_year_choices(from_year: int = None) -> list:
    """
    Get list of years for date picker.

    Args:
        from_year: Optional start year. If None, uses current_year - YEAR_RANGE_PAST

    Returns:
        List of years from from_year to current year
    """
    from datetime import datetime
    current_year = datetime.now().year
    start_year = from_year if from_year else current_year - YEAR_RANGE_PAST
    return list(range(start_year, current_year + 1))


# ─── Medal Emojis for TOP-10 ────────────────────────────────────────────────
MEDALS = ["🥇", "🥈", "🥉"]
