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

# ─── Pagination Settings ────────────────────────────────────────────────────
API_PAGE_LIMIT = 50  # KeyCRM API page limit
API_REQUEST_DELAY = 0.5  # Delay between API calls in seconds

# ─── Medal Emojis for TOP-10 ────────────────────────────────────────────────
MEDALS = ["🥇", "🥈", "🥉"]
