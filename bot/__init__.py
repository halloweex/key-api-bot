"""
KeyCRM Telegram Bot - Refactored Version

A well-organized Telegram bot for generating sales reports from KeyCRM data.

Package structure:
- config.py: Configuration and constants
- keyboards.py: Telegram keyboard builders
- formatters.py: Message formatting utilities
- services.py: Business logic for sales reporting
- handlers_legacy.py: Telegram bot handlers (handlers/ is a re-export facade)
- main.py: Application entry point

Note: Uses core.keycrm.SyncKeyCRMClient for API calls (unified client).

`main` is deliberately not re-exported here. Importing it at package level made
every `from bot.x import y` — and every `import core.z` that transitively
touched this package — construct the whole Telegram application: ~1000 modules
instead of ~60. The entry point is `python -m bot.main`, which is what the
Dockerfile runs.
"""

__version__ = "2.0.0"
__author__ = "KeyCRM Bot Team"

from bot.config import BOT_TOKEN, KEYCRM_API_KEY, DEFAULT_TIMEZONE
from core.keycrm import SyncKeyCRMClient as KeyCRMClient
from bot.services import ReportService, KeyCRMAPIError, ReportGenerationError
from bot.database import init_database, get_user_preferences, save_user_preferences

__all__ = [
    # Configuration
    "BOT_TOKEN",
    "KEYCRM_API_KEY",
    "DEFAULT_TIMEZONE",
    # API Client
    "KeyCRMClient",
    # Services
    "ReportService",
    # Exceptions
    "KeyCRMAPIError",
    "ReportGenerationError",
    # Database
    "init_database",
    "get_user_preferences",
    "save_user_preferences",
]
