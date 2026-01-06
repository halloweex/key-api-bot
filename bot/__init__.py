"""
KeyCRM Telegram Bot - Refactored Version

A well-organized Telegram bot for generating sales reports from KeyCRM data.

Package structure:
- config.py: Configuration and constants
- keyboards.py: Telegram keyboard builders
- formatters.py: Message formatting utilities
- api_client.py: Pure KeyCRM HTTP API client
- services.py: Business logic for sales reporting
- handlers.py: Telegram bot handlers
- main.py: Application entry point
"""

__version__ = "2.0.0"
__author__ = "KeyCRM Bot Team"

from bot.config import BOT_TOKEN, KEYCRM_API_KEY
from bot.api_client import KeyCRMClient
from bot.services import ReportService
from bot.main import main

__all__ = [
    "BOT_TOKEN",
    "KEYCRM_API_KEY",
    "KeyCRMClient",
    "ReportService",
    "main",
]
