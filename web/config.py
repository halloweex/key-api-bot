"""
Web dashboard configuration.
"""
import os
from pathlib import Path

# Web server settings
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "8080"))

# Paths
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# Import shared config from bot
from bot.config import (
    KEYCRM_API_KEY,
    SOURCE_MAPPING,
    SOURCE_EMOJIS,
    DEFAULT_TIMEZONE,
    VERSION,
)
