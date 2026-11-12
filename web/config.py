"""
Web dashboard configuration.
"""
from pathlib import Path


# Paths
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
STATIC_V2_DIR = BASE_DIR / "static-v2"  # React build output

# Shared with the bot: web/main.py and /api/health read it from here.
from bot.config import VERSION

__all__ = ["BASE_DIR", "TEMPLATES_DIR", "STATIC_DIR", "STATIC_V2_DIR", "VERSION"]
