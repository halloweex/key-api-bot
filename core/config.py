"""
Centralized configuration for KoreanStory Sales Bot.

This module provides a single source of truth for all configuration values.
Configuration is loaded from environment variables with sensible defaults.

Usage:
    from core.config import config

    api_key = config.api.key
    cache_ttl = config.cache.ttl_seconds
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass(frozen=True)
class APIConfig:
    """KeyCRM API configuration."""

    base_url: str = "https://openapi.keycrm.app/v1"
    key: str = field(default_factory=lambda: os.getenv("KEYCRM_API_KEY", ""))
    page_limit: int = 50
    request_timeout: int = 30
    rate_limit_delay: float = 0.3
    order_sync_buffer_hours: int = 24


@dataclass(frozen=True)
class CacheConfig:
    """Caching configuration."""

    ttl_seconds: int = 300  # 5 minutes
    warming_interval_seconds: int = 240  # 4 minutes
    warming_batch_size: int = 4


@dataclass(frozen=True)
class BotConfig:
    """Telegram bot configuration."""

    token: str = field(default_factory=lambda: os.getenv("BOT_TOKEN", ""))
    username: str = field(default_factory=lambda: os.getenv("BOT_USERNAME", "ksorderbot"))

    @property
    def admin_user_ids(self) -> Set[int]:
        """Parse admin user IDs from environment variable."""
        admin_str = os.getenv("ADMIN_USER_IDS", "")
        return {
            int(uid.strip())
            for uid in admin_str.split(",")
            if uid.strip().isdigit()
        }

    def is_admin(self, user_id: int) -> bool:
        """Check if user is an admin."""
        return user_id in self.admin_user_ids


@dataclass(frozen=True)
class WebConfig:
    """Web dashboard configuration."""

    dashboard_url: str = field(
        default_factory=lambda: os.getenv("DASHBOARD_URL", "http://108.130.86.30")
    )
    host: str = "0.0.0.0"
    port: int = 8080

    # Rate limiting
    rate_limit_per_minute: int = 30
    rate_limit_burst: int = 10


@dataclass(frozen=True)
class SourceConfig:
    """Source (channel) configuration."""

    # Source ID to name mapping
    mapping: Dict[int, str] = field(default_factory=lambda: {
        1: "Instagram",
        2: "Telegram",
        3: "Opencart",
        4: "Shopify",
    })

    # Emojis for each source
    emojis: Dict[int, str] = field(default_factory=lambda: {
        1: "📸",
        2: "✈️",
        3: "🌐",
        4: "🛍️",
    })

    # Colors for charts
    colors: Dict[int, str] = field(default_factory=lambda: {
        1: "#7C3AED",  # Instagram - purple
        2: "#2563EB",  # Telegram - blue
        3: "#F59E0B",  # Opencart - orange
        4: "#eb4200",  # Shopify - orange-red
    })

    # Sources included in dashboard visualizations (excludes deprecated Opencart)
    dashboard_sources: List[int] = field(default_factory=lambda: [1, 2, 4])

    # Sources available for TOP-10 reports
    top10_sources: List[tuple] = field(default_factory=lambda: [
        (1, "Instagram", "📸"),
        (4, "Shopify", "🛍️"),
        (2, "Telegram", "✈️"),
    ])

    def get_name(self, source_id: int, default: str = "Unknown") -> str:
        """Get source name by ID."""
        return self.mapping.get(source_id, default)

    def get_emoji(self, source_id: int, default: str = "") -> str:
        """Get source emoji by ID."""
        return self.emojis.get(source_id, default)

    def get_color(self, source_id: int, default: str = "#999999") -> str:
        """Get source color by ID."""
        return self.colors.get(source_id, default)


@dataclass(frozen=True)
class OrderConfig:
    """Order processing configuration."""

    # Status IDs for returned/canceled orders
    return_status_ids: List[int] = field(default_factory=lambda: [19, 22, 21, 23])

    # Telegram manager IDs to include
    telegram_manager_ids: List[str] = field(default_factory=lambda: ["19", "22", "4", "16"])

    # Default timezone for date calculations
    default_timezone: str = "Europe/Kyiv"


@dataclass(frozen=True)
class MilestoneConfig:
    """Revenue milestone configuration for celebrations."""

    milestones: Dict[str, List[Dict]] = field(default_factory=lambda: {
        "daily": [
            {"amount": 200000, "message": "200K Daily Revenue!", "emoji": "🎉"},
        ],
        "weekly": [
            {"amount": 800000, "message": "800K Weekly Revenue!", "emoji": "🔥"},
            {"amount": 1000000, "message": "1 MILLION Weekly!", "emoji": "🚀🎆"},
            {"amount": 2000000, "message": "2 MILLION Weekly!", "emoji": "💎🎇"},
        ],
        "monthly": [
            {"amount": 10000000, "message": "10 MILLION Monthly!", "emoji": "👑🎇🎆"},
        ],
    })

    def get_milestones(self, period_type: str) -> List[Dict]:
        """Get milestones for a period type."""
        return self.milestones.get(period_type, [])


@dataclass(frozen=True)
class AppConfig:
    """Main application configuration."""

    version: str = "1.2.0"
    api: APIConfig = field(default_factory=APIConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    bot: BotConfig = field(default_factory=BotConfig)
    web: WebConfig = field(default_factory=WebConfig)
    sources: SourceConfig = field(default_factory=SourceConfig)
    orders: OrderConfig = field(default_factory=OrderConfig)
    milestones: MilestoneConfig = field(default_factory=MilestoneConfig)


# Global config instance
config = AppConfig()


# ─── Convenience Exports (for backwards compatibility) ────────────────────────
VERSION = config.version
KEYCRM_API_KEY = config.api.key
KEYCRM_BASE_URL = config.api.base_url
BOT_TOKEN = config.bot.token
BOT_USERNAME = config.bot.username
ADMIN_USER_IDS = config.bot.admin_user_ids
DASHBOARD_URL = config.web.dashboard_url
DEFAULT_TIMEZONE = config.orders.default_timezone
SOURCE_MAPPING = config.sources.mapping
SOURCE_COLORS = config.sources.colors
RETURN_STATUS_IDS = config.orders.return_status_ids
TELEGRAM_MANAGER_IDS = config.orders.telegram_manager_ids
API_PAGE_LIMIT = config.api.page_limit
API_REQUEST_DELAY = config.api.rate_limit_delay
CACHE_TTL_SECONDS = config.cache.ttl_seconds


def is_admin(user_id: int) -> bool:
    """Check if user is an admin (backwards compatibility)."""
    return config.bot.is_admin(user_id)


class ConfigurationError(Exception):
    """Raised when required configuration is missing or invalid."""
    pass


def validate_config(require_bot: bool = False, require_api: bool = True) -> None:
    """
    Validate that all required configuration is present.

    Call this on application startup to fail fast with clear error messages
    instead of cryptic runtime failures.

    Args:
        require_bot: If True, validate bot token (for bot service)
        require_api: If True, validate API key (for web service)

    Raises:
        ConfigurationError: If required configuration is missing
    """
    errors = []

    # Always required
    if require_api and not config.api.key:
        errors.append("KEYCRM_API_KEY is required but not set")

    if require_bot and not config.bot.token:
        errors.append("BOT_TOKEN is required but not set")

    # Validate API key format (should be a reasonable length)
    if config.api.key and len(config.api.key) < 20:
        errors.append("KEYCRM_API_KEY appears to be invalid (too short)")

    # Validate bot token format (should contain colon)
    if config.bot.token and ":" not in config.bot.token:
        errors.append("BOT_TOKEN appears to be invalid (expected format: 123456:ABC-DEF...)")

    # Validate admin IDs if bot is required
    if require_bot and not config.bot.admin_user_ids:
        errors.append("ADMIN_USER_IDS is required but not set (comma-separated Telegram user IDs)")

    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        raise ConfigurationError(error_msg)
