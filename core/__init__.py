"""
Core shared library for KoreanStory Sales Bot.

This package contains shared logic used by both bot/ and web/ packages:
- exceptions: Custom exception hierarchy
- validators: Input validation functions
- pagination: Unified API pagination
- config: Centralized configuration
"""

# Import in dependency order
from core.exceptions import (
    KeyCRMError,
    KeyCRMConnectionError,
    KeyCRMAPIError,
    KeyCRMDataError,
    ValidationError,
)

from core.validators import (
    validate_date_string,
    validate_date_range,
    validate_source_id,
    validate_limit,
    validate_brand_name,
    validate_category_id,
    validate_period,
)

from core.pagination import (
    KeyCRMPaginator,
    AsyncKeyCRMPaginator,
)

from core.config import config

__all__ = [
    # Exceptions
    "KeyCRMError",
    "KeyCRMConnectionError",
    "KeyCRMAPIError",
    "KeyCRMDataError",
    "ValidationError",
    # Validators
    "validate_date_string",
    "validate_date_range",
    "validate_source_id",
    "validate_limit",
    "validate_brand_name",
    "validate_category_id",
    "validate_period",
    # Pagination
    "KeyCRMPaginator",
    "AsyncKeyCRMPaginator",
    # Config
    "config",
]
