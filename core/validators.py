"""
Input validation functions for API parameters.

All validators raise ValidationError on invalid input.
"""

import re
from datetime import date, datetime
from typing import Optional, Tuple

from core.exceptions import ValidationError


# Valid source IDs in KeyCRM
VALID_SOURCE_IDS = {1, 2, 3, 4}  # Instagram, Telegram, Opencart, Shopify

# Maximum allowed values
MAX_LIMIT = 100
MAX_BRAND_LENGTH = 255


def validate_date_string(
    value: str,
    field: str = "date",
    format: str = "%Y-%m-%d"
) -> date:
    """
    Validate and parse a date string.

    Args:
        value: Date string to validate
        field: Field name for error messages
        format: Expected date format (default: YYYY-MM-DD)

    Returns:
        Parsed date object

    Raises:
        ValidationError: If date is invalid or in wrong format
    """
    if not value:
        raise ValidationError(field, "Date is required", value)

    if not isinstance(value, str):
        raise ValidationError(field, "Must be a string", value)

    try:
        return datetime.strptime(value, format).date()
    except ValueError:
        raise ValidationError(
            field,
            f"Invalid date format. Expected {format}",
            value
        )


def validate_date_range(
    start_date: str,
    end_date: str,
    max_days: int = 365
) -> Tuple[date, date]:
    """
    Validate a date range.

    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        max_days: Maximum allowed range in days

    Returns:
        Tuple of (start_date, end_date) as date objects

    Raises:
        ValidationError: If dates are invalid or range is too large
    """
    start = validate_date_string(start_date, "start_date")
    end = validate_date_string(end_date, "end_date")

    if start > end:
        raise ValidationError(
            "date_range",
            "Start date must be before or equal to end date",
            f"{start_date} to {end_date}"
        )

    days_diff = (end - start).days
    if days_diff > max_days:
        raise ValidationError(
            "date_range",
            f"Date range cannot exceed {max_days} days",
            f"{days_diff} days"
        )

    return start, end


def validate_source_id(
    value: Optional[int],
    field: str = "source_id",
    allow_none: bool = True
) -> Optional[int]:
    """
    Validate a source ID.

    Args:
        value: Source ID to validate
        field: Field name for error messages
        allow_none: Whether None is allowed

    Returns:
        Validated source ID or None

    Raises:
        ValidationError: If source ID is invalid
    """
    if value is None:
        if allow_none:
            return None
        raise ValidationError(field, "Source ID is required")

    if not isinstance(value, int):
        raise ValidationError(field, "Must be an integer", value)

    if value not in VALID_SOURCE_IDS:
        raise ValidationError(
            field,
            f"Must be one of {sorted(VALID_SOURCE_IDS)}",
            value
        )

    return value


def validate_limit(
    value: int,
    field: str = "limit",
    min_value: int = 1,
    max_value: int = MAX_LIMIT
) -> int:
    """
    Validate a limit/count parameter.

    Args:
        value: Limit value to validate
        field: Field name for error messages
        min_value: Minimum allowed value
        max_value: Maximum allowed value

    Returns:
        Validated limit

    Raises:
        ValidationError: If limit is out of range
    """
    if not isinstance(value, int):
        raise ValidationError(field, "Must be an integer", value)

    if value < min_value:
        raise ValidationError(
            field,
            f"Must be at least {min_value}",
            value
        )

    if value > max_value:
        raise ValidationError(
            field,
            f"Cannot exceed {max_value}",
            value
        )

    return value


def validate_brand_name(
    value: Optional[str],
    field: str = "brand",
    allow_none: bool = True
) -> Optional[str]:
    """
    Validate a brand name.

    Args:
        value: Brand name to validate
        field: Field name for error messages
        allow_none: Whether None/empty is allowed

    Returns:
        Validated and stripped brand name or None

    Raises:
        ValidationError: If brand name is invalid
    """
    if value is None or value == "":
        if allow_none:
            return None
        raise ValidationError(field, "Brand name is required")

    if not isinstance(value, str):
        raise ValidationError(field, "Must be a string", value)

    value = value.strip()

    if len(value) > MAX_BRAND_LENGTH:
        raise ValidationError(
            field,
            f"Cannot exceed {MAX_BRAND_LENGTH} characters",
            f"{len(value)} characters"
        )

    # Allow alphanumeric, spaces, hyphens, dots, apostrophes, and common symbols
    if not re.match(r"^[\w\s\-\.\'&,()]+$", value, re.UNICODE):
        raise ValidationError(
            field,
            "Contains invalid characters",
            value
        )

    return value


def validate_category_id(
    value: Optional[int],
    field: str = "category_id",
    allow_none: bool = True
) -> Optional[int]:
    """
    Validate a category ID.

    Args:
        value: Category ID to validate
        field: Field name for error messages
        allow_none: Whether None is allowed

    Returns:
        Validated category ID or None

    Raises:
        ValidationError: If category ID is invalid
    """
    if value is None:
        if allow_none:
            return None
        raise ValidationError(field, "Category ID is required")

    if not isinstance(value, int):
        raise ValidationError(field, "Must be an integer", value)

    if value <= 0:
        raise ValidationError(field, "Must be a positive integer", value)

    return value


def validate_period(
    value: Optional[str],
    field: str = "period",
    allow_none: bool = True
) -> Optional[str]:
    """
    Validate a period shortcut.

    Args:
        value: Period string to validate
        field: Field name for error messages
        allow_none: Whether None is allowed

    Returns:
        Validated period string or None

    Raises:
        ValidationError: If period is invalid
    """
    valid_periods = {"today", "yesterday", "week", "last_week", "month", "last_month"}

    if value is None or value == "":
        if allow_none:
            return None
        raise ValidationError(field, "Period is required")

    if not isinstance(value, str):
        raise ValidationError(field, "Must be a string", value)

    value = value.lower().strip()

    if value not in valid_periods:
        raise ValidationError(
            field,
            f"Must be one of: {', '.join(sorted(valid_periods))}",
            value
        )

    return value


def validate_sales_type(
    value: Optional[str],
    field: str = "sales_type",
    allow_none: bool = True
) -> Optional[str]:
    """
    Validate a sales type filter (retail/b2b).

    Args:
        value: Sales type string to validate
        field: Field name for error messages
        allow_none: Whether None is allowed (defaults to 'retail')

    Returns:
        Validated sales type string or None

    Raises:
        ValidationError: If sales type is invalid
    """
    valid_types = {"retail", "b2b", "all"}

    if value is None or value == "":
        if allow_none:
            return "retail"  # Default to retail
        raise ValidationError(field, "Sales type is required")

    if not isinstance(value, str):
        raise ValidationError(field, "Must be a string", value)

    value = value.lower().strip()

    if value not in valid_types:
        raise ValidationError(
            field,
            f"Must be one of: {', '.join(sorted(valid_types))}",
            value
        )

    return value
