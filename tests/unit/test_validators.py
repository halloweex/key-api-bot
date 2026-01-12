"""
Tests for core.validators module.
"""
import pytest
from datetime import date

from core.validators import (
    validate_date_string,
    validate_date_range,
    validate_source_id,
    validate_limit,
    validate_brand_name,
    validate_category_id,
    validate_period,
)
from core.exceptions import ValidationError


class TestValidateDateString:
    """Tests for validate_date_string function."""

    def test_valid_date(self):
        """Valid date string should return date object."""
        result = validate_date_string("2026-01-15")
        assert result == date(2026, 1, 15)

    def test_valid_date_custom_format(self):
        """Custom format should work."""
        result = validate_date_string("15/01/2026", format="%d/%m/%Y")
        assert result == date(2026, 1, 15)

    def test_invalid_format(self):
        """Invalid format should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_date_string("15-01-2026")  # Wrong format
        assert "Invalid date format" in str(exc_info.value)

    def test_invalid_date(self):
        """Invalid date should raise ValidationError."""
        with pytest.raises(ValidationError):
            validate_date_string("2026-02-30")  # Feb 30 doesn't exist

    def test_empty_string(self):
        """Empty string should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_date_string("")
        assert "required" in str(exc_info.value).lower()

    def test_none_value(self):
        """None should raise ValidationError."""
        with pytest.raises(ValidationError):
            validate_date_string(None)

    def test_non_string(self):
        """Non-string should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_date_string(12345)
        assert "string" in str(exc_info.value).lower()


class TestValidateDateRange:
    """Tests for validate_date_range function."""

    def test_valid_range(self):
        """Valid date range should return tuple of dates."""
        start, end = validate_date_range("2026-01-01", "2026-01-31")
        assert start == date(2026, 1, 1)
        assert end == date(2026, 1, 31)

    def test_same_day(self):
        """Same start and end date should be valid."""
        start, end = validate_date_range("2026-01-15", "2026-01-15")
        assert start == end == date(2026, 1, 15)

    def test_start_after_end(self):
        """Start date after end date should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_date_range("2026-01-31", "2026-01-01")
        assert "before or equal" in str(exc_info.value)

    def test_range_too_large(self):
        """Range exceeding max_days should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_date_range("2025-01-01", "2026-01-31", max_days=30)
        assert "exceed" in str(exc_info.value).lower()

    def test_custom_max_days(self):
        """Custom max_days should be respected."""
        # Should work with 365 days
        validate_date_range("2025-01-01", "2025-12-31", max_days=365)

        # Should fail with 30 days
        with pytest.raises(ValidationError):
            validate_date_range("2025-01-01", "2025-12-31", max_days=30)


class TestValidateSourceId:
    """Tests for validate_source_id function."""

    def test_valid_source_ids(self):
        """Valid source IDs should return the same value."""
        assert validate_source_id(1) == 1
        assert validate_source_id(2) == 2
        assert validate_source_id(3) == 3
        assert validate_source_id(4) == 4

    def test_none_allowed(self):
        """None should be allowed by default."""
        assert validate_source_id(None) is None

    def test_none_not_allowed(self):
        """None should raise error when allow_none=False."""
        with pytest.raises(ValidationError):
            validate_source_id(None, allow_none=False)

    def test_invalid_source_id(self):
        """Invalid source ID should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_source_id(5)
        assert "1, 2, 3, 4" in str(exc_info.value)

    def test_non_integer(self):
        """Non-integer should raise ValidationError."""
        with pytest.raises(ValidationError):
            validate_source_id("1")


class TestValidateLimit:
    """Tests for validate_limit function."""

    def test_valid_limit(self):
        """Valid limit should return the same value."""
        assert validate_limit(10) == 10
        assert validate_limit(1) == 1
        assert validate_limit(100) == 100

    def test_zero(self):
        """Zero should raise ValidationError."""
        with pytest.raises(ValidationError):
            validate_limit(0)

    def test_negative(self):
        """Negative should raise ValidationError."""
        with pytest.raises(ValidationError):
            validate_limit(-5)

    def test_exceeds_max(self):
        """Exceeding max should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_limit(101)
        assert "exceed" in str(exc_info.value).lower()

    def test_custom_max(self):
        """Custom max_value should be respected."""
        assert validate_limit(50, max_value=50) == 50
        with pytest.raises(ValidationError):
            validate_limit(51, max_value=50)


class TestValidateBrandName:
    """Tests for validate_brand_name function."""

    def test_valid_brand(self):
        """Valid brand name should return stripped value."""
        assert validate_brand_name("Nike") == "Nike"
        assert validate_brand_name("  Nike  ") == "Nike"
        assert validate_brand_name("L'Oréal") == "L'Oréal"
        assert validate_brand_name("H&M") == "H&M"

    def test_none_allowed(self):
        """None should be allowed by default."""
        assert validate_brand_name(None) is None
        assert validate_brand_name("") is None

    def test_none_not_allowed(self):
        """None should raise error when allow_none=False."""
        with pytest.raises(ValidationError):
            validate_brand_name(None, allow_none=False)

    def test_too_long(self):
        """Too long brand name should raise ValidationError."""
        long_name = "A" * 300
        with pytest.raises(ValidationError) as exc_info:
            validate_brand_name(long_name)
        assert "255" in str(exc_info.value)


class TestValidatePeriod:
    """Tests for validate_period function."""

    def test_valid_periods(self):
        """Valid period shortcuts should be accepted."""
        assert validate_period("today") == "today"
        assert validate_period("yesterday") == "yesterday"
        assert validate_period("week") == "week"
        assert validate_period("last_week") == "last_week"
        assert validate_period("month") == "month"
        assert validate_period("last_month") == "last_month"

    def test_case_insensitive(self):
        """Period should be case-insensitive."""
        assert validate_period("TODAY") == "today"
        assert validate_period("Today") == "today"

    def test_none_allowed(self):
        """None should be allowed by default."""
        assert validate_period(None) is None
        assert validate_period("") is None

    def test_invalid_period(self):
        """Invalid period should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            validate_period("invalid")
        assert "today" in str(exc_info.value)


class TestValidateCategoryId:
    """Tests for validate_category_id function."""

    def test_valid_category_id(self):
        """Valid category ID should return the same value."""
        assert validate_category_id(1) == 1
        assert validate_category_id(100) == 100

    def test_none_allowed(self):
        """None should be allowed by default."""
        assert validate_category_id(None) is None

    def test_zero_invalid(self):
        """Zero should raise ValidationError."""
        with pytest.raises(ValidationError):
            validate_category_id(0)

    def test_negative_invalid(self):
        """Negative should raise ValidationError."""
        with pytest.raises(ValidationError):
            validate_category_id(-1)
