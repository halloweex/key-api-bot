"""
Tests for core.exceptions module.
"""
import pytest

from core.exceptions import (
    KeyCRMError,
    KeyCRMConnectionError,
    KeyCRMAPIError,
    KeyCRMDataError,
    ValidationError,
)


class TestKeyCRMError:
    """Tests for base KeyCRMError exception."""

    def test_message_only(self):
        """Error with message only."""
        error = KeyCRMError("Something went wrong")
        assert str(error) == "Something went wrong"
        assert error.message == "Something went wrong"
        assert error.details is None

    def test_message_with_details(self):
        """Error with message and details."""
        error = KeyCRMError("Failed to fetch", "Connection timeout")
        assert str(error) == "Failed to fetch: Connection timeout"
        assert error.message == "Failed to fetch"
        assert error.details == "Connection timeout"


class TestKeyCRMConnectionError:
    """Tests for KeyCRMConnectionError exception."""

    def test_inheritance(self):
        """Should inherit from KeyCRMError."""
        error = KeyCRMConnectionError("Connection failed")
        assert isinstance(error, KeyCRMError)

    def test_retry_after(self):
        """Should support retry_after attribute."""
        error = KeyCRMConnectionError("Rate limited", retry_after=60)
        assert error.retry_after == 60

    def test_no_retry_after(self):
        """retry_after should be None by default."""
        error = KeyCRMConnectionError("Failed")
        assert error.retry_after is None


class TestKeyCRMAPIError:
    """Tests for KeyCRMAPIError exception."""

    def test_inheritance(self):
        """Should inherit from KeyCRMError."""
        error = KeyCRMAPIError("API error")
        assert isinstance(error, KeyCRMError)

    def test_status_code(self):
        """Should support status_code attribute."""
        error = KeyCRMAPIError("Not found", status_code=404)
        assert error.status_code == 404

    def test_error_code(self):
        """Should support error_code attribute."""
        error = KeyCRMAPIError("Invalid request", error_code="INVALID_PARAM")
        assert error.error_code == "INVALID_PARAM"


class TestKeyCRMDataError:
    """Tests for KeyCRMDataError exception."""

    def test_inheritance(self):
        """Should inherit from KeyCRMError."""
        error = KeyCRMDataError("Invalid data")
        assert isinstance(error, KeyCRMError)

    def test_expected_got(self):
        """Should support expected and got attributes."""
        error = KeyCRMDataError("Type mismatch", expected="dict", got="list")
        assert error.expected == "dict"
        assert error.got == "list"


class TestValidationError:
    """Tests for ValidationError exception."""

    def test_not_keycrm_error(self):
        """Should NOT inherit from KeyCRMError."""
        error = ValidationError("date", "Invalid format")
        assert not isinstance(error, KeyCRMError)

    def test_field_and_message(self):
        """Should have field and message."""
        error = ValidationError("email", "Invalid email address")
        assert error.field == "email"
        assert error.message == "Invalid email address"
        assert str(error) == "email: Invalid email address"

    def test_with_value(self):
        """Should include value in string representation."""
        error = ValidationError("age", "Must be positive", value=-5)
        assert error.value == -5
        assert "-5" in str(error)

    def test_none_value(self):
        """None value should not be shown."""
        error = ValidationError("name", "Required")
        assert error.value is None
        assert "None" not in str(error)
