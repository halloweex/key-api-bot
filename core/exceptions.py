"""
Custom exception hierarchy for KeyCRM API operations.

Exception Hierarchy:
    KeyCRMError (base)
    ├── KeyCRMConnectionError  - Network/timeout issues (recoverable)
    ├── KeyCRMAPIError         - API returned error response
    └── KeyCRMDataError        - Invalid response structure

    ValidationError            - Input validation failed
"""


class KeyCRMError(Exception):
    """Base exception for all KeyCRM-related errors."""

    def __init__(self, message: str, details: str = None):
        self.message = message
        self.details = details
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.details:
            return f"{self.message}: {self.details}"
        return self.message


class KeyCRMConnectionError(KeyCRMError):
    """
    Network-related errors (timeout, connection refused, etc.).

    These are typically recoverable with retry.
    """

    def __init__(self, message: str, details: str = None, retry_after: int = None):
        super().__init__(message, details)
        self.retry_after = retry_after


class KeyCRMAPIError(KeyCRMError):
    """
    API returned an error response.

    Check status_code and error_code for specifics.
    """

    def __init__(
        self,
        message: str,
        details: str = None,
        status_code: int = None,
        error_code: str = None,
    ):
        super().__init__(message, details)
        self.status_code = status_code
        self.error_code = error_code


class KeyCRMDataError(KeyCRMError):
    """
    API response has unexpected structure.

    This indicates a contract violation - the API returned
    data in a format we don't understand.
    """

    def __init__(self, message: str, details: str = None, expected: str = None, got: str = None):
        super().__init__(message, details)
        self.expected = expected
        self.got = got


class ValidationError(Exception):
    """
    Input validation failed.

    Used for validating user input before processing.
    """

    def __init__(self, field: str, message: str, value: any = None):
        self.field = field
        self.message = message
        self.value = value
        super().__init__(f"{field}: {message}")

    def __str__(self) -> str:
        if self.value is not None:
            return f"{self.field}: {self.message} (got: {self.value!r})"
        return f"{self.field}: {self.message}"
