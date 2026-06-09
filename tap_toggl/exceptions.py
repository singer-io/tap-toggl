class TogglForbiddenError(Exception):
    """Raised when the API returns a 403 Forbidden response."""
    pass
class TogglError(Exception):
    """Base exception for all Toggl API errors."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class TogglQuotaExceededError(TogglError):
    """402 with quota headers — sliding-window limit reached.
    Caller should wait for retry_after seconds; backoff honors this via backoff.runtime.
    """

    def __init__(self, message=None, retry_after=None, response=None):
        super().__init__(message, response)
        self.retry_after = retry_after


class TogglQuotaWaitTooLongError(TogglError):
    """Quota reset exceeds operational retry threshold."""
    pass


class TogglRateLimitError(TogglError):
    """class representing 429 status code."""
    pass


class TogglFeatureNotAvailableError(TogglError):
    """402 without quota headers — endpoint requires a higher plan.
    Do not retry; workspace must be upgraded first.
    """
    pass


class TogglGoneError(TogglError):
    """410 Gone — endpoint no longer exists. Do not call again."""
