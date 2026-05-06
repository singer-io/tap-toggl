class TogglError(Exception):
    """Base exception for all Toggl API errors."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class TogglQuotaExceededError(TogglError):
    """402 with quota headers — sliding-window limit reached.
    Wait for X-Toggl-Quota-Resets-In seconds, then let backoff retry.
    """


class TogglRateLimitError(TogglError):
    """class representing 429 status code."""
    pass


class TogglFeatureNotAvailableError(TogglError):
    """402 without quota headers — endpoint requires a higher plan.
    Do not retry; workspace must be upgraded first.
    """


class TogglGoneError(TogglError):
    """410 Gone — endpoint no longer exists. Do not call again."""
