#
# Module dependencies.
#

import logging
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import backoff
import requests
from requests.auth import HTTPBasicAuth
from singer import utils

from tap_toggl.exceptions import (
    TogglFeatureNotAvailableError,
    TogglQuotaExceededError,
    TogglQuotaWaitTooLongError,
    TogglRateLimitError,
)

BASE_URL = "https://api.track.toggl.com/api"
API_VERSION = "v9"

logger = logging.getLogger()

# Maximum seconds to wait for quota reset before failing fast
MAX_QUOTA_WAIT_SECONDS = 300

# Default request timeout in seconds
REQUEST_TIMEOUT_SECONDS = 30


def _backoff_wait_value(exc):
    """Return retry delay based on exception type.
    - TogglQuotaExceededError: honor API-provided retry_after
    - TogglRateLimitError: 30s (retry quickly; backoff.runtime will escalate on repeated failures)
    - transient network/request errors: 5s
    """
    if isinstance(exc, TogglQuotaExceededError) and exc.retry_after:
        return float(exc.retry_after)
    if isinstance(exc, TogglRateLimitError):
        return 30.0
    return 5.0


def _on_backoff(details):
    """Log before each backoff sleep so CircleCI console stays alive."""
    exc = details.get("exception")
    wait = details.get("wait", 0)
    logger.warning(
        "Backing off for %ds before retry %d due to %s: %s",
        int(wait), details["tries"], type(exc).__name__, exc
    )


class Toggl(object):
    """ Simple wrapper for Toggl. """

    request_count = 0  # Track total API requests for quota debugging

    def __init__(self, api_token=None, start_date=None, user_agent=None, trailing_days=1):
        self.api_token = api_token
        self.trailing_days = int(trailing_days)
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(api_token, "api_token")
        self.start_date = start_date
        self.workspace_ids = []
        self.organization_ids = []
        self.user_agent = user_agent
        res = self._get(f'{BASE_URL}/{API_VERSION}/workspaces')
        for item in res:
            self.workspace_ids.append(item['id'])
            self.organization_ids.append(item['organization_id'])

    # pylint: disable=E0213
    def request_too_large(error):
        logger.warning('Request {type} exception caught:  {error}'.format(type=error.__class__.__name__, error=error))
        if isinstance(error, requests.exceptions.HTTPError):
            if error.response.status_code == 503:
                return True
        return False

    def _get_workspace_endpoints(self, endpoint):
        endpoints = []
        for workspace_id in self.workspace_ids:
            endpoints.append(endpoint.format(workspace_id=workspace_id))
        return endpoints

    def _get_organization_endpoints(self, endpoint):
        endpoints = []
        for organization_id in self.organization_ids:
            endpoints.append(endpoint.format(organization_id=organization_id))
        return endpoints

    def _paginate_endpoint(self, endpoint, page=0):
        parsed_url = urlparse(endpoint)
        query_params = parse_qs(parsed_url.query)

        query_params['page'] = [str(page)]

        updated_query = urlencode(query_params, doseq=True)
        updated_url = urlunparse(parsed_url._replace(query=updated_query))

        return updated_url

    @backoff.on_exception(backoff.runtime,
                          (requests.exceptions.RequestException,
                           TogglQuotaExceededError,
                           TogglRateLimitError),
                          value=_backoff_wait_value,
                          max_tries=5,
                          giveup=request_too_large,
                          on_backoff=_on_backoff)
    def _get(self, url, **kwargs):
        Toggl.request_count += 1
        kwargs.setdefault("timeout", REQUEST_TIMEOUT_SECONDS)
        logger.info("Request #%d: Hitting %s", Toggl.request_count, url)
        response = self.session.get(url, **kwargs)

        if response.status_code == 429:
            logger.warning('Rate limited (429) after %d requests. Backing off.', Toggl.request_count)
            raise TogglRateLimitError(
                f"{response.status_code} {response.reason} for url: {url}",
                response=response
            )

        # Log quota headers when present
        quota_remaining = response.headers.get('X-Toggl-Quota-Remaining')
        quota_resets_in = response.headers.get('X-Toggl-Quota-Resets-In')
        if quota_remaining is not None:
            logger.info("Quota remaining: %s, resets in: %ss", quota_remaining, quota_resets_in)

        # Handle 402 — Toggl uses this for two distinct cases:
        # 1. Quota exhaustion (headers present): sliding-window limit reached.
        #    Wait up to MAX_QUOTA_WAIT_SECONDS, then raise TogglQuotaExceededError so @backoff retries.
        #    If reset time exceeds MAX_QUOTA_WAIT_SECONDS, fail fast (CI safety).
        # 2. Feature restriction (no headers): endpoint needs a higher plan.
        #    Raise TogglFeatureNotAvailableError — non-retryable, do not repeat the request.
        if response.status_code == 402:
            if quota_remaining is not None or quota_resets_in is not None:
                try:
                    wait_seconds = int(quota_resets_in) if quota_resets_in else 60
                except (TypeError, ValueError):
                    logger.warning(
                        "Quota headers incomplete: remaining=%s resets_in=%s",
                        quota_remaining,
                        quota_resets_in,
                    )
                    wait_seconds = 60
                if wait_seconds <= MAX_QUOTA_WAIT_SECONDS:
                    logger.warning(
                        'API quota exceeded (402) after %d requests. '
                        'Quota remaining: %s. Retrying in %d seconds.',
                        Toggl.request_count, quota_remaining, wait_seconds
                    )
                    # backoff.runtime uses retry_after to delay the retry.
                    raise TogglQuotaExceededError(
                        f"{response.status_code} {response.reason} for url: {url}",
                        retry_after=wait_seconds,
                        response=response
                    )
                raise TogglQuotaWaitTooLongError(
                    f"Quota resets in {wait_seconds}s which exceeds max wait "
                    f"({MAX_QUOTA_WAIT_SECONDS}s). Failing fast for url: {url}",
                    response=response
                )
            # No quota headers — plan restriction, do not retry
            raise TogglFeatureNotAvailableError(
                f"{response.status_code} {response.reason} for url: {url}",
                response=response
            )

        response.raise_for_status()
        return response.json()

    def _get_response(self, url, column_name=None, bookmark=None, key=None):
        # Special paginated case for `time_entries`, which requires `key` attribute.
        if key == "data":
            page = 1 if "/tasks" in url else 0
            length = 1
            while length > 0:
                url = self._paginate_endpoint(url, page)
                res = self._get(url)
                data = res["data"]
                if data:
                    length = len(data)
                    logger.info('Endpoint returned {length} rows.'.format(length=length))
                    for item in data:
                        yield item
                    page += 1
                else:
                    length = 0

        else:
            res = self._get(url)
            res = [] if res is None else res
            data = res[key] if key is not None else res
            length = len(data)
            logger.info('Endpoint returned {length} rows.'.format(length=length))
            for item in data:
                yield item

    def _get_from_endpoints(self, endpoints, column_name=None, bookmark=None, key=None):
        for endpoint in endpoints:
            gtr = self._get_response(endpoint, key=key)
            for item in gtr:
                yield item

    def is_authorized(self):
        return self._get(f'{BASE_URL}/{API_VERSION}' + '/me')

    def workspaces(self, column_name=None, bookmark=None):
        res = self._get(f'{BASE_URL}/{API_VERSION}' + '/workspaces')
        for item in res:
            yield item

    def clients(self, column_name=None, bookmark=None):
        endpoints = self._get_workspace_endpoints(f'{BASE_URL}/{API_VERSION}' + r'/workspaces/{workspace_id}/clients')
        return self._get_from_endpoints(endpoints, column_name, bookmark)

    def groups(self, column_name=None, bookmark=None):
        endpoints = self._get_organization_endpoints(f'{BASE_URL}/{API_VERSION}' + r'/organizations/{organization_id}/groups')
        return self._get_from_endpoints(endpoints, column_name, bookmark)

    def projects(self, column_name=None, bookmark=None):
        endpoints = self._get_workspace_endpoints(f'{BASE_URL}/{API_VERSION}' + r'/workspaces/{workspace_id}/projects')
        return self._get_from_endpoints(endpoints, column_name, bookmark)

    def tasks(self, column_name=None, bookmark=None):
        endpoints = self._get_workspace_endpoints(f'{BASE_URL}/{API_VERSION}' + r'/workspaces/{workspace_id}/tasks')
        return self._get_from_endpoints(endpoints, column_name, bookmark, key='data')

    def tags(self, column_name=None, bookmark=None):
        endpoints = self._get_workspace_endpoints(f'{BASE_URL}/{API_VERSION}' + r'/workspaces/{workspace_id}/tags')
        return self._get_from_endpoints(endpoints, column_name, bookmark)

    def users(self, column_name=None, bookmark=None):
        endpoints = self._get_workspace_endpoints(f'{BASE_URL}/{API_VERSION}' + r'/workspaces/{workspace_id}/users')
        return self._get_from_endpoints(endpoints, column_name, bookmark)

    def workspace_users(self, column_name=None, bookmark=None):
        endpoints = self._get_workspace_endpoints(f'{BASE_URL}/{API_VERSION}' + r'/workspaces/{workspace_id}/workspace_users')
        return self._get_from_endpoints(endpoints, column_name, bookmark)

    def time_entries(self, column_name=None, bookmark=None):
        fmt = '%Y-%m-%d'
        end_date = datetime.today().strftime(fmt)

        try:
            start_date = (utils.strptime_with_tz(bookmark) - timedelta(days=self.trailing_days)).strftime(fmt)

        except (AttributeError, OverflowError, ValueError, TypeError):
            if bookmark is None:
                start_date = utils.strptime_with_tz(self.start_date).strftime(fmt)

        endpoints = []
        moving_start_date = utils.strptime_with_tz(start_date)
        moving_end_date = moving_start_date + timedelta(days=30)
        while moving_start_date <= utils.strptime_with_tz(end_date):
            new_endpoints = self._get_workspace_endpoints('https://api.track.toggl.com/reports/api/v2/details?workspace_id={{workspace_id}}&since={start_date}&until={end_date}&user_agent={user_agent}'.format(start_date=moving_start_date.strftime(fmt), end_date=moving_end_date.strftime(fmt), user_agent=self.user_agent))
            endpoints.extend(new_endpoints)
            moving_start_date += timedelta(days=30)
            moving_end_date = moving_start_date + timedelta(days=30)

        return self._get_from_endpoints(endpoints, column_name, bookmark, "data")
