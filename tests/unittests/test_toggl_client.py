import unittest
from unittest.mock import MagicMock, patch

import requests

from tap_toggl.exceptions import (
    TogglFeatureNotAvailableError,
    TogglQuotaExceededError,
    TogglQuotaWaitTooLongError,
    TogglRateLimitError,
)
from tap_toggl.toggl import Toggl


def _make_toggl_with_mocked_init(mock_get_fn):
    """Instantiate Toggl by mocking the initial workspace fetch."""
    workspace_response = MagicMock()
    workspace_response.json.return_value = [
        {"id": 11, "organization_id": 22}
    ]
    workspace_response.raise_for_status = MagicMock()
    workspace_response.status_code = 200
    workspace_response.headers = {}
    mock_get_fn.return_value = workspace_response
    return Toggl(api_token="test_token", start_date="2020-01-01", user_agent="test_agent")


class TestTogglClient(unittest.TestCase):

    @patch('tap_toggl.toggl.requests.get')
    def test_toggl_max_retries_exceeded(self, mock_requests_get):
        """Test that the Toggl client retries the request on failure."""
        # Mock to always raise RequestException
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.raise_for_status.side_effect = requests.exceptions.RequestException()

        mock_requests_get.return_value = mock_response

        # Should fail after max_tries (5) attempts
        with self.assertRaises(requests.exceptions.RequestException):
            Toggl(api_token="dummy_token", start_date="2020-01-01", user_agent="test_agent")

        self.assertEqual(mock_requests_get.call_count, 5)


class TestTogglInit(unittest.TestCase):
    """Tests for Toggl.__init__ and workspace/org ID population."""

    @patch('tap_toggl.toggl.requests.get')
    def test_workspace_ids_populated(self, mock_get):
        """workspace_ids are populated from the initial workspaces call."""
        client = _make_toggl_with_mocked_init(mock_get)
        self.assertEqual(client.workspace_ids, [11])

    @patch('tap_toggl.toggl.requests.get')
    def test_organization_ids_populated(self, mock_get):
        """organization_ids are populated from the initial workspaces call."""
        client = _make_toggl_with_mocked_init(mock_get)
        self.assertEqual(client.organization_ids, [22])

    @patch('tap_toggl.toggl.requests.get')
    def test_api_token_stored(self, mock_get):
        """api_token is stored on the client object."""
        client = _make_toggl_with_mocked_init(mock_get)
        self.assertEqual(client.api_token, "test_token")


class TestRequestTooLarge(unittest.TestCase):
    """Tests for the Toggl.request_too_large static/class method."""

    def test_503_http_error_returns_true(self):
        """HTTPError with status_code 503 returns True (should give up)."""
        mock_response = MagicMock()
        mock_response.status_code = 503
        err = requests.exceptions.HTTPError(response=mock_response)
        err.response = mock_response
        self.assertTrue(Toggl.request_too_large(err))

    def test_500_http_error_returns_false(self):
        """HTTPError with status_code 500 returns False (should retry)."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        err = requests.exceptions.HTTPError(response=mock_response)
        err.response = mock_response
        self.assertFalse(Toggl.request_too_large(err))

    def test_402_http_error_returns_false(self):
        """HTTPError with status_code 402 returns False (quota exceeded, should retry)."""
        mock_response = MagicMock()
        mock_response.status_code = 402
        err = requests.exceptions.HTTPError(response=mock_response)
        err.response = mock_response
        self.assertFalse(Toggl.request_too_large(err))

    def test_404_http_error_returns_false(self):
        """HTTPError with status_code 404 returns False (should retry)."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        err = requests.exceptions.HTTPError(response=mock_response)
        err.response = mock_response
        self.assertFalse(Toggl.request_too_large(err))

    def test_429_http_error_returns_false(self):
        """HTTPError with status_code 429 returns False (rate limit, should retry)."""
        mock_response = MagicMock()
        mock_response.status_code = 429
        err = requests.exceptions.HTTPError(response=mock_response)
        err.response = mock_response
        self.assertFalse(Toggl.request_too_large(err))

    def test_non_http_error_returns_false(self):
        """Non-HTTPError exceptions return False."""
        err = requests.exceptions.ConnectionError("timeout")
        self.assertFalse(Toggl.request_too_large(err))


class TestQuotaHandling(unittest.TestCase):
    """Tests for 402 handling: backoff retries with retry_after if wait <= MAX, fail fast otherwise."""

    @patch('tap_toggl.toggl.requests.get')
    def test_402_short_wait_raises_with_retry_after(self, mock_get):
        """402 with reset time <= MAX_QUOTA_WAIT_SECONDS raises TogglQuotaExceededError with retry_after set."""
        client = _make_toggl_with_mocked_init(mock_get)

        quota_response = MagicMock()
        quota_response.status_code = 402
        quota_response.headers = {
            'X-Toggl-Quota-Remaining': '0',
            'X-Toggl-Quota-Resets-In': '120',
        }
        mock_get.return_value = quota_response

        with self.assertRaises(TogglQuotaExceededError) as ctx:
            client._get.__wrapped__(client, 'https://api.track.toggl.com/api/v9/test')

        # retry_after is set so backoff.runtime knows how long to sleep
        self.assertEqual(ctx.exception.retry_after, 120)

    @patch('tap_toggl.toggl.requests.get')
    def test_402_long_wait_fails_fast_no_sleep(self, mock_get):
        """402 with reset time > MAX_QUOTA_WAIT_SECONDS raises TogglQuotaWaitTooLongError immediately (non-retryable)."""
        client = _make_toggl_with_mocked_init(mock_get)

        quota_response = MagicMock()
        quota_response.status_code = 402
        quota_response.headers = {
            'X-Toggl-Quota-Remaining': '0',
            'X-Toggl-Quota-Resets-In': '2818',
        }
        mock_get.return_value = quota_response

        with self.assertRaises(TogglQuotaWaitTooLongError):
            client._get.__wrapped__(client, 'https://api.track.toggl.com/api/v9/test')

    @patch('tap_toggl.toggl.requests.get')
    def test_402_no_resets_in_header_defaults_60s(self, mock_get):
        """402 with quota remaining but no resets-in defaults retry_after to 60s."""
        client = _make_toggl_with_mocked_init(mock_get)

        quota_response = MagicMock()
        quota_response.status_code = 402
        quota_response.headers = {'X-Toggl-Quota-Remaining': '0'}
        mock_get.return_value = quota_response

        with self.assertRaises(TogglQuotaExceededError) as ctx:
            client._get.__wrapped__(client, 'https://api.track.toggl.com/api/v9/test')

        self.assertEqual(ctx.exception.retry_after, 60)

    @patch('tap_toggl.toggl.requests.get')
    def test_402_without_quota_headers_raises_feature_error(self, mock_get):
        """402 without quota headers raises TogglFeatureNotAvailableError (plan restriction, non-retryable)."""
        client = _make_toggl_with_mocked_init(mock_get)

        feature_response = MagicMock()
        feature_response.status_code = 402
        feature_response.headers = {}
        mock_get.return_value = feature_response

        with self.assertRaises(TogglFeatureNotAvailableError):
            client._get.__wrapped__(client, 'https://api.track.toggl.com/api/v9/test')

    @patch('tap_toggl.toggl.requests.get')
    def test_429_raises_rate_limit_error(self, mock_get):
        """429 raises TogglRateLimitError so @backoff retries with exponential delay."""
        client = _make_toggl_with_mocked_init(mock_get)

        rate_response = MagicMock()
        rate_response.status_code = 429
        rate_response.headers = {}
        mock_get.return_value = rate_response

        with self.assertRaises(TogglRateLimitError):
            client._get.__wrapped__(client, 'https://api.track.toggl.com/api/v9/test')


class TestRequestCounter(unittest.TestCase):
    """Tests for the request_count tracking."""

    @patch('tap_toggl.toggl.requests.get')
    def test_request_count_increments(self, mock_get):
        """Each _get call increments the class-level request counter."""
        client = _make_toggl_with_mocked_init(mock_get)
        count_before = Toggl.request_count

        ok_response = MagicMock()
        ok_response.status_code = 200
        ok_response.headers = {}
        ok_response.json.return_value = [{"id": 1}]
        ok_response.raise_for_status = MagicMock()
        mock_get.return_value = ok_response

        client._get('https://api.track.toggl.com/api/v9/test')
        self.assertEqual(Toggl.request_count, count_before + 1)


class TestEndpointHelpers(unittest.TestCase):
    """Tests for workspace and organization endpoint builder methods."""

    @patch('tap_toggl.toggl.requests.get')
    def test_get_workspace_endpoints(self, mock_get):
        """_get_workspace_endpoints expands {workspace_id} for each id."""
        client = _make_toggl_with_mocked_init(mock_get)
        endpoints = client._get_workspace_endpoints("https://api.example.com/{workspace_id}/data")
        self.assertEqual(endpoints, ["https://api.example.com/11/data"])

    @patch('tap_toggl.toggl.requests.get')
    def test_get_organization_endpoints(self, mock_get):
        """_get_organization_endpoints expands {organization_id} for each id."""
        client = _make_toggl_with_mocked_init(mock_get)
        endpoints = client._get_organization_endpoints("https://api.example.com/{organization_id}/groups")
        self.assertEqual(endpoints, ["https://api.example.com/22/groups"])

    @patch('tap_toggl.toggl.requests.get')
    def test_paginate_endpoint_adds_page_param(self, mock_get):
        """_paginate_endpoint appends page=N query parameter."""
        client = _make_toggl_with_mocked_init(mock_get)
        url = "https://api.example.com/data"
        paginated = client._paginate_endpoint(url, page=3)
        self.assertIn("page=3", paginated)

    @patch('tap_toggl.toggl.requests.get')
    def test_paginate_endpoint_updates_existing_page(self, mock_get):
        """_paginate_endpoint replaces an existing page parameter."""
        client = _make_toggl_with_mocked_init(mock_get)
        url = "https://api.example.com/data?page=1"
        paginated = client._paginate_endpoint(url, page=5)
        self.assertIn("page=5", paginated)
        self.assertNotIn("page=1", paginated)


class TestTogglGetResponse(unittest.TestCase):
    """Tests for Toggl._get_response."""

    @patch('tap_toggl.toggl.requests.get')
    def test_non_paginated_yields_items(self, mock_get):
        """_get_response without key yields all items from the response list."""
        records = [{"id": 1}, {"id": 2}]
        # First call: workspace init. Second call: data fetch.
        init_response = MagicMock()
        init_response.json.return_value = [{"id": 11, "organization_id": 22}]
        init_response.raise_for_status = MagicMock()

        data_response = MagicMock()
        data_response.json.return_value = records
        data_response.raise_for_status = MagicMock()

        mock_get.side_effect = [init_response, data_response]
        client = Toggl(api_token="t", start_date="2020-01-01", user_agent="ua")
        result = list(client._get_response("https://api.example.com/items"))
        self.assertEqual(result, records)

    @patch('tap_toggl.toggl.requests.get')
    def test_paginated_yields_items_then_stops(self, mock_get):
        """_get_response with key='data' paginates until empty data returned."""
        init_response = MagicMock()
        init_response.json.return_value = [{"id": 11, "organization_id": 22}]
        init_response.raise_for_status = MagicMock()

        page1_response = MagicMock()
        page1_response.json.return_value = {"data": [{"id": 1}, {"id": 2}]}
        page1_response.raise_for_status = MagicMock()

        page2_response = MagicMock()
        page2_response.json.return_value = {"data": []}
        page2_response.raise_for_status = MagicMock()

        mock_get.side_effect = [init_response, page1_response, page2_response]
        client = Toggl(api_token="t", start_date="2020-01-01", user_agent="ua")
        result = list(client._get_response(
            "https://api.track.toggl.com/reports/api/v2/details?workspace_id=11",
            key="data"
        ))
        self.assertEqual(len(result), 2)


class TestTogglIsAuthorized(unittest.TestCase):
    """Tests for Toggl.is_authorized."""

    @patch('tap_toggl.toggl.requests.get')
    def test_is_authorized_calls_me_endpoint(self, mock_get):
        """is_authorized calls the /me endpoint."""
        init_response = MagicMock()
        init_response.json.return_value = [{"id": 11, "organization_id": 22}]
        init_response.raise_for_status = MagicMock()

        me_response = MagicMock()
        me_response.json.return_value = {"id": 99, "email": "user@example.com"}
        me_response.raise_for_status = MagicMock()

        mock_get.side_effect = [init_response, me_response]
        client = Toggl(api_token="t", start_date="2020-01-01", user_agent="ua")
        result = client.is_authorized()

        # Verify that /me was called
        all_urls = [c.args[0] for c in mock_get.call_args_list]
        self.assertTrue(any("/me" in url for url in all_urls))


class TestTogglWorkspaces(unittest.TestCase):
    """Tests for Toggl.workspaces() method."""

    @patch('tap_toggl.toggl.requests.get')
    def test_workspaces_yields_all_items(self, mock_get):
        """workspaces() yields each workspace dict."""
        workspace_data = [{"id": 11, "organization_id": 22}, {"id": 33, "organization_id": 44}]

        init_response = MagicMock()
        init_response.json.return_value = workspace_data
        init_response.raise_for_status = MagicMock()

        ws_response = MagicMock()
        ws_response.json.return_value = workspace_data
        ws_response.raise_for_status = MagicMock()

        mock_get.side_effect = [init_response, ws_response]
        client = Toggl(api_token="t", start_date="2020-01-01", user_agent="ua")
        result = list(client.workspaces())
        self.assertEqual(len(result), 2)


class TestTogglTimeEntries(unittest.TestCase):
    """Tests for Toggl.time_entries date range generation."""

    @patch('tap_toggl.toggl.requests.get')
    def test_time_entries_uses_bookmark(self, mock_get):
        """time_entries uses bookmark to calculate start_date with trailing_days offset."""
        call_count = [0]

        def _side_effect(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if call_count[0] == 0:
                resp.json.return_value = [{"id": 11, "organization_id": 22}]
            else:
                resp.json.return_value = {"data": []}
            call_count[0] += 1
            return resp

        mock_get.side_effect = _side_effect

        client = Toggl(api_token="t", start_date="2020-01-01", user_agent="ua", trailing_days=1)
        result = list(client.time_entries(bookmark="2020-06-01T00:00:00Z"))
        self.assertIsInstance(result, list)

    @patch('tap_toggl.toggl.requests.get')
    def test_time_entries_no_bookmark_uses_start_date(self, mock_get):
        """time_entries falls back to start_date when no bookmark is provided."""
        call_count = [0]

        def _side_effect(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if call_count[0] == 0:
                resp.json.return_value = [{"id": 11, "organization_id": 22}]
            else:
                resp.json.return_value = {"data": []}
            call_count[0] += 1
            return resp

        mock_get.side_effect = _side_effect

        client = Toggl(api_token="t", start_date="2020-01-01", user_agent="ua")
        result = list(client.time_entries(bookmark=None))
        self.assertIsInstance(result, list)
