import unittest
from unittest.mock import MagicMock, patch

import requests

from tap_toggl.toggl import Toggl


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
