import unittest
from unittest.mock import patch, MagicMock

from tap_toggl.discover import discover_streams, _apply_access_checks
from tap_toggl.streams import STREAMS, Stream
from tap_toggl.exceptions import TogglForbiddenError


class TestDiscoveryAccessChecks(unittest.TestCase):
    """Tests for stream exclusion during discovery based on 403 responses."""

    def _make_stream_entry(self, name):
        return {'stream': name, 'tap_stream_id': name, 'schema': {}, 'metadata': []}

    @patch.object(Stream, 'check_access', return_value=True)
    def test_all_streams_accessible(self, mock_check):
        """When all streams are accessible, none should be excluded."""
        streams = [self._make_stream_entry(name) for name in STREAMS.keys()]
        original_count = len(streams)

        _apply_access_checks(MagicMock(), streams)

        self.assertEqual(len(streams), original_count)

    @patch.object(Stream, 'check_access')
    def test_partial_access_excludes_forbidden_streams(self, mock_check):
        """When some streams return 403, they should be excluded from the catalog."""
        forbidden_stream = 'tasks'

        mock_check.side_effect = lambda: None  # reset

        # Patch at instance level via the class
        with patch.object(Stream, 'check_access', new=lambda self_inner: self_inner.name != forbidden_stream):
            streams = [self._make_stream_entry(name) for name in STREAMS.keys()]
            _apply_access_checks(MagicMock(), streams)

            stream_names = [s['tap_stream_id'] for s in streams]
            self.assertNotIn(forbidden_stream, stream_names)
            self.assertIn('workspaces', stream_names)

    @patch.object(Stream, 'check_access', return_value=False)
    def test_no_streams_accessible_raises_error(self, mock_check):
        """When all streams return 403, a TogglForbiddenError should be raised."""
        streams = [self._make_stream_entry(name) for name in STREAMS.keys()]

        with self.assertRaises(TogglForbiddenError):
            _apply_access_checks(MagicMock(), streams)

    @patch.object(Stream, 'check_access', return_value=True)
    @patch.object(Stream, 'load_schema', return_value={'properties': {'id': {'type': 'integer'}}})
    @patch.object(Stream, 'load_metadata', return_value=[])
    def test_discover_streams_returns_catalog_entries(self, mock_meta, mock_schema, mock_check):
        """discover_streams should return a list of catalog entries for accessible streams."""
        client = MagicMock()
        streams = discover_streams(client)

        self.assertIsInstance(streams, list)
        self.assertTrue(len(streams) > 0)
        for entry in streams:
            self.assertIn('stream', entry)
            self.assertIn('tap_stream_id', entry)
            self.assertIn('schema', entry)
            self.assertIn('metadata', entry)


class TestStreamCheckAccess(unittest.TestCase):
    """Tests for the Stream.check_access() method."""

    def test_check_access_returns_true_on_success(self):
        """check_access returns True when the API call succeeds."""
        client = MagicMock()
        client.workspaces = MagicMock(return_value=iter([{'id': 1}]))

        from tap_toggl.streams import Workspaces
        stream = Workspaces(client=client)
        self.assertTrue(stream.check_access())

    def test_check_access_returns_false_on_forbidden(self):
        """check_access returns False when TogglForbiddenError is raised."""
        client = MagicMock()
        client.tasks = MagicMock(side_effect=TogglForbiddenError("403 Forbidden"))

        from tap_toggl.streams import Tasks
        stream = Tasks(client=client)
        self.assertFalse(stream.check_access())

    def test_check_access_returns_true_on_empty_response(self):
        """check_access returns True even when the stream returns no data (empty but authorized)."""
        client = MagicMock()
        client.tags = MagicMock(return_value=iter([]))

        from tap_toggl.streams import Tags
        stream = Tags(client=client)
        self.assertTrue(stream.check_access())


class TestTogglForbiddenError(unittest.TestCase):
    """Tests for the TogglForbiddenError exception in the HTTP client."""

    def test_get_raises_forbidden_error_on_403(self):
        """The Toggl client should raise TogglForbiddenError on HTTP 403."""
        from tap_toggl.toggl import Toggl

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.raise_for_status = MagicMock()

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        # Patch __init__ to avoid real API calls during construction
        with patch.object(Toggl, '__init__', lambda self, **kwargs: None):
            client = Toggl()
            client.api_token = 'fake_token'
            client.session = mock_session

            with self.assertRaises(TogglForbiddenError):
                client._get('https://api.track.toggl.com/api/v9/workspaces/123/tasks')


class TestDiscoverStreams(unittest.TestCase):
    """Tests for discover_streams()."""

    def setUp(self):
        self.mock_client = MagicMock()

    def test_discover_streams_returns_all_streams(self):
        """discover_streams returns one entry per stream in STREAMS."""
        result = discover_streams(self.mock_client)
        self.assertEqual(len(result), len(STREAMS))

    def test_each_entry_has_required_keys(self):
        """Each catalog entry has stream, tap_stream_id, schema, and metadata."""
        result = discover_streams(self.mock_client)
        for entry in result:
            for key in ("stream", "tap_stream_id", "schema", "metadata"):
                self.assertIn(key, entry, f"Entry missing key: {key}")

    def test_stream_names_match_expected(self):
        """Discovered stream IDs match the keys in STREAMS."""
        result = discover_streams(self.mock_client)
        discovered_names = {e["tap_stream_id"] for e in result}
        self.assertEqual(discovered_names, set(STREAMS.keys()))

    def test_schema_is_dict(self):
        """Each schema is a dictionary."""
        result = discover_streams(self.mock_client)
        for entry in result:
            self.assertIsInstance(entry["schema"], dict,
                                  f"Schema for {entry['stream']} is not a dict")

    def test_metadata_is_list(self):
        """Each metadata is a list."""
        result = discover_streams(self.mock_client)
        for entry in result:
            self.assertIsInstance(entry["metadata"], list,
                                  f"Metadata for {entry['stream']} is not a list")

    def test_schema_has_properties(self):
        """Each schema dict contains a 'properties' key."""
        result = discover_streams(self.mock_client)
        for entry in result:
            self.assertIn("properties", entry["schema"],
                          f"Schema for {entry['stream']} missing 'properties'")
