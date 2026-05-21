"""Unit tests for tap_toggl.__init__ module."""

import json
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

from tap_toggl import (
    do_discover,
    do_sync,
    stream_is_selected,
    get_selected_streams,
    populate_class_schemas,
    ensure_credentials_are_authorized,
    REQUIRED_CONFIG_KEYS,
)
from tap_toggl.streams import STREAMS


def _make_catalog_entry(stream_id, selected=True):
    """Build a minimal mock catalog entry."""
    mdata_entry = {"breadcrumb": [], "metadata": {"selected": selected}}
    mock_entry = MagicMock()
    mock_entry.tap_stream_id = stream_id
    mock_entry.metadata = [mdata_entry]
    schema = MagicMock()
    schema.to_dict.return_value = {"properties": {"id": {"type": "integer"}}}
    mock_entry.schema = schema
    return mock_entry

def _make_catalog(stream_ids, selected=True):
    """Build a mock Catalog with the given stream IDs."""
    catalog = MagicMock()
    catalog.streams = [_make_catalog_entry(sid, selected) for sid in stream_ids]
    return catalog


class TestRequiredConfigKeys(unittest.TestCase):
    """Tests for the REQUIRED_CONFIG_KEYS constant."""

    def test_all_required_keys_present(self):
        """REQUIRED_CONFIG_KEYS contains all 4 expected keys."""
        expected = {"api_token", "start_date", "user_agent", "detailed_report_trailing_days"}
        self.assertEqual(set(REQUIRED_CONFIG_KEYS), expected)


class TestStreamIsSelected(unittest.TestCase):
    """Tests for stream_is_selected()."""

    def test_selected_true(self):
        """Returns True when metadata has selected=True at root breadcrumb."""
        mdata = {(): {"selected": True}}
        self.assertTrue(stream_is_selected(mdata))

    def test_selected_false(self):
        """Returns False when metadata has selected=False."""
        mdata = {(): {"selected": False}}
        self.assertFalse(stream_is_selected(mdata))

    def test_no_selected_key(self):
        """Returns False when 'selected' key is absent."""
        mdata = {(): {}}
        self.assertFalse(stream_is_selected(mdata))

    def test_empty_mdata(self):
        """Returns False for completely empty metadata map."""
        self.assertFalse(stream_is_selected({}))


class TestGetSelectedStreams(unittest.TestCase):
    """Tests for get_selected_streams()."""

    def test_returns_selected_stream_names(self):
        """Returns IDs of streams whose metadata is selected=True."""
        catalog = _make_catalog(["workspaces", "clients", "projects"], selected=True)
        result = get_selected_streams(catalog)
        self.assertIn("workspaces", result)
        self.assertIn("clients", result)
        self.assertIn("projects", result)

    def test_excludes_unselected_streams(self):
        """Excludes streams with selected=False."""
        catalog = _make_catalog(["workspaces"], selected=False)
        result = get_selected_streams(catalog)
        self.assertNotIn("workspaces", result)

    def test_empty_catalog_returns_empty_list(self):
        """Empty catalog returns empty list."""
        catalog = _make_catalog([])
        self.assertEqual(get_selected_streams(catalog), [])


class TestPopulateClassSchemas(unittest.TestCase):
    """Tests for populate_class_schemas()."""

    def test_selected_stream_gets_stream_attribute_set(self):
        """Stream instances in STREAMS get their .stream attribute set for selected entries."""
        entry = _make_catalog_entry("workspaces", selected=True)
        catalog = MagicMock()
        catalog.streams = [entry]

        populate_class_schemas(catalog, ["workspaces"])
        self.assertEqual(STREAMS["workspaces"].stream, entry)

    def test_unselected_stream_not_populated(self):
        """Unselected streams are not assigned to STREAMS."""
        STREAMS["clients"].stream = None
        entry = _make_catalog_entry("clients", selected=False)
        catalog = MagicMock()
        catalog.streams = [entry]

        populate_class_schemas(catalog, [])
        self.assertIsNone(STREAMS["clients"].stream)


class TestEnsureCredentialsAreAuthorized(unittest.TestCase):
    """Tests for ensure_credentials_are_authorized()."""

    def test_calls_is_authorized(self):
        """ensure_credentials_are_authorized calls client.is_authorized()."""
        mock_client = MagicMock()
        ensure_credentials_are_authorized(mock_client)
        mock_client.is_authorized.assert_called_once()


class TestDoDiscover(unittest.TestCase):
    """Tests for do_discover()."""

    @patch("tap_toggl.discover_streams")
    def test_do_discover_outputs_json(self, mock_discover):
        """do_discover writes a JSON catalog to stdout."""
        mock_discover.return_value = [
            {"stream": "workspaces", "tap_stream_id": "workspaces",
             "schema": {"properties": {}}, "metadata": []}
        ]
        mock_client = MagicMock()

        captured = StringIO()
        with patch("sys.stdout", captured):
            do_discover(mock_client)

        output = captured.getvalue()
        catalog = json.loads(output)
        self.assertIn("streams", catalog)
        self.assertEqual(len(catalog["streams"]), 1)

    @patch("tap_toggl.discover_streams")
    def test_do_discover_calls_discover_streams(self, mock_discover):
        """do_discover calls discover_streams with the client."""
        mock_discover.return_value = []
        mock_client = MagicMock()

        with patch("sys.stdout", StringIO()):
            do_discover(mock_client)

        mock_discover.assert_called_once_with(mock_client)


class TestDoSync(unittest.TestCase):
    """Tests for do_sync()."""

    @patch("tap_toggl.sync_stream")
    @patch("tap_toggl.singer.write_schema")
    @patch("tap_toggl.singer.write_state")
    def test_do_sync_skips_unselected_streams(self, mock_write_state, mock_write_schema, mock_sync):
        """do_sync skips streams not in the selected list."""
        mock_client = MagicMock()
        mock_client.is_authorized.return_value = True

        catalog = _make_catalog(["workspaces"], selected=False)
        state = {}

        do_sync(mock_client, catalog, state)

        mock_write_schema.assert_not_called()
        mock_sync.assert_not_called()

    @patch("tap_toggl.sync_stream")
    @patch("tap_toggl.singer.write_schema")
    @patch("tap_toggl.singer.write_state")
    def test_do_sync_writes_schema_for_selected_streams(self, mock_write_state, mock_write_schema, mock_sync):
        """do_sync calls write_schema for each selected stream."""
        mock_client = MagicMock()
        mock_client.is_authorized.return_value = True
        mock_sync.return_value = 0

        catalog = _make_catalog(["workspaces"], selected=True)
        state = {}

        do_sync(mock_client, catalog, state)

        mock_write_schema.assert_called_once()
        first_call_args = mock_write_schema.call_args[0]
        self.assertEqual(first_call_args[0], "workspaces")

    @patch("tap_toggl.sync_stream")
    @patch("tap_toggl.singer.write_schema")
    @patch("tap_toggl.singer.write_state")
    def test_do_sync_writes_final_state(self, mock_write_state, mock_write_schema, mock_sync):
        """do_sync calls write_state after all streams are processed."""
        mock_client = MagicMock()
        mock_client.is_authorized.return_value = True
        mock_sync.return_value = 0

        catalog = _make_catalog(["workspaces"], selected=True)
        state = {}

        do_sync(mock_client, catalog, state)

        # write_state is called at least once at the end
        self.assertTrue(mock_write_state.called)
