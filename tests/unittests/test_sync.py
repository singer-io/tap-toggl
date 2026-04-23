"""Unit tests for tap_toggl.sync module."""

import unittest
from unittest.mock import MagicMock, patch

from tap_toggl.sync import sync_stream
from tap_toggl.streams import Workspaces, Users


def _make_catalog_entry(tap_stream_id, schema_dict=None, metadata=None):
    """Build a minimal singer CatalogEntry-like mock."""
    entry = MagicMock()
    entry.tap_stream_id = tap_stream_id
    schema_mock = MagicMock()
    schema_mock.to_dict.return_value = schema_dict or {"properties": {}}
    entry.schema = schema_mock
    entry.metadata = metadata or []
    return entry


class TestSyncStreamIncremental(unittest.TestCase):
    """Tests for sync_stream() with INCREMENTAL streams."""

    @patch("tap_toggl.sync.singer.write_record")
    @patch("tap_toggl.sync.singer.write_state")
    def test_records_are_written(self, mock_write_state, mock_write_record):
        """sync_stream writes a record for each item yielded by the stream."""
        catalog_entry = _make_catalog_entry("workspaces")

        mock_client = MagicMock()
        instance = Workspaces(client=mock_client)
        instance.stream = catalog_entry
        instance.replication_method = "INCREMENTAL"

        # Stub instance.sync to yield two records
        instance.sync = MagicMock(return_value=iter([
            (catalog_entry, {"id": 1, "at": "2021-01-01T00:00:00Z"}),
            (catalog_entry, {"id": 2, "at": "2021-02-01T00:00:00Z"}),
        ]))

        state = {}
        count = sync_stream(state, instance)

        self.assertEqual(count, 2)
        self.assertEqual(mock_write_record.call_count, 2)

    @patch("tap_toggl.sync.singer.write_record")
    @patch("tap_toggl.sync.singer.write_state")
    def test_write_state_called_for_incremental(self, mock_write_state, mock_write_record):
        """write_state is called after each record for INCREMENTAL streams."""
        catalog_entry = _make_catalog_entry("workspaces")

        mock_client = MagicMock()
        instance = Workspaces(client=mock_client)
        instance.stream = catalog_entry
        instance.replication_method = "INCREMENTAL"

        instance.sync = MagicMock(return_value=iter([
            (catalog_entry, {"id": 1, "at": "2021-01-01T00:00:00Z"}),
        ]))

        state = {}
        sync_stream(state, instance)

        # write_state should have been called at least once
        self.assertTrue(mock_write_state.called)

    @patch("tap_toggl.sync.singer.write_record")
    @patch("tap_toggl.sync.singer.write_state")
    def test_empty_stream_returns_zero(self, mock_write_state, mock_write_record):
        """sync_stream returns 0 for an empty stream."""
        catalog_entry = _make_catalog_entry("workspaces")

        mock_client = MagicMock()
        instance = Workspaces(client=mock_client)
        instance.stream = catalog_entry
        instance.replication_method = "INCREMENTAL"
        instance.sync = MagicMock(return_value=iter([]))

        state = {}
        count = sync_stream(state, instance)

        self.assertEqual(count, 0)
        mock_write_record.assert_not_called()


class TestSyncStreamFullTable(unittest.TestCase):
    """Tests for sync_stream() with FULL_TABLE streams."""

    @patch("tap_toggl.sync.singer.write_record")
    @patch("tap_toggl.sync.singer.write_state")
    def test_full_table_no_per_record_state(self, mock_write_state, mock_write_record):
        """write_state is NOT called per record for FULL_TABLE streams."""
        catalog_entry = _make_catalog_entry("users")

        mock_client = MagicMock()
        instance = Users(client=mock_client)
        instance.stream = catalog_entry
        instance.replication_method = "FULL_TABLE"

        instance.sync = MagicMock(return_value=iter([
            (catalog_entry, {"id": 1}),
            (catalog_entry, {"id": 2}),
        ]))

        state = {}
        count = sync_stream(state, instance)

        self.assertEqual(count, 2)
        # write_state should NOT be called per record for FULL_TABLE
        mock_write_state.assert_not_called()

    @patch("tap_toggl.sync.singer.write_record")
    @patch("tap_toggl.sync.singer.write_state")
    def test_full_table_writes_correct_stream_id(self, mock_write_state, mock_write_record):
        """write_record is called with the correct stream name."""
        catalog_entry = _make_catalog_entry("users")

        mock_client = MagicMock()
        instance = Users(client=mock_client)
        instance.stream = catalog_entry
        instance.replication_method = "FULL_TABLE"

        instance.sync = MagicMock(return_value=iter([
            (catalog_entry, {"id": 42}),
        ]))

        state = {}
        sync_stream(state, instance)

        args, _ = mock_write_record.call_args
        self.assertEqual(args[0], "users")
