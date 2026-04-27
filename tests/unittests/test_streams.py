"""Unit tests for tap_toggl.streams module."""

import unittest
from unittest.mock import MagicMock

from tap_toggl.streams import (
    Stream,
    Workspaces,
    Clients,
    Groups,
    Projects,
    Tasks,
    Tags,
    Users,
    WorkspaceUsers,
    TimeEntries,
    STREAMS,
    needs_parse_to_date,
)


class TestNeedsParseToDate(unittest.TestCase):
    """Tests for the needs_parse_to_date helper."""

    def test_valid_date_string_returns_true(self):
        """Valid ISO date string returns True."""
        self.assertTrue(needs_parse_to_date("2021-01-01T00:00:00Z"))

    def test_plain_date_string_returns_true(self):
        """Plain YYYY-MM-DD string returns True."""
        self.assertTrue(needs_parse_to_date("2021-01-01"))

    def test_non_date_string_returns_false(self):
        """Non-date string like 'hello' returns False."""
        self.assertFalse(needs_parse_to_date("hello_world"))

    def test_integer_returns_false(self):
        """Non-string input (int) returns False."""
        self.assertFalse(needs_parse_to_date(12345))

    def test_none_returns_false(self):
        """None input returns False."""
        self.assertFalse(needs_parse_to_date(None))


class TestStreamIsBookmarkOld(unittest.TestCase):
    """Tests for Stream.is_bookmark_old."""

    def setUp(self):
        self.stream = Stream()
        self.stream.name = "workspaces"
        self.stream.replication_key = "at"

    def test_no_bookmark_in_state_returns_true(self):
        """No existing bookmark means any value is 'old' (should sync)."""
        self.assertTrue(self.stream.is_bookmark_old({}, "2021-01-01T00:00:00Z"))

    def test_newer_value_returns_true(self):
        """Value newer than bookmark returns True (should sync)."""
        state = {"bookmarks": {"workspaces": {"at": "2020-01-01T00:00:00Z"}}}
        self.assertTrue(self.stream.is_bookmark_old(state, "2021-01-01T00:00:00Z"))

    def test_older_value_returns_false(self):
        """Value older than bookmark returns False (already synced)."""
        state = {"bookmarks": {"workspaces": {"at": "2022-01-01T00:00:00Z"}}}
        self.assertFalse(self.stream.is_bookmark_old(state, "2021-01-01T00:00:00Z"))

    def test_none_value_returns_false(self):
        """None value returns False when bookmark exists."""
        state = {"bookmarks": {"workspaces": {"at": "2020-01-01T00:00:00Z"}}}
        self.assertFalse(self.stream.is_bookmark_old(state, None))


class TestStreamSessionBookmark(unittest.TestCase):
    """Tests for Stream session bookmark methods."""

    def setUp(self):
        self.stream = Stream()

    def test_is_session_bookmark_old_when_none(self):
        """session_bookmark=None means any value is old."""
        self.assertTrue(self.stream.is_session_bookmark_old("2021-01-01T00:00:00Z"))

    def test_is_session_bookmark_old_with_newer_value(self):
        """Value newer than session_bookmark returns True."""
        self.stream.session_bookmark = "2020-01-01T00:00:00Z"
        self.assertTrue(self.stream.is_session_bookmark_old("2021-01-01T00:00:00Z"))

    def test_is_session_bookmark_old_with_older_value(self):
        """Value older than session_bookmark returns False."""
        self.stream.session_bookmark = "2022-01-01T00:00:00Z"
        self.assertFalse(self.stream.is_session_bookmark_old("2021-01-01T00:00:00Z"))

    def test_update_session_bookmark_if_old_sets_newer(self):
        """update_session_bookmark_if_old sets bookmark when value is newer."""
        self.stream.update_session_bookmark_if_old("2021-01-01T00:00:00Z")
        self.assertEqual(self.stream.session_bookmark, "2021-01-01T00:00:00Z")

    def test_update_session_bookmark_if_old_keeps_newer_existing(self):
        """Existing newer session_bookmark is not overwritten by older value."""
        self.stream.session_bookmark = "2022-01-01T00:00:00Z"
        self.stream.update_session_bookmark_if_old("2021-01-01T00:00:00Z")
        self.assertEqual(self.stream.session_bookmark, "2022-01-01T00:00:00Z")


class TestStreamGetBookmark(unittest.TestCase):
    """Tests for Stream.get_bookmark."""

    def setUp(self):
        self.stream = Stream()
        self.stream.name = "workspaces"
        self.stream.replication_key = "at"

    def test_get_bookmark_present(self):
        """Returns bookmark value when present in state."""
        state = {"bookmarks": {"workspaces": {"at": "2021-01-01T00:00:00Z"}}}
        self.assertEqual(self.stream.get_bookmark(state), "2021-01-01T00:00:00Z")

    def test_get_bookmark_absent(self):
        """Returns None when bookmark is not present."""
        self.assertIsNone(self.stream.get_bookmark({}))


class TestStreamBookmarkNormalization(unittest.TestCase):
    """Tests that update_bookmark_if_old normalizes timestamps to %Y-%m-%dT%H:%M:%S.%fZ."""

    def setUp(self):
        self.stream = Stream()
        self.stream.name = "clients"
        self.stream.replication_key = "at"

    def test_normalizes_plus_offset_to_z_format(self):
        """Bookmark '2026-04-22T17:20:07+00:00' is normalized to '.000000Z' format."""
        state = {}
        self.stream.update_bookmark_if_old(state, "2026-04-22T17:20:07+00:00")
        import singer
        bookmark = singer.get_bookmark(state, "clients", "at")
        self.assertEqual(bookmark, "2026-04-22T17:20:07.000000Z")

    def test_normalizes_microseconds_z_format(self):
        """Bookmark '2026-04-22T17:24:09.862844Z' is preserved in '.%fZ' format."""
        state = {}
        self.stream.update_bookmark_if_old(state, "2026-04-22T17:24:09.862844Z")
        import singer
        bookmark = singer.get_bookmark(state, "clients", "at")
        self.assertEqual(bookmark, "2026-04-22T17:24:09.862844Z")

    def test_none_value_not_normalized(self):
        """None value is written as-is without normalization."""
        state = {}
        self.stream.update_bookmark_if_old(state, None)
        import singer
        bookmark = singer.get_bookmark(state, "clients", "at")
        self.assertIsNone(bookmark)


class TestStreamSyncIncremental(unittest.TestCase):
    """Tests for Stream.sync with INCREMENTAL replication."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.stream_obj = Workspaces(client=self.mock_client)

        mock_schema = MagicMock()
        mock_catalog_entry = MagicMock()
        mock_catalog_entry.tap_stream_id = "workspaces"
        mock_catalog_entry.schema = mock_schema
        mock_catalog_entry.metadata = []
        self.stream_obj.stream = mock_catalog_entry

    def test_incremental_yields_new_records(self):
        """Records newer than bookmark are yielded."""
        records = [
            {"id": 1, "at": "2021-06-01T00:00:00Z"},
            {"id": 2, "at": "2021-07-01T00:00:00Z"},
        ]
        self.mock_client.workspaces.return_value = iter(records)
        state = {"bookmarks": {"workspaces": {"at": "2021-01-01T00:00:00Z"}}}

        result = list(self.stream_obj.sync(state))
        self.assertEqual(len(result), 2)

    def test_incremental_skips_old_records(self):
        """Records older than bookmark are not yielded."""
        records = [
            {"id": 1, "at": "2019-01-01T00:00:00Z"},
        ]
        self.mock_client.workspaces.return_value = iter(records)
        state = {"bookmarks": {"workspaces": {"at": "2021-01-01T00:00:00Z"}}}

        result = list(self.stream_obj.sync(state))
        self.assertEqual(len(result), 0)

    def test_incremental_skips_string_items(self):
        """String items in response are skipped."""
        self.mock_client.workspaces.return_value = iter(["some_string"])
        state = {}

        result = list(self.stream_obj.sync(state))
        self.assertEqual(len(result), 0)


class TestStreamSyncFullTable(unittest.TestCase):
    """Tests for Stream.sync with FULL_TABLE replication."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.stream_obj = Users(client=self.mock_client)

        mock_schema = MagicMock()
        mock_catalog_entry = MagicMock()
        mock_catalog_entry.tap_stream_id = "users"
        mock_catalog_entry.schema = mock_schema
        mock_catalog_entry.metadata = []
        self.stream_obj.stream = mock_catalog_entry

    def test_full_table_yields_all_records(self):
        """All records are yielded regardless of any state."""
        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        self.mock_client.users.return_value = iter(records)
        state = {}

        result = list(self.stream_obj.sync(state))
        self.assertEqual(len(result), 3)


class TestStreamSyncInvalidReplication(unittest.TestCase):
    """Tests for Stream.sync raising on undefined replication method."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.stream_obj = Stream(client=self.mock_client)
        self.stream_obj.name = "workspaces"
        self.stream_obj.replication_method = "UNKNOWN"
        self.stream_obj.replication_key = "at"

        mock_catalog_entry = MagicMock()
        mock_catalog_entry.tap_stream_id = "workspaces"
        self.stream_obj.stream = mock_catalog_entry

        self.mock_client.workspaces.return_value = iter([{"id": 1, "at": "2021-01-01T00:00:00Z"}])

    def test_unknown_replication_method_raises(self):
        """Unknown replication method raises Exception during sync."""
        with self.assertRaises(Exception) as ctx:
            list(self.stream_obj.sync({}))
        self.assertIn("Replication key not defined", str(ctx.exception))


class TestStreamConstants(unittest.TestCase):
    """Tests for stream class attribute correctness."""

    def test_all_streams_present_in_dict(self):
        """All 9 expected streams are in the STREAMS dict."""
        expected = {"workspaces", "clients", "groups", "projects", "tasks",
                    "tags", "users", "workspace_users", "time_entries"}
        self.assertEqual(set(STREAMS.keys()), expected)

    def test_incremental_streams_have_replication_key(self):
        """All INCREMENTAL streams have a non-None replication_key."""
        for name, cls in STREAMS.items():
            instance = cls()
            if instance.replication_method == "INCREMENTAL":
                self.assertIsNotNone(instance.replication_key,
                                     f"{name} is INCREMENTAL but has no replication_key")

    def test_users_is_full_table(self):
        """Users stream uses FULL_TABLE replication."""
        self.assertEqual(Users.replication_method, "FULL_TABLE")

    def test_time_entries_replication_key(self):
        """TimeEntries uses 'at' as the replication key."""
        self.assertEqual(TimeEntries.replication_key, "at")


class TestStreamLoadSchema(unittest.TestCase):
    """Tests for Stream.load_schema."""

    def test_load_schema_returns_dict(self):
        """load_schema returns a dict with 'properties' key for all streams."""
        for name, cls in STREAMS.items():
            instance = cls()
            schema = instance.load_schema()
            self.assertIsInstance(schema, dict, f"Schema for {name} is not a dict")
            self.assertIn("properties", schema, f"Schema for {name} missing 'properties'")

    def test_load_metadata_returns_list(self):
        """load_metadata returns a non-empty list for all streams."""
        for name, cls in STREAMS.items():
            instance = cls()
            mdata = instance.load_metadata()
            self.assertIsInstance(mdata, list, f"Metadata for {name} is not a list")
            self.assertGreater(len(mdata), 0, f"Metadata for {name} is empty")
