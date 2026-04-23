"""Unit tests for tap_toggl.discover module."""

import unittest
from unittest.mock import MagicMock

from tap_toggl.discover import discover_streams
from tap_toggl.streams import STREAMS


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
