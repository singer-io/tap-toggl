"""Test tap sets a bookmark and respects it for the next sync of a stream."""
from base import TogglBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class TogglBookmarkTest(BookmarkTest, TogglBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream."""

    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "workspaces": {"at": "2026-03-01T00:00:00Z"},
            "clients": {"at": "2026-03-01T00:00:00Z"},
            "groups": {"at": "2026-03-01T00:00:00Z"},
            "projects": {"at": "2026-03-01T00:00:00Z"},
            "tasks": {"at": "2026-03-01T00:00:00Z"},
            "tags": {"at": "2026-03-01T00:00:00Z"},
            "workspace_users": {"at": "2026-03-01T00:00:00Z"},
        }
    }

    @staticmethod
    def name():
        return "tap_tester_toggl_bookmark_test"

    def streams_to_test(self):
        # Exclude FULL_TABLE streams, streams with no test data, and streams
        # with < 2 records
        streams_to_exclude = {
            "users",
            "time_entries",
            "groups",
            "tasks",
            "workspaces",
            "workspace_users",
        }
        return self.expected_stream_names().difference(streams_to_exclude)
