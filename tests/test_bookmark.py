"""Test tap sets a bookmark and respects it for the next sync of a stream."""
from base import TogglBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class TogglBookmarkTest(BookmarkTest, TogglBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream."""

    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "clients": {"at": "2026-03-01T00:00:00Z"},
            "projects": {"at": "2026-03-01T00:00:00Z"},
            "tags": {"at": "2026-03-01T00:00:00Z"},
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

    def test_first_vs_second_records(self):
        for stream in self.test_streams:
            with self.subTest(stream=stream):
                replication_method = self.expected_replication_methods.get(stream,{})

                if replication_method == self.INCREMENTAL:

                    # gather results
                    sync_1_records = [
                        record['data'] for record in
                        self.synced_records_1.get(stream, {}).get('messages', [])
                        if record.get('action') == 'upsert']

                    # remove records for sync2 that were inserted after sync 1
                    expected_replication_key = self.expected_replication_keys(stream)
                    # Make sure this is not a compound replication key
                    assert len(expected_replication_key) == 1
                    expected_replication_key = next(iter(expected_replication_key))

                    sync_2_records = [
                        record['data'] for record in
                        self.synced_records_2.get(stream, {}).get('messages', [])
                        if record.get('action') == 'upsert'
                        and self.parse_date(record['data'][expected_replication_key])
                        <= self.parse_date(self.bookmark_values_1.get(stream,{}))]

                    # Verify the number of records in the 2nd sync is less then the first
                    self.assertLessEqual(len(sync_2_records), len(sync_1_records))