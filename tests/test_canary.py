from base import TogglBaseTest
from tap_tester import connections, runner


class TogglCanaryTest(TogglBaseTest):
    """Test tap discovery mode and sync mode"""

    @staticmethod
    def name():
        return "tap_tester_toggl_canary_test"

    def test_run(self):
        """
        - Verify expected streams are synced
        """
        streams_to_test = self.expected_stream_names() - {"tasks", "tags", "groups", "projects", "time_entries", "clients"}

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the streams we want to test
        our_catalogs = [c for c in found_catalogs
                        if c.get('tap_stream_id') in streams_to_test]
        self.select_all_streams_and_fields(conn_id, our_catalogs)

        record_count = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify expected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(streams_to_test, synced_stream_names)
