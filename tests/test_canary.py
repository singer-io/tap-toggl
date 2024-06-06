from base import TogglBaseTest
from tap_tester import connections, menagerie, runner


class TogglCanaryTest(TogglBaseTest):
    """Test tap discovery mode and sync mode"""

    def name(self):
        return "tap_tester_toggl_canary_test"

    def test_run(self):
        """
        - Verify all streams hare synced
        """
        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.perform_and_verify_table_and_field_selection(conn_id, found_catalogs)

        record_count = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(streams_to_test, synced_stream_names)