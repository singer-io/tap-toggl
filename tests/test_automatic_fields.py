"""Test that with no fields selected, automatic fields are still replicated."""
from base import TogglBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class TogglAutomaticFieldsTest(MinimumSelectionTest, TogglBaseTest):
    """Test that with no fields selected for a stream, automatic fields are still replicated."""

    @staticmethod
    def name():
        return "tap_tester_toggl_automatic_fields_test"

    def streams_to_test(self):
        # Due to test data not present excluding streams
        streams_to_exclude = {
            'groups',
            'tasks',
            'time_entries'
        }
        return self.expected_stream_names().difference(streams_to_exclude)
