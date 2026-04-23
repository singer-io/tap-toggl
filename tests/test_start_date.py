"""Test that tap respects the start_date configuration property."""
from base import TogglBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class TogglStartDateTest(StartDateTest, TogglBaseTest):
    """Instantiate start date according to the desired data set and run the test."""

    @staticmethod
    def name():
        return "tap_tester_toggl_start_date_test"

    def streams_to_test(self):
        # Exclude FULL_TABLE streams (users) — they don't obey start_date
        streams_to_exclude = {"users"}
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2019-01-01T00:00:00Z"

    @property
    def start_date_2(self):
        return "2023-01-01T00:00:00Z"
