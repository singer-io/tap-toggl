"""Test that tap respects the start_date configuration property."""
import unittest

from base import TogglBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


@unittest.skip(
    "The tap does not filter INCREMENTAL streams by start_date"
    "it fetches all records from the API and filters client-side by bookmark. "
    "Only time_entries uses start_date for date windows, but has no test data."
)
class TogglStartDateTest(StartDateTest, TogglBaseTest):
    """Instantiate start date according to the desired data set and run the test."""

    @staticmethod
    def name():
        return "tap_tester_toggl_start_date_test"

    def streams_to_test(self):
        # Exclude all streams that don't filter by start_date.
        streams_to_exclude = {
            "users",
            "time_entries",
            "groups",
            "tasks",
            "workspace_users",
            "workspaces",
            "clients",
            "projects",
            "tags",
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2026-03-01T00:00:00Z"

    @property
    def start_date_2(self):
        return "2026-04-11T00:00:00Z"
