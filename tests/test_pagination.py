from base import TogglBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest


class TogglPaginationTest(PaginationTest, TogglBaseTest):
    """Ensure tap can replicate multiple pages of data for streams that use pagination."""

    @staticmethod
    def name():
        return "tap_tester_toggl_pagination_test"

    def streams_to_test(self):
        # Exclude streams that don't have enough test data to exceed one page
        streams_to_exclude = set()
        return self.expected_stream_names().difference(streams_to_exclude)
