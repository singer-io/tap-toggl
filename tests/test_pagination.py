import unittest

from base import TogglBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest


@unittest.skip("No streams have sufficient data (>= 20 records) for pagination testing")
class TogglPaginationTest(PaginationTest, TogglBaseTest):
    """Ensure tap can replicate multiple pages of data for streams that use pagination."""

    @staticmethod
    def name():
        return "tap_tester_toggl_pagination_test"

    def streams_to_test(self):
        return self.expected_stream_names()
