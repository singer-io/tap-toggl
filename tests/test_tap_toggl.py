
# 
# Module dependencies.
# 

import itertools
import unittest
import tap_toggl.streams as streams

from tap_toggl.streams import Stream
from tap_toggl.toggl import Toggl
from singer.catalog import Catalog
from singer.schema import Schema
from singer.utils import strftime


class TestStreams(unittest.TestCase):
    def test_is_bookmark_old(self):
        bookmarks = {
            "bookmarks": {
                "old_timestamp": {
                    "createdAt": "2011-11-03 18:21:26"
                },
                "current_timestamp": {
                    "createdAt": "2018-11-03 18:21:26"
                }
            }
        }

        now = "2018-11-02 18:21:26"

        OldTimestamp = Stream()
        OldTimestamp.name = "old_timestamp"
        OldTimestamp.replication_key = "createdAt"
        self.assertTrue(Stream.is_bookmark_old(OldTimestamp, bookmarks, now))

        CurrentTimestamp = Stream()
        CurrentTimestamp.name = "current_timestamp"
        CurrentTimestamp.replication_key = "createdAt"
        self.assertFalse(Stream.is_bookmark_old(CurrentTimestamp, bookmarks, now))



if __name__ == '__main__':
    unittest.main()

