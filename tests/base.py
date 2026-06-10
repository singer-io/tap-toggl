import os

from tap_tester.base_suite_tests.base_case import BaseCase


class TogglBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """

    start_date = "2026-03-01T00:00:00Z"
    IS_FORBIDDEN_STREAM = "is-forbidden-stream"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-toggl"

    @staticmethod
    def get_type():
        """The expected url route ending."""
        return "platform.toggl"

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": "2026-03-01T00:00:00Z",
            "detailed_report_trailing_days": 1,
        }

        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        return {
            "api_token": os.getenv("TAP_TOGGL_API_TOKEN"),
        }

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "workspaces": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "clients": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "groups": {
                cls.PRIMARY_KEYS: {"group_id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "projects": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "tasks": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "tags": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "users": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 1,
            },
            "workspace_users": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "time_entries": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
        }
    
    def expected_stream_names(self):
        """The expected stream names and exclude forbidden streams."""
        return {
            stream_name
            for stream_name, metadata in self.expected_metadata().items()
            if not metadata.get(self.IS_FORBIDDEN_STREAM, False)
        }