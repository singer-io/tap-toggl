import os

from tap_tester.base_suite_tests.base_case import BaseCase


class TogglBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """

    PRIMARY_KEYS = "table-key-properties"
    START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    BOOKMARK_COMPARISON_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    REPLICATION_KEYS = "valid-replication-keys"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    OBEYS_START_DATE = "obey-start-date"
    start_date = "2026-03-01T00:00:00Z"
    IS_FORBIDDEN_STREAM = "is-forbidden-stream"

    def expected_replication_method(self):
        """Return a dictionary with key of table name and value of replication
        method."""
        return {
            table: properties.get(self.REPLICATION_METHOD, None)
            for table, properties in self.expected_metadata().items()
        }

    def setUp(self):
        """Checking required environment variables."""
        if os.getenv("TAP_TOGGL_API_TOKEN", None) is None:
            raise Exception("Missing test-required environment variables")

    def get_type(self):
        """The expected url route ending."""
        return "platform.toggl"
    
    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-toggl"

    def expected_metadata(self):
        """The expected streams and metadata about the streams."""
        return {
            "workspaces": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"at"},
                self.OBEYS_START_DATE: True,
            },
            "clients": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"at"},
                self.OBEYS_START_DATE: True,
            },
            "groups": {
                self.PRIMARY_KEYS: {"group_id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"at"},
                self.OBEYS_START_DATE: True,
            },
            "projects": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"at"},
                self.OBEYS_START_DATE: True,
            },
            "tasks": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"at"},
                self.OBEYS_START_DATE: True,
            },
            "tags": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"at"},
                self.OBEYS_START_DATE: True,
            },
            "users": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: False,
            },
            "workspace_users": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"at"},
                self.OBEYS_START_DATE: True,
            },
            "time_entries": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated"},
                self.OBEYS_START_DATE: True,
            },
        }

    def expected_streams(self):
        """A set of expected stream names."""
        return {
            stream_name
            for stream_name, metadata in self.expected_metadata().items()
            if not metadata.get(self.IS_FORBIDDEN_STREAM, False)
        }

    def expected_primary_keys(self):
        """Return a dictionary with key of table name and value as a set of
        primary key fields."""
        return {
            table: properties.get(self.PRIMARY_KEYS, set()) for table, properties in self.expected_metadata().items()
        }

    def parse_date(self, date_value):
        """Pass in string-formatted-datetime, parse the value, and return it as
        an unformatted datetime object."""
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d",
        }
        for date_format in date_formats:
            try:
                date_stripped = dt.strptime(date_value, date_format)
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError(f"Tests do not account for dates of this format: {date_value}")

    def expected_replication_keys(self):
        """Return a dictionary with key of table name and value as a set of
        replication key fields."""
        return {
            table: properties.get(self.REPLICATION_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }

    def get_credentials(self):
        """Authentication information for the test account."""
        return {"api_token": os.getenv("TAP_TOGGL_API_TOKEN")}
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