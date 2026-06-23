#
# Module dependencies.
#

import json
import os
from datetime import datetime, timezone

import singer
from dateutil.parser import parse
from singer import metadata, utils

from tap_toggl.exceptions import TogglForbiddenError

LOGGER = singer.get_logger()
KEY_PROPERTIES = ['id']


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def needs_parse_to_date(string):
    if isinstance(string, str):
        try:
            parse(string)
            return True
        except ValueError:
            return False
    return False


class Stream():
    name = None
    replication_method = None
    replication_key = None
    stream = None
    key_properties = KEY_PROPERTIES
    session_bookmark = None

    def __init__(self, client=None):
        self.client = client

    def is_session_bookmark_old(self, value):
        if self.session_bookmark is None:
            return True
        return utils.strptime_with_tz(value) > utils.strptime_with_tz(self.session_bookmark)

    def update_session_bookmark_if_old(self, value):
        if self.is_session_bookmark_old(value):
            self.session_bookmark = value

    def get_bookmark(self, state):
        return singer.get_bookmark(state, self.name, self.replication_key)

    def update_bookmark_if_old(self, state, value):
        if self.is_bookmark_old(state, value):
            # Normalize to consistent ISO 8601 format (%Y-%m-%dT%H:%M:%S.%fZ).
            # The Toggl API returns timestamps in inconsistent formats across
            # endpoints (e.g. "+00:00" vs "Z", with/without microseconds).
            if value is not None:
                value = utils.strftime(utils.strptime_with_tz(value))
            singer.write_bookmark(state, self.name, self.replication_key, value)

    def is_bookmark_old(self, state, value):
        current_bookmark = self.get_bookmark(state)
        if current_bookmark is None:
            return True
        if value is None:
            return False
        return utils.strptime_with_tz(value) >= utils.strptime_with_tz(current_bookmark)

    def load_schema(self):
        schema_file = "schemas/{}.json".format(self.name)
        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)
        return schema

    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(mdata, (), 'table-key-properties', self.key_properties)
        mdata = metadata.write(mdata, (), 'forced-replication-method', self.replication_method)

        if self.replication_key:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or field_name == self.replication_key:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
            else:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

        return metadata.to_list(mdata)

    def is_selected(self):
        return self.stream is not None


    def check_access(self):
        """
        Verify that the API credentials have read access to this stream.
        Returns True if accessible, False if a 403 Forbidden error is raised.
        """
        try:
            get_data = getattr(self.client, self.name)

            # Use a recent bookmark so streams like `time_entries` don't build endpoints
            # across the full configured start_date range during discovery.
            bookmark = utils.strftime(datetime.now(timezone.utc))

            # Consume at most one record to verify access
            for _ in get_data(self.replication_key, bookmark):
                break
            return True
        except TogglForbiddenError as exc:
            LOGGER.warning(
                "Unauthorized Stream: %s, excluding from catalog. HTTP-Error-Message:'%s'",
                self.name,
                str(exc),
            )
            return False


    # The main sync function.
    def sync(self, state):
        get_data = getattr(self.client, self.name)
        bookmark = self.get_bookmark(state)
        res = get_data(self.replication_key, bookmark)

        if self.replication_method == "INCREMENTAL":
            for item in res:
                if type(item) == str:
                    continue
                if self.is_bookmark_old(state, item[self.replication_key]):
                    # must update bookmark when the entire stream is consumed.
                    # instead, we use a temporary `session_bookmark`.
                    self.update_session_bookmark_if_old(item[self.replication_key])
                    yield (self.stream, item)

        elif self.replication_method == "FULL_TABLE":
            for item in res:
                yield (self.stream, item)

        else:
            raise Exception('Replication key not defined for {}'.format(self.name))

        # After the sync, then set the bookmark based off session_bookmark.
        self.update_bookmark_if_old(state, self.session_bookmark)


class Workspaces(Stream):
    name = "workspaces"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = ["id"]


class Clients(Stream):
    name = "clients"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = ["id"]


class Groups(Stream):
    name = "groups"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = [ "group_id" ]


class Projects(Stream):
    name = "projects"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = ["id"]


class Tasks(Stream):
    name = "tasks"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = ["id"]


class Tags(Stream):
    name = "tags"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = [ "id" ]


class Users(Stream):
    name = "users"
    replication_method = "FULL_TABLE"
    key_properties = [ "id" ]


class WorkspaceUsers(Stream):
    name = "workspace_users"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = ["id"]


class TimeEntries(Stream):
    name = "time_entries"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = ["id"]


STREAMS = {
    "workspaces": Workspaces,
    "clients": Clients,
    "groups": Groups,
    "projects": Projects,
    "tasks": Tasks,
    "tags": Tags,
    "users": Users,
    "workspace_users": WorkspaceUsers,
    "time_entries": TimeEntries
}
