
# 
# Module dependencies.
# 

import os
import json
import datetime
import pytz
import singer
from singer import metadata
from singer import utils
from singer.metrics import Point
from dateutil.parser import parse


logger = singer.get_logger()
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


    def __init__(self, client=None):
        self.client = client


    def get_bookmark(self, state):
        return singer.get_bookmark(state, self.name, self.replication_key)


    def update_bookmark(self, state, value):
        singer.write_bookmark(state, self.name, self.replication_key, value)


    def is_bookmark_old(self, state, value):
        current_bookmark = self.get_bookmark(state)
        if current_bookmark is None:
            return True
        return utils.strptime_with_tz(value) > utils.strptime_with_tz(current_bookmark)


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


    # The main sync function.
    def sync(self, state):
        get_data = getattr(self.client, self.name)
        bookmark = self.get_bookmark(state)
        res = get_data(self.replication_key, bookmark)

        if self.replication_method == "INCREMENTAL":
            for item in res:
                try:
                    if self.is_bookmark_old(state, item[self.replication_key]):
                        self.update_bookmark(state, item[self.replication_key])
                        yield (self.stream, item)

                except Exception as e:
                    logger.error('Handled exception: {error}'.format(error=str(e)))
                    pass

        elif self.replication_method == "FULL_TABLE":
            for item in res:
                yield (self.stream, item)

        else:
            raise Exception('Replication key not defined for {stream}'.format(self.name))



class Workspaces(Stream):
    name = "a_workspaces"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = [ "id" ]


class Clients(Stream):
    name = "clients"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = [ "id" ]


class Groups(Stream):
    name = "groups"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = [ "id" ]


class Projects(Stream):
    name = "projects"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = [ "id" ]


class Tasks(Stream):
    name = "tasks"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = [ "id" ]


class Tags(Stream):
    name = "tags"
    replication_method = "FULL_TABLE"
    key_properties = [ "id" ]


class Users(Stream):
    name = "users"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = [ "id" ]


class WorkspaceUsers(Stream):
    name = "workspace_users"
    replication_method = "INCREMENTAL"
    replication_key = "at"
    key_properties = [ "id" ]


class TimeEntries(Stream):
    name = "time_entries"
    replication_method = "INCREMENTAL"
    replication_key = "updated"
    key_properties = [ "id" ]



STREAMS = {
    "a_workspaces": Workspaces,
    "clients": Clients,
    "groups": Groups,
    "projects": Projects,
    "tasks": Tasks,
    "tags": Tags,
    "users": Users,
    "workspace_users": WorkspaceUsers,
    "time_entries": TimeEntries
}






