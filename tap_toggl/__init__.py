#!/usr/bin/env python3

#
# Module dependencies.
#

import json
import sys
import singer
from singer import metadata
from tap_toggl.toggl import Toggl
from tap_toggl.discover import discover_streams
from tap_toggl.sync import sync_stream
from tap_toggl.streams import STREAMS


logger = singer.get_logger()


REQUIRED_CONFIG_KEYS = [
    "api_token",
    "start_date",
    "user_agent",
    "detailed_report_trailing_days"
]


def do_discover(client):
    logger.info("Starting discover")
    catalog = {"streams": discover_streams(client)}
    json.dump(catalog, sys.stdout, indent=2)
    logger.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def get_selected_streams(catalog):
    selected_stream_names = []
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        if stream_is_selected(mdata):
            selected_stream_names.append(stream.tap_stream_id)
    return selected_stream_names


def populate_class_schemas(catalog, selected_stream_names):
    for stream in catalog.streams:
        if stream.tap_stream_id in selected_stream_names:
            STREAMS[stream.tap_stream_id].stream = stream


def ensure_credentials_are_authorized(client):
    client.is_authorized()


def do_sync(client, catalog, state):
    ensure_credentials_are_authorized(client)
    selected_stream_names = get_selected_streams(catalog)
    populate_class_schemas(catalog, selected_stream_names)

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id

        mdata = metadata.to_map(stream.metadata)

        if stream_name not in selected_stream_names:
            logger.info("%s: Skipping - not selected", stream_name)
            continue

        key_properties = metadata.get(mdata, (), 'table-key-properties')
        singer.write_schema(stream_name, stream.schema.to_dict(), key_properties)
        logger.info("%s: Starting sync", stream_name)
        instance = STREAMS[stream_name](client)
        instance.stream = stream
        counter_value = sync_stream(state, instance)
        singer.write_state(state)
        logger.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    singer.write_state(state)
    logger.info("Finished sync")


@singer.utils.handle_top_exception(logger)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    creds = {
        "api_token": parsed_args.config['api_token'],
        "trailing_days": parsed_args.config['detailed_report_trailing_days'],
        "user_agent": parsed_args.config['user_agent'],
        "start_date": parsed_args.config['start_date']
    }
    client = Toggl(**creds)

    if parsed_args.discover:
        do_discover(client)
    elif parsed_args.catalog:
        state = parsed_args.state or {}
        do_sync(client, parsed_args.catalog, state)



