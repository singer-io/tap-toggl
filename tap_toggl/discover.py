
#
# Module dependencies.
#

import os

import singer

from tap_toggl.streams import STREAMS
from tap_toggl.exceptions import TogglForbiddenError


LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def _apply_access_checks(client, streams):
    """
    Probe each stream for read access and remove inaccessible streams
    from the list in place.
    Raises TogglForbiddenError if no streams are accessible.
    """
    inaccessible_streams = []

    for stream_entry in streams[:]:
        stream_name = stream_entry['tap_stream_id']
        stream_cls = STREAMS.get(stream_name)
        if stream_cls and not stream_cls(client).check_access():
            inaccessible_streams.append(stream_name)
            streams.remove(stream_entry)

    if not streams:
            raise TogglForbiddenError(
                "HTTP-error-code: 403, Error: The account credentials supplied do not have 'read' access to any "
                "of the streams supported by the tap. Data collection cannot be initiated due to lack of permissions."
            )
    
    if inaccessible_streams:
        LOGGER.warning(
            "These streams have been excluded from the catalog as the account credentials supplied "
            "do not have 'read' access to the following stream(s): %s.",
            ", ".join(inaccessible_streams),
        )


def discover_streams(client):
    streams = []

    for s in STREAMS.values():
        s = s(client)
        schema = singer.resolve_schema_references(s.load_schema())
        streams.append({'stream': s.name, 'tap_stream_id': s.name, 'schema': schema, 'metadata': s.load_metadata()})

    _apply_access_checks(client, streams)

    return streams
