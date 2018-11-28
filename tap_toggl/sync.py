
# 
# Module dependencies.
# 

import json
import singer
import singer.metrics as metrics
from singer import metadata
from singer import Transformer

logger = singer.get_logger()


def sync_stream(state, instance):
    stream = instance.stream

    with metrics.record_counter(stream.tap_stream_id) as counter:
        for (stream, record) in instance.sync(state):
            counter.increment()

            try:
                with Transformer() as transformer:
                    record = transformer.transform(record, stream.schema.to_dict(), metadata.to_map(stream.metadata))
                singer.write_record(stream.tap_stream_id, record)
                if instance.replication_method == "INCREMENTAL":
                    singer.write_state(state)

            except Exception as e:
                logger.error('Handled exception: {error}'.format(error=str(e)))
                continue

        return counter.value
