# pylint: disable=too-many-lines
from datetime import date, datetime, timedelta
import time
from urllib.parse import urlparse
from dateutil.parser import parse
from collections import defaultdict
import json

import singer
from singer import metrics, utils
from singer.utils import strptime_to_utc

from tap_pipedrive.utils import utc_dt_to_since_timestamp

LOGGER = singer.get_logger()
DEFAULT_START_DATE = (datetime.utcnow() - timedelta(days=365 * 2)).isoformat()
BUFFER_SIZE = 100


def update_currently_syncing(state, stream_name=None):

    if (stream_name is None) and ("currently_syncing" in state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.error("Stream: {} - OS Error writing record".format(stream_name))
        LOGGER.error("record: {}".format(record))
        raise err


def get_bookmark(state, stream, default):
    # default only populated on initial sync
    if (state is None) or ("bookmarks" not in state):
        return default
    return state.get("bookmarks", {}).get(stream, default)


def write_bookmark(state, stream, value):
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    state["bookmarks"][stream] = value
    LOGGER.info("Stream: {} - Write state, bookmark value: {}".format(stream, value))
    singer.write_state(state)


def sync_recents(client, config, state):
    start_date = config.get("start_date") or DEFAULT_START_DATE

    initial_bookmark_value = get_bookmark(state, "recents", start_date)
    last_bookmark_value_dt = strptime_to_utc(initial_bookmark_value)
    last_commited_timestamp = since_timestamp_str = utc_dt_to_since_timestamp(
        last_bookmark_value_dt
    )

    buffer = []

    with metrics.record_counter("recents") as counter:
        try:
            # the since_timestamp_str bleeds out of the for loop
            for since_timestamp_str, stream_name, record in client.paginate_recents(
                since_timestamp_str
            ):
                buffer.append((stream_name, record))
                if len(buffer) >= BUFFER_SIZE:
                    for stream_name, record in buffer:
                        write_record(
                            stream_name,
                            record,
                            time_extracted=utils.now(),
                        )
                    buffer.clear()
                    if last_commited_timestamp != since_timestamp_str:
                        write_bookmark(state, "recents", since_timestamp_str)
                    last_commited_timestamp = since_timestamp_str
            counter.increment()
        finally:
            for stream_name, record in buffer:
                write_record(
                    stream_name,
                    record,
                    time_extracted=utils.now(),
                )
            write_bookmark(state, "recents", since_timestamp_str)


STREAMS = {"recents": sync_recents}


def sync(client, config, state):
    start_date = config.get("start_date") or DEFAULT_START_DATE
    start_date = strptime_to_utc(start_date)
    stream_name = None
    try:
        for stream_name, sync_func in STREAMS.items():
            sync_func(client, config, state)
    except:
        LOGGER.exception(f"got error during processing of stream: '{stream_name}'")
        exit(1)
