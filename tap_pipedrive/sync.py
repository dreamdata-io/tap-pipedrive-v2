# pylint: disable=too-many-lines
from datetime import datetime, timedelta

import singer
from singer import metrics, utils
from singer.utils import strptime_to_utc

from tap_pipedrive.utils import utc_dt_to_since_timestamp

LOGGER = singer.get_logger()
DEFAULT_START_DATE = (datetime.utcnow() - timedelta(days=365 * 2)).isoformat()
BUFFER_SIZE = 100


def update_bookmark_without_write(state, stream, value):
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    state["bookmarks"][stream] = value


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


def write_bookmark(state, stream=None, value=None):
    if stream is not None and value is not None:
        if "bookmarks" not in state:
            state["bookmarks"] = {}
        state["bookmarks"][stream] = value
        LOGGER.info(
            "Stream: {} - Write state, bookmark value: {}".format(stream, value)
        )
    else:
        LOGGER.info("Stream: {} - Write state")
    singer.write_state(state)


def sync_recents(client, last_bookmark_value_dt, state):
    since_timestamp_str = utc_dt_to_since_timestamp(last_bookmark_value_dt)

    with metrics.record_counter("recents") as counter:
        try:
            # the since_timestamp_str bleeds out of the for loop
            for since_timestamp_str, stream_name, record in client.paginate_recents(
                since_timestamp_str
            ):
                write_record(
                    stream_name,
                    record,
                    time_extracted=utils.now(),
                )
                update_bookmark_without_write(state, stream_name, since_timestamp_str)
                counter.increment()
        finally:
            write_bookmark(state, "recents", since_timestamp_str)


def create_sync_non_paginated_func(stream_name, endpoint):
    def inner_sync_non_paginated_func(client, last_bookmark_value_dt, state):
        with metrics.record_counter(stream_name) as counter:
            response_json = client.make_request(endpoint)
            for record in response_json.get("data") or []:
                write_record(
                    stream_name,
                    record,
                    time_extracted=utils.now(),
                )
                counter.increment()

    return inner_sync_non_paginated_func


STREAMS = [
    (
        "activity_types",
        create_sync_non_paginated_func("activity_types", "activityTypes"),
    ),
    ("stage_type", create_sync_non_paginated_func("stage_type", "stages")),
    ("currencies", create_sync_non_paginated_func("currencies", "currencies")),
    ("recents", sync_recents),
]


def sync(client, config, state):
    start_date = config.get("start_date") or DEFAULT_START_DATE
    stream_name = None
    try:
        for stream_name, sync_func in STREAMS:
            update_currently_syncing(state, stream_name)
            initial_bookmark_value = get_bookmark(state, "recents", start_date)
            initial_bookmark_value_dt = strptime_to_utc(initial_bookmark_value)
            sync_func(client, initial_bookmark_value_dt, state)
    except:
        LOGGER.exception(f"got error during processing of stream: '{stream_name}'")
        raise
    finally:
        update_currently_syncing(state)
