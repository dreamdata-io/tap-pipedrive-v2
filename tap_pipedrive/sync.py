# pylint: disable=too-many-lines
from datetime import datetime, timedelta
import json
from streamlib import write_record, write_state, logger, exceptions
import dateutil.parser
import pytz

from collections import Counter
from tap_pipedrive.utils import utc_dt_to_since_timestamp

DEFAULT_START_DATE = (datetime.utcnow() - timedelta(days=365 * 2)).isoformat()
BUFFER_SIZE = 100

STREAM_NAME_RECORD_COUNTER = Counter()
STREAM_NAME_RECORD_SINCE_TIMESTAMP = {}
PROGRESS_LOG_FREQUENCY = 2000


def now():
    return datetime.utcnow().replace(tzinfo=pytz.UTC)


def strptime_to_utc(dtimestr):
    d_object = dateutil.parser.parse(dtimestr)
    if d_object.tzinfo is None:
        return d_object.replace(tzinfo=pytz.UTC)
    else:
        return d_object.astimezone(tz=pytz.UTC)


def update_bookmark_without_write(state, stream, value):
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    state["bookmarks"][stream] = value


def update_currently_syncing(state, stream_name=None):
    if (stream_name is None) and ("currently_syncing" in state):
        del state["currently_syncing"]
    else:
        previous_stream_name = state.get("currently_syncing", None)
        if previous_stream_name is not None:
            time_reached = STREAM_NAME_RECORD_SINCE_TIMESTAMP.get(
                previous_stream_name, None
            )
            if time_reached:
                record_count = STREAM_NAME_RECORD_COUNTER.get(
                    previous_stream_name, "not found"
                )
                logger.info(
                    f"PROGRESS: stream_name: '{stream_name}' total records produced: {record_count}, time_reached: '{time_reached}'"
                )
        # singer.set_currently_syncing(state, stream_name)
    logger.info(f"currently syncing {stream_name}")
    write_state(state)


def custom_write_record(stream_name, record, time_reached=None):
    try:
        write_record(
            stream_name, record)
        STREAM_NAME_RECORD_SINCE_TIMESTAMP[stream_name] = time_reached
        STREAM_NAME_RECORD_COUNTER.update([stream_name])
        record_count = STREAM_NAME_RECORD_COUNTER.get(stream_name)
        time_reached = f", time_reached: '{time_reached}'" if time_reached else ""

        if record_count % PROGRESS_LOG_FREQUENCY == 0:
            logger.info(
                f"PROGRESS: stream_name: '{stream_name}' records produced yet: {record_count}{time_reached}"
            )
    except OSError as err:
        logger.error(
            "Stream: {} - OS Error writing record".format(stream_name))
        logger.error("record: {}".format(record))
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
        logger.info(
            "Stream: {} - Write state, bookmark value: {}".format(
                stream, value)
        )
    else:
        logger.info("Stream: {} - Write state")
    write_state(state)


def sync_recents(client, last_bookmark_value_dt, state):
    since_timestamp_str = utc_dt_to_since_timestamp(last_bookmark_value_dt)

    # with metrics.record_counter("recents") as counter:
    try:
        # the since_timestamp_str bleeds out of the for loop
        for since_timestamp_str, stream_name, record in client.paginate_recents(
            since_timestamp_str
        ):
            custom_write_record(
                stream_name,
                record,
                time_reached=since_timestamp_str,
            )
            update_bookmark_without_write(
                state, stream_name, since_timestamp_str)
            # counter.increment()
    finally:
        write_bookmark(state, "recents", since_timestamp_str)


def create_sync_non_paginated_func(stream_name, endpoint):
    def inner_sync_non_paginated_func(client, last_bookmark_value_dt, state):
        # with metrics.record_counter(stream_name) as counter:
        response_json = client.make_request(endpoint)
        for record in response_json.get("data") or []:
            custom_write_record(
                stream_name,
                record,
            )
            # counter.increment()

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
    logger.info(f"starting sync with start_date: {start_date}")
    stream_name = None
    try:
        for stream_name, sync_func in STREAMS:
            update_currently_syncing(state, stream_name)
            initial_bookmark_value = get_bookmark(state, "recents", start_date)
            initial_bookmark_value_dt = strptime_to_utc(initial_bookmark_value)
            sync_func(client, initial_bookmark_value_dt, state)
    except:
        logger.exception(
            f"got error during processing of stream: '{stream_name}'")
        raise
    finally:

        logger.info(
            f"COMPLETE STATS: stream_name_time_reached: {json.dumps(STREAM_NAME_RECORD_SINCE_TIMESTAMP)} stream_name_records_produced: {STREAM_NAME_RECORD_COUNTER.most_common()}"
        )
        update_currently_syncing(state)
