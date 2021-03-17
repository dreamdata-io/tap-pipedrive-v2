#!/usr/bin/env python3

import sys
import json
import argparse
import singer
from singer import metadata, utils
from tap_pipedrive.client import PipedriveClient
from tap_pipedrive.sync import sync


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret",
    "refresh_token",
    "user_agent"
]


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    config = parsed_args.config

    client = PipedriveClient(**config)

    state = {}
    if parsed_args.state:
        state = parsed_args.state

    sync(client=client, config=config, state=state)


if __name__ == "__main__":
    main()
