#!/usr/bin/env python3
from tap_pipedrive.sync import sync
from tap_pipedrive.client import PipedriveClient


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret",
    "refresh_token",
    "user_agent",
]


def main(config, state=None):

    client = PipedriveClient(**config)

    state = state or {}

    sync(client=client, config=config, state=state)


if __name__ == "__main__":
    main()
