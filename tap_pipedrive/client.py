import time
import requests
import singer
from requests.exceptions import ConnectionError, RequestException, HTTPError, Timeout
from json import JSONDecodeError
import json
from streamlib import logger, exceptions

import backoff


BASE_API_URL = "https://api.pipedrive.com/v1"
AUTH_URL = "https://oauth.pipedrive.com/oauth"
PAGINATION_LIMIT = 200


class WaitAndRetry(Exception):
    pass


class PipedriveClient:

    client_id: str = None
    client_secret: str = None
    access_token: str = None
    refresh_token: str = None
    user_agent: str = None

    def __init__(self, **config):
        self.user_agent = config["user_agent"]
        self._session = requests.session()
        self._set_creds(config)
        if self.refresh_token:
            self.request_refresh_token()

    def _set_creds(self, creds):
        self.client_id = creds.get("client_id", self.client_id)
        self.client_secret = creds.get("client_secret", self.client_secret)
        self.access_token = creds.get("access_token", self.access_token)
        self.refresh_token = creds.get("refresh_token", self.refresh_token)

    def request_refresh_token(self):
        creds = self._session.post(
            f"{AUTH_URL}/token",
            data={
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
            },
        )
        creds.raise_for_status()
        self._set_creds(creds.json())

    @backoff.on_exception(
        backoff.expo,
        (Timeout, ConnectionError, RequestException,
         WaitAndRetry, HTTPError, exceptions.Error),
        max_tries=4,
        factor=2,
    )
    def make_request(self, endpoint, **params):
        url = "{}/{}".format(BASE_API_URL, endpoint)

        headers = {
            "User-Agent": self.user_agent,
            "Authorization": f"Bearer {self.access_token}",
        }
        response = self._session.get(url, params=params, headers=headers)

        if response.status_code == 429:
            raise exceptions.APIThrottleError(
                "got rate limited, waiting a bit")
        elif response.status_code == 500:
            raise exceptions.APIError(
                f"got internal server error from pipedrive, waiting a bit  url: {url} response: {response.text}")
        elif response.status_code in [400, 401, 403]:
            self.request_refresh_token()
            raise exceptions.APITransientAuth(
                f"got possible bad auth, refreshing tokens and trying again url: {url} response: {response.text}")
        else:
            response.raise_for_status()
            rate_limit_time_remaining = response.headers.get(
                "X-RateLimit-Remaining")
            rate_limit_reset = response.headers.get("X-RateLimit-Reset", 0)
            if rate_limit_time_remaining and rate_limit_reset:
                if int(rate_limit_time_remaining) < 1:
                    sleep_period_s = int(rate_limit_reset)
                    error_message = (
                        f"got rate limited, waiting {sleep_period_s} seconds"
                    )

                    logger.warning(error_message)
                    time.sleep(sleep_period_s)
            try:
                return response.json()
            except JSONDecodeError:
                raise exceptions.APIBadResponse(
                    f"got bad json, trying again (status: {response.status_code}, response_text: {response.text}")

    def paginate_request(self, endpoint, **params):
        try:
            has_more_results = True
            start = 0
            response_json = None
            params["limit"] = PAGINATION_LIMIT
            while has_more_results:
                params["start"] = start
                response_json = self.make_request(endpoint, **params)

                for obj in response_json["data"] or []:
                    yield obj

                metadata = response_json["additional_data"]

                # since_timestamp_str = metadata["since_timestamp"]
                has_more_results = metadata["pagination"]["more_items_in_collection"]
                start = metadata["pagination"].get("next_start")
        except:
            logger.exception(
                f"Got error during recents pagination! response_json: {json.dumps(response_json)}"
            )
            raise

    def paginate_recents(self, since_timestamp_str):
        record = None
        ITEMS_PARAM = ",".join(
            [
                "activity",
                "deal",
                "note",
                "person",
                "organization",
                "pipeline",
                "product",
                "stage",
                "user",
            ]
        )
        SUBQUERIES = {
            "deal": [
                {
                    "stream_name": "deal_flow",
                    "endpoint_template": "deals/{}/flow",
                    "params": {"all_changes": 1},
                }
            ]
        }
        for obj in self.paginate_request(
            "recents", since_timestamp=since_timestamp_str, items=ITEMS_PARAM
        ):
            stream_name = obj["item"]
            data = obj["data"]

            if data is None:
                continue
            if not isinstance(data, list):
                data = [data]
            for record in data:
                since_timestamp_str = (
                    record.get("update_time")
                    or record.get("modified")
                    or record["created"]
                )
                yield since_timestamp_str, stream_name, record
                for subquery in SUBQUERIES.get(stream_name, []):
                    for sub_obj in self.paginate_request(
                        subquery["endpoint_template"].format(record["id"])
                    ):
                        yield since_timestamp_str, subquery["stream_name"], sub_obj[
                            "data"
                        ]
