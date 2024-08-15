from typing import List, Tuple

from bizon.api.source import APISource

from .auth import DummyAuth
from .config import DummySourceConfig
from .fake_api import fake_api_call


class DummySource(APISource):

    def __init__(self, config: DummySourceConfig):
        super().__init__(config)

    @property
    def url_entity(self) -> str:
        return f"https://api.dummy.com/v1/{self.config.stream_name}"

    def get_auth(self) -> DummyAuth:
        return DummyAuth(config=self.config.authentication)

    def check_connection(self) -> Tuple[bool, str | None]:
        # Here we could check if the connection is established
        return True, None

    def get_total_records_count(self) -> int | None:
        # If available, return total number of records in the source for this stream
        # Otherwise, return None
        # In our case we have 5 records
        return 5

    def get(self, pagination: dict = None) -> Tuple[dict, List[dict]]:

        response: dict = None

        # If no pagination data is passed, we want to reach first page
        if not pagination:
            response = fake_api_call(url=self.url_entity)

        # If we have pagination data we pass it to the API
        else:
            response = fake_api_call(url=self.url_entity, cursor=pagination.get("cursor"))

        # Now we process the response to:
        # - allow bizon to process the records and write them to destination
        # - iterate on next page if needed

        # We parse records and next cursor from the response
        records = response.get("results")
        next_cursor = response.get("next", {}).get("cursor")

        next_pagination = {"cursor": next_cursor} if next_cursor else {}

        if records:
            return next_pagination, records

        return next_pagination, []
