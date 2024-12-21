from enum import Enum
from typing import Any, List, Tuple

from requests.auth import AuthBase

from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://pokeapi.co/api/v2"


# Define the streams that the source supports
class PokeAPIStreams(str, Enum):
    POKEMON = "pokemon"
    BERRY = "berry"
    ITEM = "item"


# Define the config class for the source
class PokeAPISourceConfig(SourceConfig):
    stream: PokeAPIStreams


class PeriscopeSource(AbstractSource):

    def __init__(self, config: PokeAPISourceConfig):
        super().__init__(config)
        self.config: PokeAPISourceConfig = config

    @property
    def url_entity(self) -> str:
        return f"{BASE_URL}/{self.config.stream}"

    @staticmethod
    def streams() -> List[str]:
        return [item.value for item in PokeAPIStreams]

    @staticmethod
    def get_config_class() -> AbstractSource:
        return PokeAPISourceConfig

    def check_connection(self) -> Tuple[bool | Any | None]:
        # Make a request to the base URL to check if the connection is successful
        _ = self.session.get(self.url_entity)
        return True, None

    def get_authenticator(self) -> AuthBase:
        # We return None because we don't need any authentication
        return None

    def get_total_records_count(self) -> int | None:
        # Return the total number of records in the stream
        response = self.session.get(self.url_entity)
        return response.json().get("count")

    def get_entity_list(self, pagination: dict = None) -> SourceIteration:
        # If pagination is provided, use the next URL to get the next set of records
        url = pagination.get("next") if pagination else self.url_entity
        response = self.session.get(url)

        data = response.json()

        return SourceIteration(
            next_pagination={"next": data.get("next")} if data.get("next") else {},
            records=[
                SourceRecord(
                    id=record["name"],
                    data=record,
                )
                for record in data["results"]
            ],
        )

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream in [PokeAPIStreams.POKEMON, PokeAPIStreams.BERRY, PokeAPIStreams.ITEM]:
            return self.get_entity_list(pagination)

        raise NotImplementedError(f"Stream {self.config.stream} not implemented for PokeAPI source")
