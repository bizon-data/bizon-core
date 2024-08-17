from abc import ABC, abstractmethod
from typing import List

from bizon.source.models import SourceRecord

from .config import DestinationConfig, DestinationTypes


class AbstractDestination(ABC):

    def __init__(self, source_name: str, stream_name: str, config: DestinationConfig):
        self.source_name = source_name
        self.stream_name = stream_name
        self.config = config

    @property
    def name(self) -> str:
        return self.config.name

    @abstractmethod
    def check_connection(self) -> bool:
        pass

    @abstractmethod
    def write_records(self, source_records: List[SourceRecord]):
        pass


class DestinationFactory:
    @staticmethod
    def get_destination(source_name: str, stream_name: str, destination_config_dict: dict) -> AbstractDestination:

        if destination_config_dict.get("name") == DestinationTypes.LOGGER:
            from .logger.src.config import LoggerDestinationConfig
            from .logger.src.destination import LoggerDestination

            config = LoggerDestinationConfig.model_validate(obj=destination_config_dict.get("config"))
            return LoggerDestination(source_name=source_name, stream_name=stream_name, config=config)

        if destination_config_dict.get("name") == DestinationTypes.BIGQUERY:
            from .bigquery.src.config import BigQueryConfig
            from .bigquery.src.destination import BigQueryDestination

            config = BigQueryConfig.model_validate(obj=destination_config_dict.get("config"))
            return BigQueryDestination(source_name=source_name, stream_name=stream_name, config=config)

        raise ValueError(
            f"Destination {destination_config_dict.get('name')}" f"with params {destination_config_dict} not found"
        )
