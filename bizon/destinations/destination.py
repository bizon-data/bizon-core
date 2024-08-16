from abc import ABC, abstractmethod
from typing import List

from .config import DestinationConfig, DestinationTypes


class AbstractDestination(ABC):

    def __init__(self, config: DestinationConfig):
        self.config = config

    @abstractmethod
    def check_connection(self) -> bool:
        pass

    @abstractmethod
    def write_records(self, records: List[str]):
        pass


class DestinationFactory:
    @staticmethod
    def get_destination(config: DestinationConfig) -> AbstractDestination:

        if config.name == DestinationTypes.LOGGER:
            from .logger.src.destination import LoggerDestination

            return LoggerDestination(config=config)

        if config.name == DestinationTypes.BIGQUERY:
            from .bigquery.src.client import BigQueryDestination

            return BigQueryDestination(config=config)

        raise ValueError(f"Destination {config.name} not found")
