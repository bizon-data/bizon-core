from typing import List

from loguru import logger

from bizon.destinations.destination import AbstractDestination

from .config import LoggerDestinationConfig


class LoggerDestination(AbstractDestination):
    def __init__(self, config: LoggerDestinationConfig) -> None:
        pass

    def check_connection(self) -> bool:
        return True

    def write_records(self, records: List[str]):
        for record in records:
            logger.info(record)
