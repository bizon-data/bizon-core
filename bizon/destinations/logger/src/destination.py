from typing import List

from loguru import logger

from bizon.destinations.config import DestinationConfig
from bizon.destinations.destination import AbstractDestination

from .config import LoggerDestinationConfig


class LoggerDestination(AbstractDestination):

    def __init__(self, source_name: str, stream_name: str, config: DestinationConfig):
        super().__init__(source_name, stream_name, config)

    def check_connection(self) -> bool:
        return True

    def write_records(self, records: List[str]):
        for record in records:
            logger.info(record)
