from typing import List

from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.source.models import SourceRecord

from .config import LoggerDestinationConfig


class LoggerDestination(AbstractDestination):

    def __init__(self, source_name: str, stream_name: str, config: LoggerDestinationConfig):
        super().__init__(source_name, stream_name, config)

    def check_connection(self) -> bool:
        return True

    def write_records(self, source_records: List[SourceRecord]):
        for record in source_records:
            logger.info(record.data)
