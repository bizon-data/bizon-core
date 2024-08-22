from typing import List, Tuple

from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destinations.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend
from bizon.source.models import SourceRecord

from .config import LoggerDestinationConfig


class LoggerDestination(AbstractDestination):

    def __init__(self, sync_metadata: SyncMetadata, config: LoggerDestinationConfig, backend: AbstractBackend):
        super().__init__(sync_metadata, config, backend)

    def check_connection(self) -> bool:
        return True

    def delete_table(self) -> bool:
        return True

    def write_records(self, source_records: List[SourceRecord]) -> Tuple[bool, str]:
        for record in source_records:
            logger.info(record.data)
        return True, ""
