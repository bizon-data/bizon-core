from typing import List, Tuple

import polars as pl
from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destination.destination import AbstractDestination
from bizon.engine.backend.backend import AbstractBackend

from .config import LoggerDestinationConfig


class LoggerDestination(AbstractDestination):

    def __init__(self, sync_metadata: SyncMetadata, config: LoggerDestinationConfig, backend: AbstractBackend):
        super().__init__(
            sync_metadata=sync_metadata,
            config=config,
            backend=backend,
        )

    def check_connection(self) -> bool:
        return True

    def delete_table(self) -> bool:
        return True

    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, str]:
        for record in df_destination_records.iter_rows(named=True):
            logger.info(record["source_data"])
        return True, ""
