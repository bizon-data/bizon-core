from abc import ABC, abstractmethod
from typing import List, Tuple, Union

from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.models import CursorStatus
from bizon.source.models import SourceRecord

from .config import AbstractDestinationConfig, DestinationTypes


class AbstractDestination(ABC):

    def __init__(self, sync_metadata: SyncMetadata, config: AbstractDestinationConfig, backend: AbstractBackend):
        self.sync_metadata = sync_metadata
        self.config = config
        self.backend = backend
        self.buffer = []

    @property
    def name(self) -> str:
        return self.config.name

    @abstractmethod
    def check_connection(self) -> bool:
        pass

    @abstractmethod
    def write_records(self, source_records: List[SourceRecord]) -> Tuple[bool, Union[str, None]]:
        """Write records to destination for the given iteration and return success status and error message"""
        pass

    def write_records_and_update_cursor(
        self, source_records: List[SourceRecord], iteration: int, last_iteration: bool = False
    ) -> bool:
        """Write records to destination and update the cursor for the given iteration"""

        # TODO: Implement batch writing
        # Write records if len(buffer) >= batch_size
        if len(self.buffer) < 2000 and not last_iteration:
            self.buffer.extend(source_records)
            logger.info(f"Buffered {len(source_records)} records")
            success = True
        else:
            success, error_msg = self.write_records(self.buffer)
            self.buffer = []

        if iteration % self.backend.config.syncCursorInDBEvery == 0:

            cursor = self.backend.get_last_cursor_by_stream_and_iteration(
                source_name=self.sync_metadata.source_name,
                stream_name=self.sync_metadata.stream_name,
                iteration=iteration,
            )

            if not cursor:
                raise ValueError(
                    f"Cursor not found for stream {self.sync_metadata.stream_name} and iteration {iteration}"
                )

            if success:
                # If successful, update cursor status
                self.backend.update_stream_cursor_status(
                    cursor_id=cursor.id,
                    cursor_status=CursorStatus.WRITTEN_IN_DESTINATION,
                )
                return

            # If failed, update cursor status
            self.backend.update_stream_cursor_status(
                cursor_id=cursor.id,
                cursor_status=CursorStatus.FAILED,
                error_message=error_msg,
            )

        return True


class DestinationFactory:
    @staticmethod
    def get_destination(
        sync_metadata: SyncMetadata,
        config: AbstractDestinationConfig,
        backend: AbstractBackend,
    ) -> AbstractDestination:

        if config.name == DestinationTypes.LOGGER:
            from .logger.src.destination import LoggerDestination

            return LoggerDestination(sync_metadata=sync_metadata, config=config.config, backend=backend)

        if config.name == DestinationTypes.BIGQUERY:
            from .bigquery.src.destination import BigQueryDestination

            return BigQueryDestination(sync_metadata=sync_metadata, config=config.config, backend=backend)

        raise ValueError(f"Destination {config.name}" f"with params {config} not found")
