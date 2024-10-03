import sys
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import List, Optional, Tuple, Union

from loguru import logger
from pydantic import BaseModel, Field

from bizon.common.models import SyncMetadata
from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.models import JobStatus
from bizon.source.config import SourceSyncModes
from bizon.source.models import SourceRecord

from .buffer import DestinationBuffer
from .config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)
from .models import DestinationRecord


class DestinationBufferStatus(str, Enum):
    """Destination buffer status"""

    RECORDS_WRITTEN = "RECORDS_WRITTEN"
    RECORDS_WRITTEN_THEN_BUFFERED = "RECORDS_WRITTEN_THEN_BUFFERED"
    RECORDS_BUFFERED = "RECORDS_BUFFERED"
    NO_RECORDS = "NO_RECORDS"


class DestinationIteration(BaseModel):
    success: bool = Field(..., description="Success status of the iteration")
    error_message: Optional[str] = Field(None, description="Error message if iteration failed")
    records_written: int = Field(0, description="Number of records written to the destination")
    from_source_iteration: Optional[int] = Field(None, description="From source iteration identifier buffer starts")
    to_source_iteration: Optional[int] = Field(
        None, description="To source iteration identifier buffer ends, inclusive"
    )
    pagination: Optional[dict] = Field(None, description="Source pagination for next interation recovery purposes")


class AbstractDestination(ABC):

    def __init__(self, sync_metadata: SyncMetadata, config: AbstractDestinationDetailsConfig, backend: AbstractBackend):
        self.sync_metadata = sync_metadata
        self.config = config
        self.backend = backend
        self.buffer = DestinationBuffer(
            buffer_size=self.config.buffer_size, buffer_flush_timeout=self.config.buffer_flush_timeout
        )

    @abstractmethod
    def check_connection(self) -> bool:
        pass

    @abstractmethod
    def delete_table(self) -> bool:
        """Delete table in destination"""
        pass

    @abstractmethod
    def write_records(self, destination_records: List[DestinationRecord]) -> Tuple[bool, Union[str, None]]:
        """Write records to destination for the given iteration and return success status and error message"""
        pass

    @staticmethod
    def convert_source_records_to_destination_records(
        source_records: List[SourceRecord], extracted_at: datetime
    ) -> List[DestinationRecord]:
        """Convert source records to destination records"""
        return [
            DestinationRecord.from_source_record(source_record=source_record, extracted_at=extracted_at)
            for source_record in source_records
        ]

    def prepare_destination(self):
        """Prepare destination before writing records"""
        # Delete table if full refresh
        if self.sync_metadata.sync_mode == SourceSyncModes.FULL_REFRESH:
            self.delete_table()

    def buffer_flush_handler(self, session=None) -> DestinationIteration:
        nb_records_to_write = len(self.buffer.records)
        # Initialize destination iteration
        destination_iteration = DestinationIteration(
            success=False,
            records_written=0,
            pagination=self.buffer.pagination,
        )
        success, error_msg = self.write_records(destination_records=self.buffer.records)
        if success:
            destination_iteration.records_written = nb_records_to_write
            self.backend.update_stream_job_status(
                job_id=self.sync_metadata.job_id,
                job_status=JobStatus.SUCCEEDED,
                session=session,
            )

        else:
            self.backend.update_stream_job_status(
                job_id=self.sync_metadata.job_id,
                job_status=JobStatus.FAILED,
                error_message=error_msg,
                session=session,
            )

        destination_iteration.success = success
        destination_iteration.from_source_iteration = self.buffer.from_iteration
        destination_iteration.to_source_iteration = self.buffer.to_iteration
        destination_iteration.pagination = self.buffer.pagination

        # Update destination cursor
        self.create_cursors(destination_iteration=destination_iteration)

    def write_or_buffer_records(
        self,
        destination_records: List[DestinationRecord],
        iteration: int,
        last_iteration: bool = False,
        session=None,
        pagination: dict = None,
    ) -> DestinationBufferStatus:
        """Write records to destination or buffer them for the given iteration"""

        # Last iteration, write all records to destination
        if last_iteration:
            logger.debug("Writing last iteration records to destination")
            assert len(destination_records) == 0, "Last iteration should not have any records"
            self.buffer_flush_handler(session=session)
            self.buffer.flush()
            return DestinationBufferStatus.RECORDS_WRITTEN

        # Don't write empty records to destination
        if len(destination_records) == 0 and not last_iteration:
            logger.warning("No records to write to destination. Check source and queue provider.")
            return DestinationBufferStatus.NO_RECORDS

        # Write records to destination if buffer size is 0
        if self.buffer.buffer_size == 0:
            logger.debug("Writing last iteration records to destination")
            self.buffer.add_source_iteration_records_to_buffer(
                iteration=iteration, records=destination_records, pagination=pagination
            )
            self.buffer_flush_handler(session=session)
            self.buffer.flush()
            return DestinationBufferStatus.RECORDS_WRITTEN

        logger.info(f"Buffer free space {self.buffer.buffer_free_space_pct}%")
        logger.info(f"Buffer current size {self.buffer.current_size} bytes")
        logger.info(
            f"Buffer ripeness {self.buffer.ripeness / 60} min. Max ripeness {self.buffer.buffer_flush_timeout / 60} min."  # noqa
        )

        # Write buffer to destination if buffer is ripe and create a new buffer for the new iteration
        if self.buffer.is_ripe:
            logger.debug(
                f"Buffer is ripe (buffering for longer than buffer_flush_timeout: {self.buffer.buffer_flush_timeout} seconds), writing buffer to destination"  # noqa
            )
            self.buffer_flush_handler(session=session)
            self.buffer.flush()
            self.buffer.add_source_iteration_records_to_buffer(
                iteration=iteration, records=destination_records, pagination=pagination
            )
            return DestinationBufferStatus.RECORDS_WRITTEN_THEN_BUFFERED

        # Buffer can hold all records from this iteration
        elif self.buffer.buffer_free_space >= sys.getsizeof(destination_records):
            self.buffer.add_source_iteration_records_to_buffer(
                iteration=iteration, records=destination_records, pagination=pagination
            )
            return DestinationBufferStatus.RECORDS_BUFFERED

        # Buffer can contain some records from this iteration
        # For now we will write all records to destination and then buffer the remaining records
        else:
            self.buffer_flush_handler(session=session)
            self.buffer.flush()
            self.buffer.add_source_iteration_records_to_buffer(
                iteration=iteration, records=destination_records, pagination=pagination
            )
            return DestinationBufferStatus.RECORDS_WRITTEN_THEN_BUFFERED

    def create_cursors(self, destination_iteration: DestinationIteration):
        self.backend.create_destination_cursor(
            job_id=self.sync_metadata.job_id,
            source_name=self.sync_metadata.source_name,
            stream_name=self.sync_metadata.stream_name,
            destination_name=self.sync_metadata.destination_name,
            from_source_iteration=destination_iteration.from_source_iteration,
            to_source_iteration=destination_iteration.to_source_iteration,
            rows_written=destination_iteration.records_written,
            pagination=destination_iteration.pagination,
            success=destination_iteration.success,
        )

    def write_records_and_update_cursor(
        self,
        source_records: List[SourceRecord],
        iteration: int,
        extracted_at: datetime,
        last_iteration: bool = False,
        pagination: dict = None,
    ) -> bool:
        """
        Write records to destination and update the cursor for the given iteration.
        Stores the source pagination for recovery purposes.
        """

        # Case when producer failed to fetch data from first iteration
        if iteration == 0 and len(source_records) == 0:
            logger.warning("Source failed to fetch data from the first iteration, no records will be written.")
            return False

        # Prepare destination
        if iteration == 0:
            self.prepare_destination()

        destination_records = self.convert_source_records_to_destination_records(
            source_records=source_records,
            extracted_at=extracted_at,
        )

        # Buffer records otherwise write to destination
        self.write_or_buffer_records(
            destination_records=destination_records,
            iteration=iteration,
            last_iteration=last_iteration,
            pagination=pagination,
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

        elif config.name == DestinationTypes.BIGQUERY:
            from .bigquery.src.destination import BigQueryDestination

            return BigQueryDestination(sync_metadata=sync_metadata, config=config.config, backend=backend)

        elif config.name == DestinationTypes.FILE:
            from .file.src.destination import FileDestination

            return FileDestination(sync_metadata=sync_metadata, config=config.config, backend=backend)

        raise ValueError(f"Destination {config.name}" f"with params {config} not found")
