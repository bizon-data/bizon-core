from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Optional, Tuple, Union

import polars as pl
from loguru import logger
from pydantic import BaseModel, Field

from bizon.common.models import SyncMetadata
from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.models import JobStatus
from bizon.source.config import SourceSyncModes

from .buffer import DestinationBuffer
from .config import (
    AbstractDestinationConfig,
    AbstractDestinationDetailsConfig,
    DestinationTypes,
)
from .models import transform_to_df_destination_records


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
    def write_records(self, df_destination_records: pl.DataFrame) -> Tuple[bool, Union[str, None]]:
        """Write records to destination for the given iteration and return success status and error message"""
        pass

    def finalize(self) -> bool:
        """Finalize destination after writing records, usually used to copy data from temp table to main table"""
        pass

    def buffer_flush_handler(self, session=None) -> DestinationIteration:
        # TODO: Add retry strategy

        # Initialize destination iteration
        destination_iteration = DestinationIteration(
            success=False,
            records_written=0,
            pagination=self.buffer.pagination,
        )

        logger.info(
            f"Writing in destination from source iteration {self.buffer.from_iteration} to {self.buffer.to_iteration}"
        )

        success, error_msg = self.write_records(df_destination_records=self.buffer.df_destination_records)

        if success:
            # We wrote records to destination so we keep it
            destination_iteration.records_written = self.buffer.df_destination_records.height
            logger.info(f"Successfully wrote {destination_iteration.records_written} records to destination")

        else:
            # We failed to write records to destination so we keep the error message
            destination_iteration.error_message = error_msg

        destination_iteration.success = success
        destination_iteration.from_source_iteration = self.buffer.from_iteration
        destination_iteration.to_source_iteration = self.buffer.to_iteration
        destination_iteration.pagination = self.buffer.pagination

        # Update destination cursor
        self.create_cursors(destination_iteration=destination_iteration)

        return destination_iteration

    def write_or_buffer_records(
        self,
        df_destination_records: pl.DataFrame,
        iteration: int,
        last_iteration: bool = False,
        session=None,
        pagination: dict = None,
    ) -> DestinationBufferStatus:
        """Write records to destination or buffer them for the given iteration"""

        # Last iteration, write all records to destination
        if last_iteration:

            if self.buffer.df_destination_records.height == 0 and self.config.buffer_size == 0:
                logger.warning("No records to write to destination, already written, buffer is empty.")
                return DestinationBufferStatus.RECORDS_WRITTEN

            logger.debug("Writing last iteration records to destination")
            assert df_destination_records.height == 0, "Last iteration should not have any records"
            destination_iteration = self.buffer_flush_handler(session=session)

            if destination_iteration.success:
                # Update job status to success

                # Set job status to SUCCEEDED
                # /!\ If the sync mode is STREAM, the job status is kept to RUNNING
                job_status = (
                    JobStatus.SUCCEEDED if self.sync_metadata.sync_mode != SourceSyncModes.STREAM else JobStatus.RUNNING
                )

                self.backend.update_stream_job_status(
                    job_id=self.sync_metadata.job_id,
                    job_status=job_status,
                    session=session,
                )

            # Flush buffer
            self.buffer.flush()
            # Call the finalizing operations to wrap up sync
            self.finalize()
            return DestinationBufferStatus.RECORDS_WRITTEN

        # Don't write empty records to destination
        if df_destination_records.height == 0 and not last_iteration:
            logger.warning("No records to write to destination. Check source and queue provider.")
            return DestinationBufferStatus.NO_RECORDS

        # Write records to destination if buffer size is 0 or streaming
        if self.buffer.buffer_size == 0:
            logger.info("Writing records to destination.")
            self.buffer.add_source_iteration_records_to_buffer(
                iteration=iteration, df_destination_records=df_destination_records, pagination=pagination
            )
            self.buffer_flush_handler(session=session)
            self.buffer.flush()
            return DestinationBufferStatus.RECORDS_WRITTEN

        logger.info(f"Buffer free space {self.buffer.buffer_free_space_pct}%")
        logger.info(f"Buffer current size {round(self.buffer.current_size / 1024 / 1024, 2)}  Mb")
        logger.info(
            f"Buffer ripeness {round(self.buffer.ripeness / 60, 2)} min. Max ripeness {round(self.buffer.buffer_flush_timeout / 60, 2)} min."  # noqa
        )

        # Write buffer to destination if buffer is ripe and create a new buffer for the new iteration
        if self.buffer.is_ripe:
            logger.info(
                f"Buffer is ripe (buffering for longer than buffer_flush_timeout: {self.buffer.buffer_flush_timeout} seconds), writing buffer to destination"  # noqa
            )
            self.buffer_flush_handler(session=session)
            self.buffer.flush()
            self.buffer.add_source_iteration_records_to_buffer(
                iteration=iteration, df_destination_records=df_destination_records, pagination=pagination
            )
            return DestinationBufferStatus.RECORDS_WRITTEN_THEN_BUFFERED

        # Buffer can hold all records from this iteration
        elif self.buffer.buffer_free_space >= df_destination_records.estimated_size(unit="b"):
            self.buffer.add_source_iteration_records_to_buffer(
                iteration=iteration, df_destination_records=df_destination_records, pagination=pagination
            )
            return DestinationBufferStatus.RECORDS_BUFFERED

        # Buffer can contain some records from this iteration
        # For now we will write all records to destination and then buffer the remaining records
        else:
            self.buffer_flush_handler(session=session)
            self.buffer.flush()
            self.buffer.add_source_iteration_records_to_buffer(
                iteration=iteration, df_destination_records=df_destination_records, pagination=pagination
            )
            return DestinationBufferStatus.RECORDS_WRITTEN_THEN_BUFFERED

    def create_cursors(self, destination_iteration: DestinationIteration):
        self.backend.create_destination_cursor(
            job_id=self.sync_metadata.job_id,
            name=self.sync_metadata.name,
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
        df_source_records: pl.DataFrame,
        extracted_at: datetime,
        iteration: int,
        last_iteration: bool = False,
        pagination: dict = None,
    ) -> bool:
        """
        Write records to destination and update the cursor for the given iteration.
        Stores the source pagination for recovery purposes.
        """

        # Case when producer failed to fetch data from first iteration
        if iteration == 0 and df_source_records.height == 0:
            logger.warning("Source failed to fetch data from the first iteration, no records will be written.")
            return False

        # Convert to df_destinaton_records
        df_source_records = transform_to_df_destination_records(
            df_source_records=df_source_records, extracted_at=extracted_at
        )

        # Buffer records otherwise write to destination
        self.write_or_buffer_records(
            df_destination_records=df_source_records,
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
            from bizon.connectors.destinations.logger.src.destination import LoggerDestination

            return LoggerDestination(sync_metadata=sync_metadata, config=config.config, backend=backend)

        elif config.name == DestinationTypes.BIGQUERY:
            from bizon.connectors.destinations.bigquery.src.destination import BigQueryDestination

            return BigQueryDestination(sync_metadata=sync_metadata, config=config.config, backend=backend)

        elif config.name == DestinationTypes.BIGQUERY_STREAMING:
            from bizon.connectors.destinations.bigquery_streaming.src.destination import BigQueryStreamingDestination

            return BigQueryStreamingDestination(sync_metadata=sync_metadata, config=config.config, backend=backend)

        elif config.name == DestinationTypes.FILE:
            from bizon.connectors.destinations.file.src.destination import FileDestination

            return FileDestination(sync_metadata=sync_metadata, config=config.config, backend=backend)

        raise ValueError(f"Destination {config.name}" f"with params {config} not found")
