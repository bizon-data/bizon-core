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
from bizon.monitoring.monitor import AbstractMonitor
from bizon.source.callback import AbstractSourceCallback
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
    def __init__(
        self,
        sync_metadata: SyncMetadata,
        config: AbstractDestinationDetailsConfig,
        backend: AbstractBackend,
        source_callback: AbstractSourceCallback,
        monitor: AbstractMonitor,
    ):
        self.sync_metadata = sync_metadata
        self.config = config
        self.backend = backend
        self.monitor = monitor
        self.buffer = DestinationBuffer(
            buffer_size=self.config.buffer_size, buffer_flush_timeout=self.config.buffer_flush_timeout
        )
        self.source_callback = source_callback
        self.destination_id = config.destination_id

        self._record_schemas = None
        self._clustering_keys = None

    @property
    def record_schemas(self):
        if self._record_schemas is None and self.config.record_schemas:
            self._record_schemas = {
                schema.destination_id: schema.record_schema for schema in self.config.record_schemas
            }
        return self._record_schemas

    @property
    def clustering_keys(self):
        if self._clustering_keys is None and self.config.record_schemas:
            self._clustering_keys = {
                schema.destination_id: schema.clustering_keys for schema in self.config.record_schemas
            }
        return self._clustering_keys

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
            f"Writing in destination {self.destination_id} from source iteration {self.buffer.from_iteration} to {self.buffer.to_iteration}"
        )

        success, error_msg = self.write_records(df_destination_records=self.buffer.df_destination_records)

        if success:
            # We wrote records to destination so we keep it
            destination_iteration.records_written = self.buffer.df_destination_records.height
            logger.info(
                f"Successfully wrote {destination_iteration.records_written} records to destination {self.destination_id}"
            )

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
        publish: bool = True,
    ) -> DestinationBufferStatus:
        """Write records to destination or buffer them for the given iteration.

        `publish=False` means the producer stopped on an error. Everything already staged is
        still flushed — the staging table and the destination cursors are the resume state —
        but the sync is neither published nor marked successful.
        """

        # Last iteration, write all records to destination
        if last_iteration:
            if self.buffer.df_destination_records.height == 0 and self.buffer.is_empty:
                logger.info("No records to write to destination, already written, buffer is empty.")
                return DestinationBufferStatus.RECORDS_WRITTEN

            logger.debug("Writing last iteration records to destination")
            assert df_destination_records.height == 0, "Last iteration should not have any records"
            destination_iteration = self.buffer_flush_handler(session=session)

            if destination_iteration.success and publish:
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

            if not publish:
                # Publishing here is what used to turn a failed run into a silently truncated
                # destination table: finalize() copies the staging table over the destination
                # with WRITE_TRUNCATE regardless of how the producer exited.
                #
                # The job is left RUNNING, which means different things per sync mode.
                # Incremental resumes it: the producer restarts from the destination cursors this
                # run wrote and the staged rows are published by the next successful finalize().
                # A full refresh does NOT resume -- get_or_create_job() cancels a RUNNING
                # full-refresh job and creates a new one, whose job_id has no cursor, so
                # _ensure_clean_temp_table() drops the staging table and the run starts over.
                # Do not rewrite this as "the staging table is the resume state" for both.
                logger.warning(
                    f"Producer failed: skipping publication for {self.destination_id}. "
                    "The destination table is left untouched."
                )
                return DestinationBufferStatus.RECORDS_WRITTEN

            # Call the finalizing operations to wrap up sync
            self.finalize()
            return DestinationBufferStatus.RECORDS_WRITTEN

        # Don't write empty records to destination
        if df_destination_records.height == 0 and not last_iteration:
            logger.info("No records to write to destination. Check source and queue provider.")
            return DestinationBufferStatus.NO_RECORDS

        # Write records to destination if buffer size is 0 or streaming
        if self.buffer.buffer_size == 0:
            logger.info(f"Writing records to destination {self.destination_id}.")
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
        logger.info(
            f"Current records size to process: {round(df_destination_records.estimated_size(unit='b') / 1024 / 1024, 2)} Mb."
        )

        if df_destination_records.estimated_size(unit="b") > self.buffer.buffer_size:
            raise ValueError(
                f"Records size {round(df_destination_records.estimated_size(unit='b') / 1024 / 1024, 2)} Mb is greater than buffer size {round(self.buffer.buffer_size / 1024 / 1024, 2)} Mb. Please increase destination buffer_size or reduce batch_size from the source."
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
        publish: bool = True,
    ) -> bool:
        """
        Write records to destination and update the cursor for the given iteration.
        Stores the source pagination for recovery purposes.
        """

        # Nothing to write on the first iteration: either the producer failed before yielding
        # anything, or the stream is genuinely empty. `publish` is what tells the two apart --
        # the message used to assert failure in both cases, which sent operators hunting for a
        # source bug that was not there.
        if iteration == 0 and df_source_records.height == 0:
            if publish:
                logger.warning("Source returned no records on the first iteration, nothing will be written.")
            else:
                logger.warning("Source failed on the first iteration, no records will be written.")
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
            publish=publish,
        )

        return True


class DestinationFactory:
    @staticmethod
    def get_destination(
        sync_metadata: SyncMetadata,
        config: AbstractDestinationConfig,
        backend: AbstractBackend,
        source_callback: AbstractSourceCallback,
        monitor: AbstractMonitor,
    ) -> AbstractDestination:
        if config.name == DestinationTypes.LOGGER:
            from bizon.connectors.destinations.logger.src.destination import (
                LoggerDestination,
            )

            return LoggerDestination(
                sync_metadata=sync_metadata,
                config=config.config,
                backend=backend,
                source_callback=source_callback,
                monitor=monitor,
            )

        elif config.name == DestinationTypes.BIGQUERY:
            from bizon.connectors.destinations.bigquery.src.destination import (
                BigQueryDestination,
            )

            return BigQueryDestination(
                sync_metadata=sync_metadata,
                config=config.config,
                backend=backend,
                source_callback=source_callback,
                monitor=monitor,
            )

        elif config.name == DestinationTypes.BIGQUERY_STREAMING:
            from bizon.connectors.destinations.bigquery_streaming.src.destination import (
                BigQueryStreamingDestination,
            )

            return BigQueryStreamingDestination(
                sync_metadata=sync_metadata,
                config=config.config,
                backend=backend,
                source_callback=source_callback,
                monitor=monitor,
            )

        elif config.name == DestinationTypes.BIGQUERY_STREAMING_V2:
            from bizon.connectors.destinations.bigquery_streaming_v2.src.destination import (
                BigQueryStreamingV2Destination,
            )

            return BigQueryStreamingV2Destination(
                sync_metadata=sync_metadata,
                config=config.config,
                backend=backend,
                source_callback=source_callback,
                monitor=monitor,
            )

        elif config.name == DestinationTypes.FILE:
            from bizon.connectors.destinations.file.src.destination import (
                FileDestination,
            )

            return FileDestination(
                sync_metadata=sync_metadata,
                config=config.config,
                backend=backend,
                source_callback=source_callback,
                monitor=monitor,
            )

        raise ValueError(f"Destination {config.name}with params {config} not found")
