from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Tuple, Union

from loguru import logger

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
from .models import DestinationIteration, DestinationRecord


class AbstractDestination(ABC):

    def __init__(self, sync_metadata: SyncMetadata, config: AbstractDestinationDetailsConfig, backend: AbstractBackend):
        self.sync_metadata = sync_metadata
        self.config = config
        self.backend = backend
        self.buffer = DestinationBuffer(buffer_size=self.config.buffer_size)

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

    def write_or_buffer_records(
        self,
        destination_records: List[DestinationRecord],
        iteration: int,
        last_iteration: bool = False,
        session=None,
    ) -> DestinationIteration:
        """Write records to destination or buffer them for the given iteration"""

        # Last iteration, write all records to destination
        if last_iteration:
            logger.debug(f"Writing last iteration records to destination")
            assert len(destination_records) == 0, "Last iteration should not have any records"
            nb_records_to_write = len(self.buffer.records)
            success, error_msg = self.write_records(destination_records=self.buffer.records)

            if success:
                self.backend.update_stream_job_status(
                    job_id=self.sync_metadata.job_id,
                    job_status=JobStatus.SUCCEEDED,
                    session=session,
                )

            destination_iteration = DestinationIteration(
                success=success,
                error_message=error_msg,
                records_written=nb_records_to_write if success else 0,
                from_source_iteration=self.buffer.from_iteration,
                to_source_iteration=self.buffer.to_iteration,
            )

            self.buffer.flush()
            return destination_iteration

        # Buffer can hold all records from this iteration
        elif self.buffer.buffer_free_space >= len(destination_records):
            self.buffer.add_source_iteration_records_to_buffer(iteration=iteration, records=destination_records)
            return DestinationIteration(
                success=True,
                error_message=None,
                records_written=0,
            )

        # Buffer can contain some records from this iteration
        # For now we will write all records to destination and then buffer the remaining records
        elif self.buffer.buffer_free_space > 0:
            nb_records_to_write = len(self.buffer.records)
            success, error_msg = self.write_records(destination_records=self.buffer.records)

            destination_iteration = DestinationIteration(
                success=success,
                error_message=error_msg,
                records_written=nb_records_to_write if success else 0,
                from_source_iteration=self.buffer.from_iteration,
                to_source_iteration=self.buffer.to_iteration,
            )

            self.buffer.flush()
            assert len(destination_records) < self.buffer.buffer_size, "Buffer size cannot be smaller than records size"
            self.buffer.add_source_iteration_records_to_buffer(iteration=iteration, records=destination_records)
            return destination_iteration

        # Buffer cannot contain any records from this iteration
        else:
            nb_records_to_write = len(self.buffer.records)
            success, error_msg = self.write_records(destination_records=self.buffer.records)

            destination_iteration = DestinationIteration(
                success=success,
                error_message=error_msg,
                records_written=nb_records_to_write if success else 0,
                from_source_iteration=self.buffer.from_iteration,
                to_source_iteration=self.buffer.to_iteration,
            )

            self.buffer.flush()
            assert len(destination_records) < self.buffer.buffer_size, "Buffer size cannot be smaller than records size"

            self.buffer.add_source_iteration_records_to_buffer(iteration=iteration, records=destination_records)
            return destination_iteration

    def create_cursors(self, iteration: int, destination_iteration: DestinationIteration):
        self.backend.create_destination_cursor(
            job_id=self.sync_metadata.job_id,
            source_name=self.sync_metadata.source_name,
            stream_name=self.sync_metadata.stream_name,
            destination_name=self.sync_metadata.destination_name,
            from_source_iteration=iteration,
            to_source_iteration=iteration,
            rows_written=destination_iteration.records_written,
        )

    def write_records_and_update_cursor(
        self, source_records: List[SourceRecord], iteration: int, extracted_at: datetime, last_iteration: bool = False
    ) -> bool:
        """Write records to destination and update the cursor for the given iteration"""

        # Prepare destination
        if iteration == 0:
            self.prepare_destination()

        destination_records = self.convert_source_records_to_destination_records(
            source_records=source_records, extracted_at=extracted_at
        )

        # Buffer records otherwise write to destination
        destination_iteration = self.write_or_buffer_records(
            destination_records=destination_records,
            iteration=iteration,
            last_iteration=last_iteration,
        )

        # Update cursor in DB
        if iteration % self.backend.config.syncCursorInDBEvery == 0:
            # Update cursors
            self.create_cursors(iteration=iteration, destination_iteration=destination_iteration)

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
