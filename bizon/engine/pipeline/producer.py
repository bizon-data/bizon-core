import ast
import multiprocessing
import multiprocessing.synchronize
import threading
import traceback
from datetime import datetime
from time import sleep
from typing import Tuple, Union

from loguru import logger
from pytz import UTC

from bizon.common.models import BizonConfig
from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.models import CursorStatus
from bizon.engine.queue.queue import AbstractQueue
from bizon.source.cursor import Cursor
from bizon.source.source import AbstractSource

from .models import PipelineReturnStatus


class Producer:
    def __init__(
        self, bizon_config: BizonConfig, queue: AbstractQueue, source: AbstractSource, backend: AbstractBackend
    ):
        self.bizon_config = bizon_config
        self.queue = queue
        self.source = source
        self.backend = backend

    @property
    def name(self) -> str:
        return f"producer-{self.source.config.name}-{self.source.config.stream}"

    def get_or_create_cursor(self, job_id: str, session=None) -> Cursor:
        """Get or create a cursor for the current stream, return the cursor"""
        # Try to get the cursor from the DB
        cursor_from_db = self.backend.get_last_cursor_by_job_id(job_id=job_id)

        cursor = None

        if cursor_from_db:
            # Retrieve the job
            job = self.backend.get_stream_job_by_id(job_id=job_id)

            logger.info(
                f"Recovered cursor from DB for iteration {cursor_from_db.from_source_iteration} -> {cursor_from_db.to_source_iteration}"
            )

            # Initialize the recovery from the DestinationCursor
            cursor = Cursor.from_db(
                source_name=self.source.config.name,
                stream_name=self.source.config.stream,
                job_id=job_id,
                total_records=job.total_records_to_fetch,
                iteration=cursor_from_db.to_source_iteration + 1,
                rows_fetched=self.backend.get_number_of_written_rows_for_job(job_id=job_id),
                pagination=ast.literal_eval(cursor_from_db.pagination),
            )
        else:
            # Get the total number of records
            total_records = self.source.get_total_records_count()
            # Initialize the cursor
            cursor = Cursor(
                source_name=self.source.config.name,
                stream_name=self.source.config.stream,
                job_id=job_id,
                total_records=total_records,
            )
        return cursor

    def handle_max_iterations(self, cursor: Cursor) -> bool:
        """Handle the case where the max_iterations is reached for the current cursor
        If max_iterations is reached return True
        Else return False
        """
        if self.source.config.max_iterations and cursor.iteration > self.source.config.max_iterations:
            logger.warning(
                f"Max iteration of {self.source.config.max_iterations} reached for this cursor, terminating ..."
            )
            return True
        return False

    def is_queue_full(self, cursor: Cursor) -> Tuple[bool, int, int]:
        """Handle the case where the queue is full
        If the queue is full return True, 0, 0
        Else return False, int[queue_size], int[approximate_nb_records_in_queue]
        """
        queue_size = self.queue.get_size()

        if not isinstance(queue_size, int):
            return False, 0, 0

        if not isinstance(cursor.avg_records_per_iteration, int):
            return False, 0, 0

        approximate_nb_records_in_queue = queue_size * cursor.avg_records_per_iteration

        if approximate_nb_records_in_queue >= self.queue.config.max_nb_messages:
            return True, queue_size, approximate_nb_records_in_queue

        return False, queue_size, approximate_nb_records_in_queue

    def run(
        self, job_id: int, stop_event: Union[multiprocessing.synchronize.Event, threading.Event]
    ) -> PipelineReturnStatus:

        return_value: PipelineReturnStatus = PipelineReturnStatus.SUCCESS

        # Init queue
        try:
            self.queue.connect()
        except Exception as e:
            logger.error(
                f"Error while connecting to the queue: {e} for job_id {job_id}"
                f"Check the queue error logs for more details."
            )
            return PipelineReturnStatus.QUEUE_ERROR

        # Get or create the cursor
        try:
            cursor = self.get_or_create_cursor(job_id=job_id, session=self.backend.session)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(
                f"Error while getting or creating cursor: {e} for job_id {job_id}"
                f"Check the backend error logs for more details."
            )
            logger.info("Terminating destination ...")
            self.queue.terminate(iteration=0)
            return PipelineReturnStatus.BACKEND_ERROR

        while not cursor.is_finished:

            if stop_event.is_set():
                logger.info("Stop event is set, terminating producer ...")
                return PipelineReturnStatus.KILLED_BY_RUNNER

            timestamp_start_iteration = datetime.now(tz=UTC)

            # Handle the case where last cursor already reach max_iterations
            terminate = self.handle_max_iterations(cursor)
            if terminate:
                break

            # Check if we need to create a new cursor in the DB
            if cursor.iteration % self.backend.config.syncCursorInDBEvery == 0:
                # Create a new cursor in the DB
                try:
                    self.backend.create_source_cursor(
                        job_id=job_id,
                        name=self.bizon_config.name,
                        source_name=self.source.config.name,
                        stream_name=self.source.config.stream,
                        iteration=cursor.iteration,
                        rows_fetched=cursor.rows_fetched,
                        next_pagination=cursor.pagination,
                        cursor_status=CursorStatus.PULLING,
                    )
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error(
                        f"Error while creating cursor in the DB: {e} for iteration {cursor.iteration}"
                        f"Check the backend error logs for more details."
                    )
                    return_value = PipelineReturnStatus.BACKEND_ERROR
                    break

            # Get current time, we consider this as the time the data was extracted
            extracted_at = datetime.now(tz=UTC)

            # Check if queue is full, we wait a random time and retry
            is_queue_full, queue_size, approximate_nb_records_in_queue = self.is_queue_full(cursor)
            if is_queue_full:
                QUEUE_FULL_WAITING_TIME = 2
                logger.warning(
                    f"Queue is full contains {queue_size} iterations with {approximate_nb_records_in_queue} records, waiting {QUEUE_FULL_WAITING_TIME} seconds before retrying..."
                )
                sleep(QUEUE_FULL_WAITING_TIME)
                continue

            # Get the next data
            try:
                source_iteration = self.source.get(pagination=cursor.pagination)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error(
                    f"Error while fetching data from source: {e} for iteration first iteration {cursor.iteration}"
                    f"Check the backend error logs for more details."
                )
                return_value = PipelineReturnStatus.SOURCE_ERROR
                break

            # Put the data in the queue
            try:
                self.queue.put(
                    source_iteration=source_iteration,
                    iteration=cursor.iteration,
                    extracted_at=extracted_at,
                )
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error(
                    f"Error while putting data in the queue: {e} for iteration {cursor.iteration}"
                    f"Check the queue error logs for more details."
                )
                return_value = PipelineReturnStatus.QUEUE_ERROR
                break

            # Update the cursor state
            try:
                cursor.update_state(
                    pagination_dict=source_iteration.next_pagination, nb_records_fetched=len(source_iteration.records)
                )
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error(
                    f"Error while updating cursor state: {e} for iteration {cursor.iteration}"
                    f"Check the source error logs for more details."
                )
                return_value = PipelineReturnStatus.SOURCE_ERROR
                break

            # Items in queue
            items_in_queue = f"{self.queue.get_size()} items in queue." if self.queue.get_size() else ""

            logger.info(
                (
                    f"Iteration {cursor.iteration} finished in {datetime.now(tz=UTC) - timestamp_start_iteration}. {items_in_queue}"
                )
            )

        logger.info("Terminating destination ...")

        try:
            self.queue.terminate(iteration=cursor.iteration)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(
                f"Error while terminating the queue: {e} for iteration {cursor.iteration}"
                f"Check the queue error logs for more details."
            )
            # Prevent masking any previous error
            return_value = (
                PipelineReturnStatus.QUEUE_ERROR if return_value == PipelineReturnStatus.SUCCESS else return_value
            )

        return return_value
