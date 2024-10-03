import ast
from datetime import datetime

from loguru import logger
from pytz import UTC

from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.models import CursorStatus, DestinationCursor
from bizon.engine.queue.queue import AbstractQueue
from bizon.source.cursor import Cursor
from bizon.source.source import AbstractSource

from .models import PipelineReturnStatus


class Producer:
    def __init__(self, queue: AbstractQueue, source: AbstractSource, backend: AbstractBackend):
        self.queue = queue
        self.source = source
        self.backend = backend

    @property
    def name(self) -> str:
        return f"producer-{self.source.config.source_name}-{self.source.config.stream_name}"

    def get_or_create_cursor(self, job_id: str, session=None) -> Cursor:
        """Get or create a cursor for the current stream, return the cursor"""
        # Try to get the cursor from the DB
        cursor_from_db = self.backend.get_last_cursor_by_job_id(job_id=job_id)

        cursor = None

        if cursor_from_db:
            # Retrieve the job
            job = self.backend.get_stream_job_by_id(job_id=job_id)

            logger.info(f"Recovering cursor from DB: {cursor_from_db}")

            # Initialize the recovery from the DestinationCursor
            cursor = Cursor.from_db(
                source_name=self.source.config.source_name,
                stream_name=self.source.config.stream_name,
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
                source_name=self.source.config.source_name,
                stream_name=self.source.config.stream_name,
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

    def run(self, job_id: int):

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
            logger.error(
                f"Error while getting or creating cursor: {e} for job_id {job_id}"
                f"Check the backend error logs for more details."
            )
            logger.info("Terminating destination ...")
            self.queue.terminate(iteration=0)
            return PipelineReturnStatus.BACKEND_ERROR

        while not cursor.is_finished:

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
                        source_name=self.source.config.source_name,
                        stream_name=self.source.config.stream_name,
                        iteration=cursor.iteration,
                        rows_fetched=cursor.rows_fetched,
                        next_pagination=cursor.pagination,
                        cursor_status=CursorStatus.PULLING,
                    )
                except Exception as e:
                    logger.error(
                        f"Error while creating cursor in the DB: {e} for iteration {cursor.iteration}"
                        f"Check the backend error logs for more details."
                    )
                    return_value = PipelineReturnStatus.BACKEND_ERROR
                    break

            # Get the next data
            try:
                source_iteration = self.source.get(pagination=cursor.pagination)
            except Exception as e:
                logger.error(
                    f"Error while fetching data from source: {e} for iteration first iteration {cursor.iteration}"
                    f"Check the backend error logs for more details."
                )
                return_value = PipelineReturnStatus.SOURCE_ERROR
                break

            # Get current time, we consider this as the time the data was extracted
            extracted_at = datetime.now(tz=UTC)

            # Put the data in the queue
            try:
                self.queue.put(
                    source_records=source_iteration.records,
                    iteration=cursor.iteration,
                    extracted_at=extracted_at,
                    pagination=source_iteration.next_pagination,
                )
            except Exception as e:
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
                logger.error(
                    f"Error while updating cursor state: {e} for iteration {cursor.iteration}"
                    f"Check the source error logs for more details."
                )
                return_value = PipelineReturnStatus.SOURCE_ERROR
                break

        logger.info("Terminating destination ...")

        try:
            self.queue.terminate(iteration=cursor.iteration)
        except Exception as e:
            logger.error(
                f"Error while terminating the queue: {e} for iteration {cursor.iteration}"
                f"Check the queue error logs for more details."
            )
            # Prevent masking any previous error
            return_value = (
                PipelineReturnStatus.QUEUE_ERROR if return_value == PipelineReturnStatus.SUCCESS else return_value
            )

        return return_value
