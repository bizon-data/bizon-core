import ast
from datetime import datetime

from loguru import logger
from pytz import UTC

from bizon.engine.backend.models import CursorStatus
from bizon.source.cursor import Cursor
from bizon.source.source import Source

from .backend.backend import AbstractBackend
from .queue.queue import AbstractQueue


class Producer:
    def __init__(self, queue: AbstractQueue, source: Source, backend: AbstractBackend):
        self.queue = queue
        self.source = source
        self.backend = backend

    @property
    def name(self) -> str:
        return f"producer-{self.source.config.source_name}-{self.source.config.stream_name}"

    def get_or_create_cursor(self, job_id: str, session=None) -> Cursor:
        """Get or create a cursor for the current stream, return the cursor"""
        # Try to get the cursor from the DB
        cursor_from_bd = self.backend.get_last_source_cursor_by_job_id(job_id=job_id)

        cursor = None

        if cursor_from_bd:
            # Retrieve the job
            job = self.backend.get_stream_job_by_id(job_id=job_id, session=session)

            logger.info(f"Recovering cursor from DB: {cursor_from_bd}")

            # Initialize the cursor from the DB
            cursor = Cursor.from_db(
                source_name=self.source.config.source_name,
                stream_name=self.source.config.stream_name,
                job_id=job_id,
                total_records=job.total_records_to_fetch,
                iteration=cursor_from_bd.iteration,
                rows_fetched=cursor_from_bd.rows_fetched,
                pagination=ast.literal_eval(cursor_from_bd.next_pagination),
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

        # Init queue
        self.queue.connect()

        # Get or create the cursor
        cursor = self.get_or_create_cursor(job_id=job_id)

        while not cursor.is_finished:

            # Handle the case where last cursor already reach max_iterations
            terminate = self.handle_max_iterations(cursor)
            if terminate:
                break

            # Check if we need to create a new cursor in the DB
            if cursor.iteration % self.backend.config.syncCursorInDBEvery == 0:
                # Create a new cursor in the DB
                self.backend.create_source_cursor(
                    job_id=job_id,
                    source_name=self.source.config.source_name,
                    stream_name=self.source.config.stream_name,
                    iteration=cursor.iteration,
                    rows_fetched=cursor.rows_fetched,
                    next_pagination=cursor.pagination,
                    cursor_status=CursorStatus.PULLING,
                )

            # Get the next data
            source_iteration = self.source.get(pagination=cursor.pagination)

            # Get current time, we consider this as the time the data was extracted
            extracted_at = datetime.now(tz=UTC)

            # Put the data in the queue
            self.queue.put(
                source_records=source_iteration.records, iteration=cursor.iteration, extracted_at=extracted_at
            )

            # Update the cursor state
            cursor.update_state(
                pagination_dict=source_iteration.next_pagination, nb_records_fetched=len(source_iteration.records)
            )

        logger.info("Terminating destination ...")
        self.queue.terminate(iteration=cursor.iteration)
