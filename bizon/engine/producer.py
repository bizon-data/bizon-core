import ast

from loguru import logger

from bizon.engine.backend.models import CursorStatus, JobStatus
from bizon.source.cursor import Cursor
from bizon.source.source import Source

from .backend.backend import AbstractBackend
from .backend.models import StreamJob
from .queue.queue import AbstractQueue


class Producer:
    def __init__(self, queue: AbstractQueue, source: Source, backend: AbstractBackend):
        self.queue = queue
        self.source = source
        self.backend = backend

    @property
    def name(self) -> str:
        return f"producer-{self.source.config.source_name}-{self.source.config.stream_name}"

    def get_or_create_job(self, force_create: bool = False, session=None) -> StreamJob:
        """Get or create a job for the current stream, return its ID"""
        # Retrieve the last job for this stream
        job = self.backend.get_running_stream_job(
            source_name=self.source.config.source_name,
            stream_name=self.source.config.stream_name,
            session=session,
        )

        if job:
            # If a job is already running, we cancel it and create a new one
            if force_create:
                logger.info(f"Found an existing job, cancelling it...")
                self.backend.update_stream_job_status(job_id=job.id, job_status=JobStatus.CANCELED)
                logger.info(f"Job {job.id} canceled. Creating a new one...")
            else:
                logger.debug(f"Found an existing job: {job.id}")
                return job

        # If no job is running, we create a new one:
        # Get the total number of records
        total_records = self.source.get_total_records_count()

        # Create a new job
        job = self.backend.create_stream_job(
            source_name=self.source.config.source_name,
            stream_name=self.source.config.stream_name,
            total_records_to_fetch=total_records,
        )

        return job

    def get_or_create_cursor(self, job_id: str, session=None) -> Cursor:
        """Get or create a cursor for the current stream, return the cursor"""
        # Try to get the cursor from the DB
        cursor_from_bd = self.backend.get_last_cursor_by_job_id(job_id=job_id)

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

    def run(self):

        # Init backend to DB
        self.backend.check_prerequisites()
        self.backend.create_all_tables()

        # Init queue
        self.queue.connect()

        # First we check if the connection is successful and initialize the cursor
        check_connection, connection_error = self.source.check_connection()

        if not check_connection:
            logger.error(f"Error while connecting to source: {connection_error}")
            return

        # Get or create the job, if force_ignore_checkpoint, we cancel the existing job and create a new one
        job = self.get_or_create_job(force_create=self.source.config.force_ignore_checkpoint)

        # Get or create the cursor
        cursor = self.get_or_create_cursor(job_id=job.id)

        # Handle the case where last cursor already reach max_iterations
        if self.source.config.max_iterations and cursor.iteration > self.source.config.max_iterations:
            logger.warning(
                f"Max iteration of {self.source.config.max_iterations} reached for this cursor, terminating ..."
            )
            self.queue.terminate(iteration=cursor.iteration)
            return

        # Set job status to running
        self.backend.update_stream_job_status(job_id=job.id, job_status=JobStatus.RUNNING)

        while not cursor.is_finished:

            if self.source.config.max_iterations and cursor.iteration > self.source.config.max_iterations:
                logger.warning(
                    f"Max iteration of {self.source.config.max_iterations} reached for this cursor, terminating ..."
                )
                self.queue.terminate(iteration=cursor.iteration)
                return

            # Check if we need to create a new cursor in the DB
            if cursor.iteration % self.backend.config.syncCursorInDBEvery == 0:
                # Create a new cursor in the DB
                self.backend.create_stream_cursor(
                    job_id=job.id,
                    source_name=self.source.config.source_name,
                    stream_name=self.source.config.stream_name,
                    iteration=cursor.iteration,
                    rows_fetched=cursor.rows_fetched,
                    next_pagination=cursor.pagination,
                    cursor_status=CursorStatus.PULLING,
                )

            # Get the next data
            source_iteration = self.source.get(pagination=cursor.pagination)

            # Put the data in the queue
            self.queue.put(source_records=source_iteration.records, iteration=cursor.iteration)

            # Update the cursor state
            cursor.update_state(
                pagination_dict=source_iteration.next_pagination, nb_records_fetched=len(source_iteration.records)
            )

        logger.info("Terminating destination ...")
        self.queue.terminate(iteration=cursor.iteration)
