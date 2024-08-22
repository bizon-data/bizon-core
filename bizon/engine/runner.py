import ast
from abc import ABC, abstractmethod

from loguru import logger

from bizon.cli.parser import parse_from_yaml
from bizon.common.models import BizonConfig, SyncMetadata
from bizon.destinations.destination import AbstractDestination, DestinationFactory
from bizon.source.cursor import Cursor
from bizon.source.source import Source
from bizon.sources import SOURCES

from .backend.backend import AbstractBackend, BackendFactory
from .backend.models import JobStatus, StreamJob
from .config import RunnerTypes
from .producer import Producer
from .queue.queue import AbstractQueue, QueueFactory


class AbstractRunner(ABC):
    def __init__(self, config: dict):
        self.config: dict = config
        self.bizon_config = BizonConfig.model_validate(obj=config)
        self._is_running: bool = False

        # Instaniate the source, backend and queue
        self.source: Source = self.get_source()
        self.backend: AbstractBackend = self.get_backend()
        self.queue: AbstractQueue = self.get_queue()

        # Init Job
        self.job = self.init_job()
        self.sync_metadata = SyncMetadata.from_bizon_config(job_id=self.job.id, config=self.bizon_config)

        # Init destination for a given jobId
        self.destination: AbstractDestination = self.get_destination()

    @property
    def is_running(self) -> bool:
        """Check if the pipeline is running"""
        return self._is_running

    @classmethod
    def from_yaml(cls, filepath: str):
        """Create a Runner instance from a yaml file"""
        config = parse_from_yaml(filepath)
        return cls(config=config)

    def get_source(self) -> Source:
        """Get an instance of the source based on the source config dict"""

        logger.info(
            f"Creating client for {self.bizon_config.source.source_name} - {self.bizon_config.source.stream_name} ..."
        )
        # Get the client class, validate the config and return the client
        return SOURCES.get_instance(
            source_name=self.bizon_config.source.source_name,
            stream_name=self.bizon_config.source.stream_name,
            source_config_dict=self.config["source"],
        )

    def get_destination(self) -> AbstractDestination:
        """Get an instance of the destination based on the destination config dict"""
        return DestinationFactory.get_destination(
            sync_metadata=self.sync_metadata,
            config=self.bizon_config.destination,
            backend=self.backend,
        )

    def get_backend(self) -> AbstractBackend:
        """Get an instance of the backend based on the backend config dict"""
        return BackendFactory.get_backend(
            config=self.bizon_config.engine.backend,
        )

    def get_producer(self) -> Producer:
        return Producer(
            queue=self.queue,
            source=self.source,
            backend=self.backend,
        )

    def get_queue(self):
        return QueueFactory.get_queue(
            config=self.bizon_config.engine.queue,
        )

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

    def init_job(self) -> StreamJob:
        """Initialize a job for the current stream"""
        self.backend.check_prerequisites()
        self.backend.create_all_tables()

        # First we check if the connection is successful and initialize the cursor
        check_connection, connection_error = self.source.check_connection()

        if not check_connection:
            logger.error(f"Error while connecting to source: {connection_error}")
            return

        # Get or create the job, if force_ignore_checkpoint, we cancel the existing job and create a new one
        job = self.get_or_create_job(force_create=self.source.config.force_ignore_checkpoint)

        # Set job status to running
        self.backend.update_stream_job_status(job_id=job.id, job_status=JobStatus.RUNNING)

        return job

    @abstractmethod
    def run(self) -> bool:
        """Run the pipeline with dedicated adapter for source and destination"""
        pass


class RunnerFactory:
    @staticmethod
    def create_from_config_dict(config: dict) -> AbstractRunner:

        bizon_config = BizonConfig.model_validate(obj=config)

        if bizon_config.engine.runner.type == RunnerTypes.THREADS:
            from .runners.thread import ThreadRunner

            return ThreadRunner(config=config)

        raise ValueError(f"Runner type {bizon_config.engine.runner.type} is not supported")

    @staticmethod
    def create_from_yaml(filepath: str) -> AbstractRunner:
        config = parse_from_yaml(filepath)
        return RunnerFactory.create_from_config_dict(config)
