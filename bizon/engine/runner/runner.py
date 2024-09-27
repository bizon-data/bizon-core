from abc import ABC, abstractmethod

from loguru import logger

from bizon.cli.utils import parse_from_yaml
from bizon.common.models import BizonConfig, SyncMetadata
from bizon.destinations.destination import AbstractDestination, DestinationFactory
from bizon.engine.backend.backend import AbstractBackend, BackendFactory
from bizon.engine.backend.models import JobStatus, StreamJob
from bizon.engine.pipeline.producer import Producer
from bizon.engine.queue.queue import AbstractQueue, QueueFactory
from bizon.source.discover import get_source_instance_by_source_and_stream
from bizon.source.source import AbstractSource


class AbstractRunner(ABC):
    def __init__(self, config: dict):

        # Internal state
        self._is_running: bool = False

        # Attributes should be serializable for multiprocessing
        self.config: dict = config
        self.bizon_config = BizonConfig.model_validate(obj=config)

    @property
    def is_running(self) -> bool:
        """Return True if the pipeline is running"""
        return self._is_running

    @classmethod
    def from_yaml(cls, filepath: str):
        """Create a Runner instance from a yaml file"""
        config = parse_from_yaml(filepath)
        return cls(config=config)

    @staticmethod
    def get_source(bizon_config: BizonConfig, config: dict) -> AbstractSource:
        """Get an instance of the source based on the source config dict"""

        logger.info(f"Creating client for {bizon_config.source.source_name} - {bizon_config.source.stream_name} ...")

        # Get the client class, validate the config and return the client
        return get_source_instance_by_source_and_stream(
            source_name=bizon_config.source.source_name,
            stream_name=bizon_config.source.stream_name,
            source_config=config["source"],  # We pass the raw config to have flexibility for custom sources
        )

    @staticmethod
    def get_destination(bizon_config: BizonConfig, backend: AbstractBackend, job_id: str) -> AbstractDestination:
        """Get an instance of the destination based on the destination config dict"""

        sync_metadata = SyncMetadata.from_bizon_config(job_id=job_id, config=bizon_config)

        return DestinationFactory.get_destination(
            sync_metadata=sync_metadata,
            config=bizon_config.destination,
            backend=backend,
        )

    @staticmethod
    def get_backend(bizon_config: BizonConfig, **kwargs) -> AbstractBackend:
        """Get an instance of the backend based on the backend config dict"""
        return BackendFactory.get_backend(config=bizon_config.engine.backend, **kwargs)

    @staticmethod
    def get_producer(source: AbstractSource, queue: AbstractQueue, backend: AbstractBackend) -> Producer:
        return Producer(
            source=source,
            queue=queue,
            backend=backend,
        )

    @staticmethod
    def get_queue(bizon_config: BizonConfig, **kwargs) -> AbstractQueue:
        return QueueFactory.get_queue(
            config=bizon_config.engine.queue,
            **kwargs,
        )

    @staticmethod
    def get_or_create_job(
        bizon_config: BizonConfig,
        backend: AbstractBackend,
        source: AbstractSource,
        force_create: bool = False,
        session=None,
    ) -> StreamJob:
        """Get or create a job for the current stream, return its ID"""
        # Retrieve the last job for this stream
        job = backend.get_running_stream_job(
            source_name=bizon_config.source.source_name,
            stream_name=bizon_config.source.stream_name,
            session=session,
        )

        if job:
            # If force_create and a job is already running, we cancel it and create a new one
            if force_create:
                logger.info(f"Found an existing job, cancelling it...")
                backend.update_stream_job_status(job_id=job.id, job_status=JobStatus.CANCELED)
                logger.info(f"Job {job.id} canceled. Creating a new one...")
            # Otherwise we return the existing job
            else:
                logger.debug(f"Found an existing job: {job.id}")
                return job

        # If no job is running, we create a new one:
        # Get the total number of records
        total_records = source.get_total_records_count()

        # Create a new job
        job = backend.create_stream_job(
            source_name=bizon_config.source.source_name,
            stream_name=bizon_config.source.stream_name,
            total_records_to_fetch=total_records,
            session=session,
            job_status=JobStatus.STARTED,
        )

        return job

    @staticmethod
    def init_job(bizon_config: BizonConfig, config: dict, **kwargs) -> StreamJob:
        """Initialize a job for the current stream"""

        backend = AbstractRunner.get_backend(bizon_config=bizon_config, **kwargs)
        backend.check_prerequisites()
        backend.create_all_tables()

        # First we check if the connection is successful and initialize the cursor
        source = AbstractRunner.get_source(bizon_config=bizon_config, config=config)

        check_connection, connection_error = source.check_connection()
        logger.info(
            f"Connection to source {bizon_config.source.source_name} - {bizon_config.source.stream_name} successful"
        )

        if not check_connection:
            logger.error(f"Error while connecting to source: {connection_error}")
            raise ConnectionError(f"Error while connecting to source: {connection_error}")

        # Get or create the job, if force_ignore_checkpoint, we cancel the existing job and create a new one
        job = AbstractRunner.get_or_create_job(
            bizon_config=bizon_config,
            backend=backend,
            source=source,
            force_create=bizon_config.source.force_ignore_checkpoint,
        )

        # Set job status to running
        backend.update_stream_job_status(job_id=job.id, job_status=JobStatus.RUNNING)

        return job

    @staticmethod
    def instanciate_and_run_producer(bizon_config: BizonConfig, config: dict, job_id: str, **kwargs):

        source = AbstractRunner.get_source(bizon_config=bizon_config, config=config)
        queue = AbstractRunner.get_queue(bizon_config=bizon_config, **kwargs)
        backend = AbstractRunner.get_backend(bizon_config=bizon_config, **kwargs)

        producer = AbstractRunner.get_producer(
            source=source,
            queue=queue,
            backend=backend,
        )

        status = producer.run(job_id)
        return status

    @staticmethod
    def instanciate_and_run_consumer(bizon_config: BizonConfig, job_id: str, **kwargs):

        queue = AbstractRunner.get_queue(bizon_config=bizon_config, **kwargs)
        backend = AbstractRunner.get_backend(bizon_config=bizon_config, **kwargs)
        destination = AbstractRunner.get_destination(bizon_config=bizon_config, backend=backend, job_id=job_id)

        consumer = queue.get_consumer(destination=destination)

        status = consumer.run()
        return status

    @abstractmethod
    def run(self) -> bool:
        """Run the pipeline with dedicated adapter for source and destination"""
        pass
