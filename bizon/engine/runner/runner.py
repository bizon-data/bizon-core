import multiprocessing
import multiprocessing.synchronize
import os
import sys
import threading
from abc import ABC, abstractmethod
from typing import Union

from loguru import logger

from bizon.alerting.models import AlertMethod
from bizon.cli.utils import parse_from_yaml
from bizon.common.models import BizonConfig, SyncMetadata
from bizon.destination.destination import AbstractDestination, DestinationFactory
from bizon.engine.backend.backend import AbstractBackend, BackendFactory
from bizon.engine.backend.models import JobStatus, StreamJob
from bizon.engine.pipeline.producer import Producer
from bizon.engine.queue.queue import AbstractQueue, QueueFactory
from bizon.engine.runner.config import RunnerStatus
from bizon.monitoring.monitor import AbstractMonitor, MonitorFactory
from bizon.source.callback import AbstractSourceCallback
from bizon.source.config import SourceSyncModes
from bizon.source.discover import get_source_instance_by_source_and_stream
from bizon.source.source import AbstractSource
from bizon.transform.transform import Transform


class AbstractRunner(ABC):
    def __init__(self, config: dict):

        # Internal state
        self._is_running: bool = False

        self.config = config
        self.bizon_config = BizonConfig.model_validate(obj=self.config)

        # Set pipeline information as environment variables
        os.environ["BIZON_SYNC_NAME"] = self.bizon_config.name
        os.environ["BIZON_SOURCE_NAME"] = self.bizon_config.source.name
        os.environ["BIZON_SOURCE_STREAM"] = self.bizon_config.source.stream
        os.environ["BIZON_DESTINATION_NAME"] = self.bizon_config.destination.name

        # Set log level
        logger.info(f"Setting log level to {self.bizon_config.engine.runner.log_level.name}")
        logger.remove()
        logger.add(sys.stderr, level=self.bizon_config.engine.runner.log_level)

        if self.bizon_config.alerting:
            logger.info(f"Setting up alerting method {self.bizon_config.alerting.type}")
            if self.bizon_config.alerting.type == AlertMethod.SLACK:
                from bizon.alerting.slack.handler import SlackHandler

                alert = SlackHandler(
                    config=self.bizon_config.alerting.config,
                    log_levels=self.bizon_config.alerting.log_levels,
                )
            alert.add_handlers()

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

        logger.info(f"Creating client for {bizon_config.source.name} - {bizon_config.source.stream} ...")

        # Get the client class, validate the config and return the client
        return get_source_instance_by_source_and_stream(
            source_name=bizon_config.source.name,
            stream_name=bizon_config.source.stream,
            source_config=config["source"],  # We pass the raw config to have flexibility for custom sources
        )

    @staticmethod
    def get_destination(
        bizon_config: BizonConfig, backend: AbstractBackend, job_id: str, source_callback: AbstractSourceCallback
    ) -> AbstractDestination:
        """Get an instance of the destination based on the destination config dict"""

        sync_metadata = SyncMetadata.from_bizon_config(job_id=job_id, config=bizon_config)

        return DestinationFactory.get_destination(
            sync_metadata=sync_metadata,
            config=bizon_config.destination,
            backend=backend,
            source_callback=source_callback,
        )

    @staticmethod
    def get_backend(bizon_config: BizonConfig, **kwargs) -> AbstractBackend:
        """Get an instance of the backend based on the backend config dict"""
        return BackendFactory.get_backend(config=bizon_config.engine.backend, **kwargs)

    @staticmethod
    def get_producer(
        bizon_config: BizonConfig, source: AbstractSource, queue: AbstractQueue, backend: AbstractBackend
    ) -> Producer:
        return Producer(
            bizon_config=bizon_config,
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
    def get_transform(bizon_config: BizonConfig) -> Transform:
        """Return the transform instance to apply to the source records"""
        return Transform(transforms=bizon_config.transforms)

    @staticmethod
    def get_monitoring_client(bizon_config: BizonConfig) -> AbstractMonitor:
        """Return the monitoring client instance"""
        return MonitorFactory.get_monitor(bizon_config)

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
            name=bizon_config.name,
            source_name=bizon_config.source.name,
            stream_name=bizon_config.source.stream,
            session=session,
        )

        if job:
            # If force_create and a job is already running, we cancel it and create a new one
            if force_create:
                logger.info("Found an existing job, cancelling it...")
                backend.update_stream_job_status(job_id=job.id, job_status=JobStatus.CANCELED)
                logger.info(f"Job {job.id} canceled. Creating a new one...")
            # Otherwise we return the existing job
            else:
                logger.info(f"Found an existing job: {job.id}")
                return job

        # If no job is running, we create a new one:
        # Get the total number of records
        if bizon_config.source.sync_mode == SourceSyncModes.STREAM:
            total_records = None  # Not available for stream mode
        else:
            total_records = source.get_total_records_count()

        # Create a new job
        job = backend.create_stream_job(
            name=bizon_config.name,
            source_name=bizon_config.source.name,
            stream_name=bizon_config.source.stream,
            sync_mode=bizon_config.source.sync_mode,
            total_records_to_fetch=total_records,
            session=session,
            job_status=JobStatus.STARTED,
        )

        logger.info(f"Created a new job: {job.id}")

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
        logger.info(f"Connection to source {bizon_config.source.name} - {bizon_config.source.stream} successful")

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
    def instanciate_and_run_producer(
        bizon_config: BizonConfig,
        config: dict,
        job_id: str,
        stop_event: Union[multiprocessing.synchronize.Event, threading.Event],
        **kwargs,
    ):

        # Get the source instance
        source = AbstractRunner.get_source(bizon_config=bizon_config, config=config)

        # Get the queue instance
        queue = AbstractRunner.get_queue(bizon_config=bizon_config, **kwargs)

        # Get the backend instance
        backend = AbstractRunner.get_backend(bizon_config=bizon_config, **kwargs)

        # Create the producer instance
        producer = AbstractRunner.get_producer(
            bizon_config=bizon_config,
            source=source,
            queue=queue,
            backend=backend,
        )

        # Run the producer
        status = producer.run(job_id, stop_event)
        return status

    @staticmethod
    def instanciate_and_run_consumer(
        bizon_config: BizonConfig,
        config: dict,
        job_id: str,
        stop_event: Union[multiprocessing.synchronize.Event, threading.Event],
        **kwargs,
    ):
        # Get the source callback instance
        source_callback = AbstractRunner.get_source(
            bizon_config=bizon_config, config=config
        ).get_source_callback_instance()

        # Get the queue instance
        queue = AbstractRunner.get_queue(bizon_config=bizon_config, **kwargs)

        # Get the backend instance
        backend = AbstractRunner.get_backend(bizon_config=bizon_config, **kwargs)

        # Get the destination instance
        destination = AbstractRunner.get_destination(
            bizon_config=bizon_config, backend=backend, job_id=job_id, source_callback=source_callback
        )

        # Get the transform instance
        transform = AbstractRunner.get_transform(bizon_config=bizon_config)

        # Get the monitor instance
        monitor = AbstractRunner.get_monitoring_client(bizon_config=bizon_config)

        # Create the consumer instance
        consumer = queue.get_consumer(
            destination=destination,
            transform=transform,
            monitor=monitor,
        )

        # Run the consumer
        status = consumer.run(stop_event)
        return status

    @abstractmethod
    def run(self) -> RunnerStatus:
        """Run the pipeline with dedicated adapter for source and destination"""
        pass
