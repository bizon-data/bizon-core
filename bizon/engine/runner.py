from abc import ABC, abstractmethod

from loguru import logger

from bizon.cli.parser import parse_from_yaml
from bizon.common.models import BizonConfig
from bizon.destinations.destination import AbstractDestination, DestinationFactory
from bizon.engine.backend.backend import Backend
from bizon.engine.producer import Producer
from bizon.engine.queue.queue import AbstractQueue, QueueFactory
from bizon.source.source import Source
from bizon.sources import SOURCES

from .config import RunnerTypes


class AbstractRunner(ABC):
    def __init__(self, config: dict):
        self.config: dict = config
        self.bizon_config = BizonConfig.model_validate(obj=config)
        self._is_running: bool = False

        # Instaniate the source, backend and destination
        self.source: Source = self.get_source()
        self.backend: Backend = self.get_backend()
        self.queue: AbstractQueue = self.get_queue()
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
            source_name=self.bizon_config.source.source_name,
            stream_name=self.bizon_config.source.stream_name,
            destination_config_dict=self.config["destination"],
        )

    def get_backend(self) -> Backend:
        """Get an instance of the backend based on the backend config dict"""
        return Backend(config=self.bizon_config.engine.backend)

    def get_producer(self) -> Producer:
        return Producer(queue=self.queue, source=self.source, backend=self.backend)

    def get_queue(self):
        return QueueFactory.get_queue(config=self.bizon_config.engine.queue)

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
