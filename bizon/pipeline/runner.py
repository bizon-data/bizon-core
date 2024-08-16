from abc import ABC, abstractmethod

from loguru import logger

from bizon.api.source import APISource
from bizon.backend.client import BackendClient
from bizon.cli.parser import parse_from_yaml
from bizon.common.config import SyncConfig
from bizon.sources import SOURCES

from .config import RunnerTypes
from .producer import Producer
from .queue import AbstractQueue


class AbstractRunner(ABC):
    def __init__(self, config: dict):
        self.raw_config = config
        self.basic_config_validation()
        self.config = SyncConfig.model_validate(obj=config)
        self._is_running: bool = False

        # Instaniate the source, backend and destination
        self.source: APISource = self.get_source()
        self.backend: BackendClient = self.get_backend()
        self.queue: AbstractQueue = self.get_queue()

    @property
    def is_running(self) -> bool:
        """Check if the pipeline is running"""
        return self._is_running

    @classmethod
    def from_yaml(cls, filepath: str):
        """Create a Runner instance from a yaml file"""
        config = parse_from_yaml(filepath)
        return cls(config=config)

    def basic_config_validation(self):
        for key in ["source", "backend", "consumer"]:
            if key not in self.raw_config:
                raise ValueError(f"Missing {key} in the config, please provide it")

    def get_source(self) -> APISource:
        """Get an instance of the source based on the source config dict"""

        logger.info(f"Creating client for {self.config.source.source_name} - {self.config.source.stream_name} ...")
        # Get the client class, validate the config and return the client
        return SOURCES.get_instance(
            source_name=self.config.source.source_name,
            stream_name=self.config.source.stream_name,
            raw_source_config=self.raw_config["source"],
        )

    def get_backend(self) -> BackendClient:
        """Get an instance of the backend based on the backend config dict"""
        return BackendClient.from_raw_config(self.raw_config["backend"])

    def get_producer(self) -> Producer:
        return Producer(queue=self.queue, source=self.source, backend=self.backend)

    @abstractmethod
    def get_queue(self) -> AbstractQueue:
        """Get an instance of the queue based on the messaging system"""
        pass

    @abstractmethod
    def run(self) -> bool:
        """Run the pipeline with dedicated adapter for source and destination"""
        pass


class RunnerFactory:

    @staticmethod
    def create_from_config_dict(config: dict) -> AbstractRunner:

        sync_config = SyncConfig.model_validate(obj=config)

        if sync_config.pipeline.runner == RunnerTypes.THREADS:
            from bizon.pipeline.adapters.threads.runner import ThreadRunner

            return ThreadRunner(config=config)

        if sync_config.pipeline.runner == RunnerTypes.KAFKA:
            from bizon.pipeline.adapters.kafka.runner import KafkaRunner

            return KafkaRunner(config=config)

        raise ValueError(f"Runner type {sync_config.pipeline.runner} is not supported")

    @staticmethod
    def create_from_yaml(filepath: str) -> AbstractRunner:
        config = parse_from_yaml(filepath)
        return RunnerFactory.create_from_config_dict(config)
