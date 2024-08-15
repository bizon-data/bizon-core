import time

from loguru import logger

from bizon.api.source import APISource
from bizon.backend.client import BackendClient
from bizon.cli.parser import parse_from_yaml
from bizon.common.config import SyncConfig
from bizon.sources import SOURCES

from .constants import CONSUMER_START_DELAY, IS_ALIVE_CHECK_INTERVAL
from .consumer import DestinationThread
from .producer import SourceThread
from .queue import PipelineQueue


class Runner:
    def __init__(self, config: dict):
        self.raw_config = config
        self.basic_config_validation()
        self.config = SyncConfig.model_validate(obj=config)
        self.queue = PipelineQueue()
        self._is_running: bool = False

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
        for key in ["source", "backend", "destination"]:
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

    def run(self) -> bool:
        """Run the pipeline with dedicated threads for source and destination"""

        # Create the source thread
        source = self.get_source()
        source_backend = self.get_backend()
        source_thread = SourceThread(queue=self.queue, source=source, backend=source_backend)

        # Create the destination thread
        destination_thread = DestinationThread(queue=self.queue, destination_config=self.config.destination)

        # Start the source thread (producer)
        source_thread.start()
        self._is_running = True
        time.sleep(CONSUMER_START_DELAY)

        # Start the destination thread (consumer)
        destination_thread.start()

        while source_thread.is_alive():
            time.sleep(IS_ALIVE_CHECK_INTERVAL)
        self._is_running = False
        return True
