import time

from loguru import logger

from bizon.api.source import APISource
from bizon.backend.client import BackendClient
from bizon.cli.parser import parse_from_yaml
from bizon.sources import SOURCES

from .constants import CONSUMER_START_DELAY, IS_ALIVE_CHECK_INTERVAL
from .consumer import DestinationThread
from .producer import SourceThread
from .queue import PipelineQueue


class Runner:
    def __init__(self, config: dict):
        self.config = config
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

    def get_source_from_config(self, raw_source_config: dict) -> APISource:
        """Get an instance of the source based on the source config dict"""

        # Get the client and config class
        source_name = raw_source_config.get("source_name")
        if not source_name:
            raise ValueError("source_name is required in the source config")

        stream_name = raw_source_config.get("stream_name")
        if not stream_name:
            raise ValueError("stream_name is required in the source config")

        logger.info(f"Creating client for {source_name} - {stream_name} ...")

        # Get the client class, validate the config and return the client
        connector = SOURCES.get_instance(
            source_name=source_name, stream_name=stream_name, raw_source_config=raw_source_config
        )
        return connector

    def get_backend_from_config(self, raw_backend_config: dict) -> BackendClient:
        """Get an instance of the backend based on the backend config dict"""
        return BackendClient.from_raw_config(raw_backend_config)

    def run(self) -> bool:
        """Run the pipeline with dedicated threads for source and destination"""

        for key in ["source", "backend", "destination"]:
            if key not in self.config:
                raise ValueError(f"Missing {key} in the config, please provide it")

        # Create the source
        source = self.get_source_from_config(self.config["source"])
        source_backend = self.get_backend_from_config(self.config["backend"])
        source_thread = SourceThread(queue=self.queue, source=source, backend=source_backend)

        # Create the destination
        destination = DestinationThread(queue=self.queue, destination_config=self.config["destination"])

        # Start the source thread (producer)
        source_thread.start()
        self._is_running = True
        time.sleep(CONSUMER_START_DELAY)

        # Start the destination thread (consumer)
        destination.start()

        while source_thread.is_alive():
            time.sleep(IS_ALIVE_CHECK_INTERVAL)
        self._is_running = False
        return True
