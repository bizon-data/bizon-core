import time

from loguru import logger

from bizon.api.client import APIClient
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

    def get_client_from_config(self, raw_source_config: dict) -> APIClient:
        """Get an instance of the source based on the source config dict"""

        # Get the client and config class
        source_name = raw_source_config.get("name")
        stream_name = raw_source_config.get("stream_name")

        logger.info(f"Creating client for {source_name} - {stream_name} ...")

        # Get the client class, validate the config and return the client
        connector = SOURCES.get_instance(
            source_name=source_name, stream_name=stream_name, raw_source_config=raw_source_config
        )
        return connector

    def run(self) -> bool:
        """Run the pipeline with dedicated threads for source and destination"""

        # Create the source
        client = self.get_client_from_config(self.config.get("source", {}))
        source = SourceThread(queue=self.queue, client=client)

        # Create the destination
        destination = DestinationThread(queue=self.queue, destination_config=self.config.get("destination", {}))

        # Start the source thread (producer)
        source.start()
        self._is_running = True
        time.sleep(CONSUMER_START_DELAY)

        # Start the destination thread (consumer)
        destination.start()

        while source.is_alive():
            time.sleep(IS_ALIVE_CHECK_INTERVAL)
        self._is_running = False
        return True
