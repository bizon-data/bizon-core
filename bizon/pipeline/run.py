import time

from bizon.cli.parser import parse_from_yaml

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
        return self._is_running

    @classmethod
    def from_yaml(cls, filepath: str):
        """Create a Runner instance from a yaml file"""
        config = parse_from_yaml(filepath)
        return cls(config=config)

    def run(self):
        """Run the pipeline with dedicated threads for source and destination"""
        source = SourceThread(queue=self.queue, source_config=self.config.get("source", {}))
        destination = DestinationThread(queue=self.queue, destination_config=self.config.get("destination", {}))

        source.start()
        self._is_running = True
        time.sleep(2)

        destination.start()
        time.sleep(2)

        while source.is_alive():
            time.sleep(1)
        self._is_running = False
