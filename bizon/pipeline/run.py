import time

from bizon.common.models.config import SyncConfig

from .consumer import DestinationThread
from .producer import SourceThread
from .queue import PipelineQueue


class Runner:
    def __init__(self, config: SyncConfig):
        self.config = config
        self.queue = PipelineQueue()

    def run(self):
        source = SourceThread(queue=self.queue, source_config=self.config.source)
        destination = DestinationThread(queue=self.queue, destination_config=self.config.destination)

        source.start()
        time.sleep(2)

        destination.start()
        time.sleep(2)
