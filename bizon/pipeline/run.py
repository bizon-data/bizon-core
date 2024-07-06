import time

from .consumer import DestinationThread
from .producer import SourceThread
from .queue import PipelineQueue


class Runner:
    def __init__(self, config):
        self.config = config
        self.queue = PipelineQueue()

    def run(self):
        source = SourceThread(queue=self.queue)
        destination = DestinationThread(queue=self.queue)

        source.start()
        time.sleep(2)

        destination.start()
        time.sleep(2)
