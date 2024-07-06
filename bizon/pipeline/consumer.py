from threading import Thread

from loguru import logger

from bizon.destinations.models.config import DestinationConfig

from .constants import QUEUE_TERMINATION
from .queue import PipelineQueue


class DestinationThread(Thread):
    def __init__(self, queue: PipelineQueue, destination_config: DestinationConfig):
        super(DestinationThread, self).__init__()
        self.name = "destination"
        self.queue = queue

    def run(self) -> None:
        while True:
            message = self.queue.get()
            print("Consuming data from queue: ", len(message))

            if isinstance(message, str) and message == QUEUE_TERMINATION:
                logger.info("[Destination] Terminating thread ...")
                break
