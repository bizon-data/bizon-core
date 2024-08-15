from threading import Thread
from typing import List

from loguru import logger

from bizon.common.config import DestinationConfig

from .constants import QUEUE_TERMINATION
from .queue import PipelineQueue, QueueMessage


class DestinationThread(Thread):
    def __init__(self, queue: PipelineQueue, destination_config: DestinationConfig):
        super(DestinationThread, self).__init__()
        self.name = "destination"
        self.queue = queue

    def run(self) -> None:
        while True:
            queue_message: QueueMessage = self.queue.get()
            print("Consuming data from queue: ", len(queue_message.records))

            if queue_message.signal == QUEUE_TERMINATION:
                logger.info("[Destination] Terminating thread ...")
                break
