from threading import Thread

from loguru import logger

from bizon.common.config import DestinationConfig
from bizon.pipeline.consumer import AbstractConsumer
from bizon.pipeline.queue import QUEUE_TERMINATION

from .queue import PythonnQueue, QueueMessage


class ThreadedConsumer(Thread, AbstractConsumer):
    def __init__(self, queue: PythonnQueue, config: DestinationConfig):
        Thread.__init__(self)
        AbstractConsumer.__init__(self, queue=queue, config=config)

    def run(self) -> None:
        while True:
            queue_message: QueueMessage = self.queue.get()
            print("Consuming data from queue: ", len(queue_message.records))

            if queue_message.signal == QUEUE_TERMINATION:
                logger.info("[Destination] Terminating thread ...")
                break
