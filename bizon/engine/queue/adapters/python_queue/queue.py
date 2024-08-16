import queue
import random
import time
from typing import List

from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.engine.queue.config import QueueConfig
from bizon.engine.queue.queue import AbstractQueue, AbstractQueueConsumer, QueueMessage

from .config import QUEUE_TERMINATION, PythonQueueConfig
from .consumer import PythonQueueConsumer


class PythonQueue(AbstractQueue):
    def __init__(self, config: QueueConfig) -> None:
        super().__init__(config)
        self.queue = queue.Queue(maxsize=config.queue.max_size)

    def get_consumer(self, destination: AbstractDestination) -> AbstractQueueConsumer:
        return PythonQueueConsumer(config=self.config, queue=self.queue, destination=destination)

    def put(self, records: List[dict], index: int, signal: str = None):
        if not self.queue.full():
            self.queue.put(QueueMessage(index=index, records=records, signal=signal).model_dump())
            logger.debug(f"Putting data from index {index} ({self.queue.qsize()} items in queue)")
        else:
            logger.debug("Queue is full, waiting for consumer to consume data")
            time.sleep(random.random())
            self.put(records=records, index=index, signal=signal)

    def get(self) -> QueueMessage:
        if not self.queue.empty():
            data = self.queue.get()
            queue_message = QueueMessage.model_validate(data)
            logger.debug(f"Got {len(queue_message.records)} records from queue")
            return queue_message
        else:
            logger.debug("Queue is empty, waiting for producer to produce data")
            time.sleep(random.random())
            return self.get()

    def terminate(self) -> bool:
        self.put(records=[], index=0, signal=QUEUE_TERMINATION)
        logger.info("Sent termination signal to destination.")
        return True
