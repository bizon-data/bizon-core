import queue
import random
import time
from typing import List

from loguru import logger

from bizon.common.models import SyncMetadata
from bizon.destinations.destination import AbstractDestination
from bizon.engine.queue.queue import (
    QUEUE_TERMINATION,
    AbstractQueue,
    AbstractQueueConsumer,
    QueueMessage,
)
from bizon.source.models import SourceRecord

from .config import PythonQueueConfigDetails
from .consumer import PythonQueueConsumer


class PythonQueue(AbstractQueue):

    def __init__(self, config: PythonQueueConfigDetails) -> None:
        super().__init__(config)
        self.config: PythonQueueConfigDetails = config
        self.queue = queue.Queue(maxsize=self.config.queue.max_size)

    def connect(self):
        # No connection to establish for PythonQueue
        pass

    def get_consumer(self, destination: AbstractDestination) -> AbstractQueueConsumer:
        return PythonQueueConsumer(config=self.config, queue=self.queue, destination=destination)

    def put(self, source_records: List[SourceRecord], iteration: int, signal: str = None):
        if not self.queue.full():
            self.queue.put(QueueMessage(iteration=iteration, source_records=source_records, signal=signal).model_dump())
            logger.debug(f"Putting data from iteration {iteration} ({self.queue.qsize()} items in queue)")
        else:
            logger.warning("Queue is full, waiting for consumer to consume data")
            time.sleep(random.random())
            self.put(source_records=source_records, iteration=iteration, signal=signal)

    def get(self) -> QueueMessage:
        if not self.queue.empty():
            data = self.queue.get()
            queue_message = QueueMessage.model_validate(data)
            logger.debug(f"Got {len(queue_message.source_records)} records from queue")
            return queue_message
        else:
            logger.debug("Queue is empty, waiting for producer to produce data")
            time.sleep(random.random())
            return self.get()

    def terminate(self, iteration: int) -> bool:
        self.put(source_records=[], iteration=iteration, signal=QUEUE_TERMINATION)
        logger.info("Sent termination signal to destination.")
        return True
