import queue
import random
import time
from typing import List

from loguru import logger

from bizon.pipeline.queue import AbstractQueue, QueueMessage


class PythonnQueue(AbstractQueue):
    def __init__(self, queue_size: int = 1000):
        self.queue = queue.Queue(maxsize=queue_size)

    def put(self, records: List[dict], topic: str, index: int, signal: str = None):
        if not self.queue.full():
            self.queue.put(QueueMessage(index=index, topic=topic, records=records, signal=signal).model_dump())
            logger.debug(f"Putting data from index {index} ({self.queue.qsize()} items in queue)")
        else:
            logger.debug("Queue is full, waiting for consumer to consume data")
            time.sleep(random.random())
            self.put(records=records, topic=topic, index=index, signal=signal)

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

    def terminate(self):
        self.queue.shutdown()
        logger.debug("Terminating queue ...")
