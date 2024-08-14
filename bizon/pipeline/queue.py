import queue
import random
import time
from typing import List

from loguru import logger


class PipelineQueue:
    def __init__(self, queue_size: int = 1000):
        self.queue = queue.Queue(maxsize=queue_size)

    def put(self, records: List[dict], index: int):
        if not self.queue.full():
            self.queue.put(records)
            logger.debug(f"Putting data from index {index} ({self.queue.qsize()} items in queue)")
        else:
            logger.debug("Queue is full, waiting for consumer to consume data")
            time.sleep(random.random())
            self.put(records, index)

    def get(self) -> dict:
        if not self.queue.empty():
            data = self.queue.get()
            logger.debug(f"Got {len(data)} records from queue")
            return data
        else:
            logger.debug("Queue is empty, waiting for producer to produce data")
            time.sleep(random.random())
            return self.get()

    def terminate(self):
        self.queue.shutdown()
        logger.debug("Terminating queue ...")
