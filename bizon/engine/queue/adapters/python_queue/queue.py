import random
import time
from multiprocessing import Queue

from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.engine.queue.queue import (
    QUEUE_TERMINATION,
    AbstractQueue,
    AbstractQueueConsumer,
    QueueMessage,
)

from .config import PythonQueueConfigDetails
from .consumer import PythonQueueConsumer


class PythonQueue(AbstractQueue):

    def __init__(self, config: PythonQueueConfigDetails, **kwargs) -> None:
        super().__init__(config)
        self.config: PythonQueueConfigDetails = config

        assert "queue" in kwargs, "queue param passed in kwargs is required for PythonQueue"
        self.queue: Queue = kwargs["queue"]

    def connect(self):
        # No connection to establish for PythonQueue
        pass

    def get_consumer(self, destination: AbstractDestination) -> AbstractQueueConsumer:
        return PythonQueueConsumer(config=self.config, queue=self.queue, destination=destination)

    def put_queue_message(self, queue_message: QueueMessage):
        if not self.queue.full():
            self.queue.put(queue_message.model_dump())
            logger.debug(f"Putting data from iteration {queue_message.iteration} items in queue)")
        else:
            logger.warning("Queue is full, waiting for consumer to consume data")
            time.sleep(random.random())
            self.put_queue_message(queue_message)

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
