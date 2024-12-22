import random
import time
from multiprocessing import Queue
from typing import Union

from loguru import logger

from bizon.destination.destination import AbstractDestination
from bizon.engine.queue.config import QUEUE_TERMINATION, QueueMessage
from bizon.engine.queue.queue import AbstractQueue, AbstractQueueConsumer
from bizon.source.models import SourceIteration
from bizon.transform.transform import Transform

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

    def get_consumer(self, destination: AbstractDestination, transform: Transform) -> AbstractQueueConsumer:
        return PythonQueueConsumer(config=self.config, queue=self.queue, destination=destination, transform=transform)

    def put_queue_message(self, queue_message: QueueMessage):
        if not self.queue.full():
            self.queue.put(queue_message)
            logger.debug(f"Putting data from iteration {queue_message.iteration} items in queue)")
        else:
            logger.warning("Queue is full, waiting for consumer to consume data")
            time.sleep(random.random())
            self.put_queue_message(queue_message)

    def get(self) -> QueueMessage:
        if not self.queue.empty():
            queue_message: QueueMessage = self.queue.get()
            logger.debug(f"Got {len(queue_message.df_source_records.height)} records from queue")
            return queue_message
        else:
            logger.debug("Queue is empty, waiting for producer to produce data")
            time.sleep(random.random())
            return self.get()

    def get_size(self) -> Union[int, None]:
        if hasattr(self.queue, "qsize"):
            return self.queue.qsize()
        return None

    def terminate(self, iteration: int) -> bool:
        self.put(
            source_iteration=SourceIteration(next_pagination={}, records=[]),
            iteration=iteration,
            signal=QUEUE_TERMINATION,
        )
        logger.info("Sent termination signal to destination.")
        return True
