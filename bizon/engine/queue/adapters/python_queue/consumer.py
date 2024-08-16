from loguru import logger

from bizon.destinations.destination import AbstractDestination
from bizon.engine.queue.queue import AbstractQueue, AbstractQueueConsumer, QueueMessage

from .config import QUEUE_TERMINATION, PythonQueueConfig


class PythonQueueConsumer(AbstractQueueConsumer):
    def __init__(self, config: PythonQueueConfig, queue: AbstractQueue, destination: AbstractDestination):
        super().__init__(config, destination=destination)
        self.queue = queue

    def run(self) -> None:
        while True:
            message: dict = self.queue.get()
            queue_message = QueueMessage.model_validate(message)

            self.destination.write_records(queue_message.records)
            logger.info(f"Consumed data from queue: {len(queue_message.records)}")

            if queue_message.signal == QUEUE_TERMINATION:
                logger.info(f"[Destination:{self.destination.name}] Terminating thread ...")
                break
