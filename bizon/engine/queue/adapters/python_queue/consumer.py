import multiprocessing
import multiprocessing.synchronize
import threading
from typing import Union

from loguru import logger

from bizon.destination.destination import AbstractDestination
from bizon.engine.pipeline.consumer import AbstractQueueConsumer
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.queue.queue import AbstractQueue
from bizon.engine.queue.config import QueueMessage
from bizon.transform.transform import Transform

from .config import PythonQueueConfig


class PythonQueueConsumer(AbstractQueueConsumer):
    def __init__(
        self, config: PythonQueueConfig, queue: AbstractQueue, destination: AbstractDestination, transform: Transform
    ):
        super().__init__(config, destination=destination, transform=transform)
        self.queue = queue

    def run(self, stop_event: Union[threading.Event, multiprocessing.synchronize.Event]) -> PipelineReturnStatus:

        while True:

            # Handle kill signal from the runner
            if stop_event.is_set():
                logger.info("Stop event is set, closing consumer ...")
                return PipelineReturnStatus.KILLED_BY_RUNNER

            # Retrieve the message from the queue
            queue_message: QueueMessage = self.queue.get()

            status = self.process_queue_message(queue_message)

            if status != PipelineReturnStatus.RUNNING:
                self.queue.task_done()
                return status
