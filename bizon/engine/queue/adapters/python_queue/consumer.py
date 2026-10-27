import multiprocessing
import multiprocessing.synchronize
import threading
from queue import Empty
from typing import Union

from loguru import logger

from bizon.destination.destination import AbstractDestination
from bizon.engine.pipeline.consumer import AbstractQueueConsumer
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.queue.config import QueueMessage
from bizon.engine.queue.queue import AbstractQueue
from bizon.monitoring.monitor import AbstractMonitor
from bizon.transform.transform import Transform

from .config import PythonQueueConfig

STOP_EVENT_POLL_SECONDS = 1


class PythonQueueConsumer(AbstractQueueConsumer):
    def __init__(
        self,
        config: PythonQueueConfig,
        queue: AbstractQueue,
        destination: AbstractDestination,
        transform: Transform,
        monitor: AbstractMonitor,
    ):
        super().__init__(
            config,
            destination=destination,
            transform=transform,
            monitor=monitor,
        )
        self.queue = queue
        self.monitor.track_pipeline_status(PipelineReturnStatus.RUNNING)

    def run(self, stop_event: Union[threading.Event, multiprocessing.synchronize.Event]) -> PipelineReturnStatus:
        while True:
            # Handle kill signal from the runner
            if stop_event.is_set():
                logger.info("Stop event is set, closing consumer ...")
                self.monitor.track_pipeline_status(PipelineReturnStatus.KILLED_BY_RUNNER)
                return PipelineReturnStatus.KILLED_BY_RUNNER

            # A blocking get() would never see the stop event, e.g. when the producer died without sending a
            # termination signal.
            try:
                queue_message: QueueMessage = self.queue.get(timeout=STOP_EVENT_POLL_SECONDS)
            except Empty:
                continue

            status = self.process_queue_message(queue_message)

            if status != PipelineReturnStatus.RUNNING:
                self.queue.task_done()
                return status
