import multiprocessing
import multiprocessing.synchronize
import threading
import traceback
from typing import Union

from loguru import logger

from bizon.destination.destination import AbstractDestination
from bizon.engine.pipeline.consumer import AbstractQueueConsumer
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.queue.queue import QUEUE_TERMINATION, AbstractQueue, QueueMessage
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
            if stop_event.is_set():
                logger.info("Stop event is set, closing consumer ...")
                return PipelineReturnStatus.KILLED_BY_RUNNER
            # Retrieve the message from the queue
            queue_message: QueueMessage = self.queue.get()

            # Apply the transformation
            try:
                df_source_records = self.transform.apply_transforms(df_source_records=queue_message.df_source_records)
            except Exception as e:
                logger.error(f"Error applying transformation: {e}")
                logger.error(traceback.format_exc())
                return PipelineReturnStatus.TRANSFORM_ERROR

            try:
                if queue_message.signal == QUEUE_TERMINATION:
                    logger.info("Received termination signal, waiting for destination to close gracefully ...")
                    self.destination.write_records_and_update_cursor(
                        df_source_records=df_source_records,
                        iteration=queue_message.iteration,
                        extracted_at=queue_message.extracted_at,
                        pagination=queue_message.pagination,
                        last_iteration=True,
                    )
                    break
            except Exception as e:
                logger.error(f"Error writing records to destination: {e}")
                return PipelineReturnStatus.DESTINATION_ERROR

            try:
                self.destination.write_records_and_update_cursor(
                    df_source_records=df_source_records,
                    iteration=queue_message.iteration,
                    extracted_at=queue_message.extracted_at,
                    pagination=queue_message.pagination,
                )
            except Exception as e:
                logger.error(f"Error writing records to destination: {e}")
                return PipelineReturnStatus.DESTINATION_ERROR

            self.queue.task_done()
        return PipelineReturnStatus.SUCCESS
