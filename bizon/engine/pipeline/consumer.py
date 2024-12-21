import multiprocessing
import multiprocessing.synchronize
import threading
import traceback
from abc import ABC, abstractmethod
from typing import Union

from loguru import logger

from bizon.destination.destination import AbstractDestination
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.queue.config import AbstractQueueConfig, QueueMessage
from bizon.engine.queue.config import QUEUE_TERMINATION
from bizon.transform.transform import Transform


class AbstractQueueConsumer(ABC):
    def __init__(self, config: AbstractQueueConfig, destination: AbstractDestination, transform: Transform):
        self.config = config
        self.destination = destination
        self.transform = transform

    @abstractmethod
    def run(self, stop_event: Union[multiprocessing.synchronize.Event, threading.Event]) -> PipelineReturnStatus:
        pass

    def process_queue_message(self, queue_message: QueueMessage) -> PipelineReturnStatus:

        # Apply the transformation
        try:
            df_source_records = self.transform.apply_transforms(df_source_records=queue_message.df_source_records)
        except Exception as e:
            logger.error(f"Error applying transformation: {e}")
            logger.error(traceback.format_exc())
            return PipelineReturnStatus.TRANSFORM_ERROR

        # Handle last iteration
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
                return PipelineReturnStatus.SUCCESS

        except Exception as e:
            logger.error(f"Error writing records to destination: {e}")
            return PipelineReturnStatus.DESTINATION_ERROR

        # Write the records to the destination
        try:
            self.destination.write_records_and_update_cursor(
                df_source_records=df_source_records,
                iteration=queue_message.iteration,
                extracted_at=queue_message.extracted_at,
                pagination=queue_message.pagination,
            )
            return PipelineReturnStatus.RUNNING

        except Exception as e:
            logger.error(f"Error writing records to destination: {e}")
            return PipelineReturnStatus.DESTINATION_ERROR

        raise RuntimeError("Should not reach this point")
