import multiprocessing
import multiprocessing.synchronize
import threading
import traceback
from abc import ABC, abstractmethod
from typing import Union

from loguru import logger

from bizon.destination.destination import AbstractDestination
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.queue.config import (
    QUEUE_TERMINATION,
    QUEUE_TERMINATION_ERROR,
    AbstractQueueConfig,
    QueueMessage,
)
from bizon.monitoring.monitor import AbstractMonitor
from bizon.transform.transform import Transform


class AbstractQueueConsumer(ABC):
    def __init__(
        self,
        config: AbstractQueueConfig,
        destination: AbstractDestination,
        transform: Transform,
        monitor: AbstractMonitor,
    ):
        self.config = config
        self.destination = destination
        self.transform = transform
        self.monitor = monitor

    @abstractmethod
    def run(self, stop_event: Union[multiprocessing.synchronize.Event, threading.Event]) -> PipelineReturnStatus:
        pass

    def process_queue_message(self, queue_message: QueueMessage) -> PipelineReturnStatus:
        # The producer aborted on an error. Stop WITHOUT finalizing: do not write a
        # last iteration, which would run the destination's finalize() (for BigQuery,
        # the WRITE_TRUNCATE swap of the temp table into the production table) and set
        # the stream job to SUCCEEDED on top of a partial extract.
        #
        # Checked before the transform so a transform error cannot mask the producer's
        # failure, and so we never touch the destination on this path.
        #
        # The job is deliberately left RUNNING. That is the state get_or_create_job()
        # already knows how to recover from: a leftover running full_refresh job is
        # cancelled and restarted from page 1 on the next run.
        if queue_message.signal == QUEUE_TERMINATION_ERROR:
            logger.error("Received error termination signal from producer, aborting without finalizing destination.")
            self.monitor.track_pipeline_status(PipelineReturnStatus.SOURCE_ERROR)
            return PipelineReturnStatus.SOURCE_ERROR

        # Apply the transformation
        try:
            df_source_records = self.transform.apply_transforms(df_source_records=queue_message.df_source_records)
        except Exception as e:
            logger.error(f"Error applying transformation: {e}")
            logger.error(traceback.format_exc())
            self.monitor.track_pipeline_status(PipelineReturnStatus.TRANSFORM_ERROR)
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
                self.monitor.track_pipeline_status(PipelineReturnStatus.SUCCESS)
                return PipelineReturnStatus.SUCCESS

        except Exception as e:
            logger.error(f"Error writing records to destination: {e}")
            self.monitor.track_pipeline_status(PipelineReturnStatus.DESTINATION_ERROR)
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
            self.monitor.track_pipeline_status(PipelineReturnStatus.DESTINATION_ERROR)
            return PipelineReturnStatus.DESTINATION_ERROR

        raise RuntimeError("Should not reach this point")
