import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

import polars as pl
from pytz import UTC

from bizon.destination.destination import AbstractDestination
from bizon.engine.pipeline.consumer import AbstractQueueConsumer
from bizon.source.models import SourceIteration, source_record_schema
from bizon.transform.transform import Transform

from .config import (
    AbastractQueueConfigDetails,
    AbstractQueueConfig,
    QueueMessage,
    QueueTypes,
)


class AbstractQueue(ABC):
    def __init__(self, config: AbastractQueueConfigDetails) -> None:
        self.config = config

    @abstractmethod
    def connect(self):
        """Connect to the queue system"""
        pass

    @abstractmethod
    def get_consumer(self, destination: AbstractDestination, transform: Transform) -> AbstractQueueConsumer:
        pass

    @abstractmethod
    def put_queue_message(self, queue_message: QueueMessage):
        """Put a QueueMessage object in the queue system"""
        pass

    @abstractmethod
    def get(self) -> QueueMessage:
        """Get a QueueMessage object from the queue system"""
        pass

    @abstractmethod
    def get_size(self) -> Union[int, None]:
        """If queue is compatible, return size of the queue"""
        pass

    @abstractmethod
    def terminate(self, iteration: int) -> bool:
        """Send a termination signal in the queue system"""
        pass

    def put(
        self,
        source_iteration: SourceIteration,
        iteration: int,
        signal: str = None,
        extracted_at: datetime = None,
    ):
        # Create a DataFrame from the SourceIteration records
        df_source_records = pl.DataFrame(
            {
                "id": [record.id for record in source_iteration.records],
                "data": [json.dumps(record.data, ensure_ascii=False) for record in source_iteration.records],
                "timestamp": [record.timestamp for record in source_iteration.records],
            },
            schema=source_record_schema,
        )

        queue_message = QueueMessage(
            iteration=iteration,
            df_source_records=df_source_records,
            extracted_at=extracted_at if extracted_at else datetime.now(tz=UTC),
            pagination=source_iteration.next_pagination,
            signal=signal,
        )

        self.put_queue_message(queue_message)


class QueueFactory:
    @staticmethod
    def get_queue(
        config: AbstractQueueConfig,
        **kwargs,
    ) -> AbstractQueue:
        if config.type == QueueTypes.PYTHON_QUEUE:
            from .adapters.python_queue.queue import PythonQueue

            # For PythonQueue, queue param is required in kwargs
            # It contains an instance of multiprocessing.Queue
            return PythonQueue(config=config.config, **kwargs)

        if config.type == QueueTypes.KAFKA:
            from .adapters.kafka.queue import KafkaQueue

            return KafkaQueue(config=config.config)

        if config.type == QueueTypes.RABBITMQ:
            from .adapters.rabbitmq.queue import RabbitMQ

            return RabbitMQ(config=config.config)

        raise ValueError(f"Queue type {config.type} is not supported")
