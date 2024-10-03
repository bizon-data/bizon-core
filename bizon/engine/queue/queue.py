from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel
from pytz import UTC

from bizon.destinations.destination import AbstractDestination
from bizon.engine.pipeline.consumer import AbstractQueueConsumer
from bizon.source.models import SourceRecord

from .config import AbastractQueueConfigDetails, AbstractQueueConfig, QueueTypes

QUEUE_TERMINATION = "TERMINATION"


class QueueMessage(BaseModel):
    iteration: int
    source_records: List[SourceRecord]
    extracted_at: datetime = datetime.now(tz=UTC)
    pagination: Optional[dict] = None
    signal: Optional[str] = None


class AbstractQueue(ABC):
    def __init__(self, config: AbastractQueueConfigDetails) -> None:
        self.config = config

    @abstractmethod
    def connect(self):
        """Connect to the queue system"""
        pass

    @abstractmethod
    def get_consumer(self, destination: AbstractDestination) -> AbstractQueueConsumer:
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
    def terminate(self, iteration: int) -> bool:
        """Send a termination signal in the queue system"""
        pass

    def put(
        self,
        source_records: List[SourceRecord],
        iteration: int,
        signal: str = None,
        extracted_at: datetime = None,
        pagination: dict = None,
    ):
        queue_message = QueueMessage(
            iteration=iteration,
            source_records=source_records,
            extracted_at=extracted_at if extracted_at else datetime.now(tz=UTC),
            pagination=pagination,
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
