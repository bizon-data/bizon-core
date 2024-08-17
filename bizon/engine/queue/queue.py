from abc import ABC, abstractmethod
from typing import List, Optional

from pydantic import BaseModel

from bizon.destinations.destination import AbstractDestination
from bizon.source.models import SourceRecord

from .config import QueueConfig, QueueTypes


class QueueMessage(BaseModel):
    index: int
    source_records: List[SourceRecord]
    signal: Optional[str] = None


class AbstractQueueConsumer(ABC):
    def __init__(self, config: QueueConfig, destination: AbstractDestination):
        self.config = config
        self.destination = destination

    @abstractmethod
    def run(self):
        pass


class AbstractQueue(ABC):
    def __init__(self, config: QueueConfig) -> None:
        self.config = config

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def get_consumer(self, destination: AbstractDestination) -> AbstractQueueConsumer:
        pass

    @abstractmethod
    def put(self, source_records: List[SourceRecord], index: int, signal: str = None):
        pass

    @abstractmethod
    def get(self) -> QueueMessage:
        pass

    @abstractmethod
    def terminate(self) -> bool:
        pass


class QueueFactory:
    @staticmethod
    def get_queue(config: QueueConfig) -> AbstractQueue:
        if config.type == QueueTypes.PYTHON_QUEUE:
            from .adapters.python_queue.queue import PythonQueue

            return PythonQueue(config=config.config)

        if config.type == QueueTypes.KAFKA:
            from .adapters.kafka.queue import KafkaQueue

            return KafkaQueue(config=config.config)

        raise ValueError(f"Queue type {config.type} is not supported")
