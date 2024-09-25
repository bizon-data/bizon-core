from abc import ABC, abstractmethod
from enum import Enum

from bizon.destinations.destination import AbstractDestination
from bizon.engine.queue.config import AbstractQueueConfig


class AbstractQueueConsumer(ABC):
    def __init__(self, config: AbstractQueueConfig, destination: AbstractDestination):
        self.config = config
        self.destination = destination

    @abstractmethod
    def run(self):
        pass
