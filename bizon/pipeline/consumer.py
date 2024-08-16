from abc import ABC, abstractmethod

from bizon.common.config import DestinationConfig

from .queue import AbstractQueue


class AbstractConsumer(ABC):
    def __init__(self, queue: AbstractQueue, config: DestinationConfig):
        super(AbstractConsumer, self).__init__()
        self.config = config
        self.name = f"destination"
        self.queue = queue

    @abstractmethod
    def run(self):
        pass
