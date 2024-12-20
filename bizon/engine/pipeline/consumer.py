import multiprocessing
import multiprocessing.synchronize
import threading
from abc import ABC, abstractmethod
from typing import Union

from bizon.destination.destination import AbstractDestination
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.queue.config import AbstractQueueConfig
from bizon.transform.transform import Transform


class AbstractQueueConsumer(ABC):
    def __init__(self, config: AbstractQueueConfig, destination: AbstractDestination, transform: Transform):
        self.config = config
        self.destination = destination
        self.transform = transform

    @abstractmethod
    def run(self, stop_event: Union[multiprocessing.synchronize.Event, threading.Event]) -> PipelineReturnStatus:
        pass
