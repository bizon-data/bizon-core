from abc import ABC, abstractmethod
from typing import List, Optional

from loguru import logger
from pydantic import BaseModel

QUEUE_TERMINATION = "TERMINATION"


class QueueMessage(BaseModel):
    index: int
    records: List[dict]
    signal: Optional[str] = None


class AbstractQueue(ABC):

    @abstractmethod
    def put(self, records: List[dict], index: int, signal: str = None):
        pass

    @abstractmethod
    def get(self) -> QueueMessage:
        pass

    @abstractmethod
    def terminate(self):
        pass

    def send_termination_signal(self, index: int = 0) -> bool:
        self.put(records=[], index=index, signal=QUEUE_TERMINATION)
        logger.info("Sent termination signal to destination.")
        return True
