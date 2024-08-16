from typing import List

from bizon.pipeline.queue import AbstractQueue, QueueMessage


class KafkaQueue(AbstractQueue):
    def __init__(self) -> None:
        super().__init__()

    def put(self, records: List[dict], topic: str, index: int, signal: str = None):
        return super().put(records, topic, index, signal)

    def get(self, topic: str) -> QueueMessage:
        return super().get(topic)

    def terminate(self):
        return super().terminate()
