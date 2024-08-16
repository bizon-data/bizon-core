from abc import ABC

from pydantic import BaseModel


class AbastractQueueConfigDetails(BaseModel, ABC):
    queue: BaseModel
    consumer: BaseModel
