from enum import Enum

from pydantic import BaseModel


class JobStatus(str, Enum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class CursorStatus(str, Enum):
    NOT_STARTED = "not_started"
    SUCCESS = "success"
    FAILED = "failed"


class BizonRecord(BaseModel):
    _bizon_id: str
    _bizon_loaded_at: str
    _bizon_data: str
