import os
import time
from datetime import datetime
from typing import Callable, List, Literal, Optional

from loguru import logger
from pydantic import BaseModel
from pytz import UTC
from requests import RequestException
from sqlalchemy.exc import SQLAlchemyError

from bizon.engine.backend.backend import BackendSchemaMissingError
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.runner.config import RunnerStatus

FailureClass = Literal[
    "config", "source", "destination", "backend", "queue", "transform", "stream", "killed", "unknown"
]

_STATUS_FAILURE_CLASSES = {
    PipelineReturnStatus.SOURCE_ERROR: "source",
    PipelineReturnStatus.DESTINATION_ERROR: "destination",
    PipelineReturnStatus.BACKEND_ERROR: "backend",
    PipelineReturnStatus.QUEUE_ERROR: "queue",
    PipelineReturnStatus.TRANSFORM_ERROR: "transform",
    PipelineReturnStatus.STREAM_ERROR: "stream",
}


class RunResult(BaseModel):
    status: Literal["running", "success", "failure"]
    failure_class: Optional[FailureClass] = None
    job_id: Optional[str] = None
    producer: Optional[str] = None
    consumer: Optional[str] = None
    stream: Optional[str] = None
    records_written: Optional[int] = None
    started_at: datetime
    duration_s: Optional[float] = None
    error: Optional[str] = None


def failure_class_from_status(status: RunnerStatus) -> Optional[FailureClass]:
    """The side that failed first. The producer goes first: when it fails the consumer reports the
    producer's failure too, and a side stopped because the other one failed reports KILLED_BY_RUNNER."""
    statuses = [status.producer, status.consumer, status.stream]
    for pipeline_status in statuses:
        if pipeline_status in _STATUS_FAILURE_CLASSES:
            return _STATUS_FAILURE_CLASSES[pipeline_status]
    if PipelineReturnStatus.KILLED_BY_RUNNER in statuses:
        return "killed"
    return None


def failure_class_from_exception(error: Exception) -> FailureClass:
    if isinstance(error, (ConnectionError, RequestException)):
        return "source"
    if isinstance(error, (SQLAlchemyError, BackendSchemaMissingError)):
        return "backend"
    return "unknown"


def _error_line(message: str) -> str:
    # A logged traceback ends with the exception that caused it, which is the useful part.
    lines = [line for line in message.strip().splitlines() if line.strip()]
    if lines and lines[0].startswith("Traceback"):
        return lines[-1].strip()
    return lines[0].strip() if lines else message


class RunResultRecorder:
    """Writes the outcome of `bizon run --result-json`.

    The file says `running` as soon as the run starts, so a process killed from outside (OOM, a
    deadline) leaves that behind rather than the previous run's outcome.
    """

    def __init__(self, path: Optional[str]):
        self.path = path
        self.started_at = datetime.now(tz=UTC)
        self._started = time.monotonic()
        self._errors: List[str] = []
        self._sink_id: Optional[int] = None
        self._write(RunResult(status="running", started_at=self.started_at))

    def capture_errors(self):
        """Collect ERROR logs from here on. Call after the runner is built: it resets loguru's sinks."""
        if self.path:
            self._sink_id = logger.add(lambda message: self._errors.append(message.record["message"]), level="ERROR")

    def failed(self, error: Exception, failure_class: FailureClass):
        self._write(
            RunResult(
                status="failure",
                failure_class=failure_class,
                started_at=self.started_at,
                duration_s=self._duration(),
                error=f"{type(error).__name__}: {error}",
            )
        )

    def finished(self, status: RunnerStatus, records_written: Callable[[str], Optional[int]]):
        if not self.path:
            return

        written = None
        if status.job_id:
            try:
                written = records_written(status.job_id)
            except Exception as error:
                logger.warning(f"Could not count the records written by job {status.job_id}: {error}")

        self._write(
            RunResult(
                status="success" if status.is_success else "failure",
                failure_class=None if status.is_success else failure_class_from_status(status),
                job_id=status.job_id,
                producer=status.producer.value if status.producer else None,
                consumer=status.consumer.value if status.consumer else None,
                stream=status.stream.value if status.stream else None,
                records_written=written,
                started_at=self.started_at,
                duration_s=self._duration(),
                error=None if status.is_success or not self._errors else _error_line(self._errors[0]),
            )
        )

    def _duration(self) -> float:
        return round(time.monotonic() - self._started, 3)

    def _write(self, result: RunResult):
        if not self.path:
            return
        if self._sink_id is not None and result.status != "running":
            logger.remove(self._sink_id)
            self._sink_id = None
        # Write then rename, so a reader never sees a half-written file.
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w") as f:
            f.write(result.model_dump_json(indent=2))
        os.replace(tmp_path, self.path)
