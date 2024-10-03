from datetime import datetime
from enum import Enum
from uuid import uuid4

from pytz import UTC
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, relationship

TABLE_STREAM_INFO = "stream_jobs"
TABLE_SOURCE_CURSOR = "source_cursors"
TABLE_DESTINATION_CURSOR = "destination_cursors"


def generate_uuid():
    return str(uuid4().hex)


class JobStatus(str, Enum):
    STARTED = "started"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELED = "cancelled"
    SUCCEEDED = "succeeded"


class CursorStatus(str, Enum):
    STARTED = "started"
    PULLING = "pulling"
    PULLED = "pulled"
    WRITTEN_IN_DESTINATION = "written_in_destination"
    FAILED = "failed"


class Base(DeclarativeBase):
    pass


class StreamJob(Base):
    __tablename__ = TABLE_STREAM_INFO

    id = Column(String(100), primary_key=True, default=generate_uuid, doc="Unique identifier for the job")
    source_name = Column(String(100), nullable=False, doc="Name of the source")
    stream_name = Column(String(100), nullable=False, doc="Name of the stream")
    attempt = Column(Integer, default=0, doc="Number of attempts to run the job")
    total_records_to_fetch = Column(
        Integer, nullable=True, default=None, doc="Total number of records present in the source"
    )
    created_at = Column(
        DateTime, nullable=False, default=datetime.now(tz=UTC), doc="Timestamp when the job was created"
    )
    updated_at = Column(DateTime, nullable=True, default=None, doc="Timestamp when the job was last updated")
    status = Column(String(100), default=JobStatus.STARTED, doc="Status of the job")
    error_message = Column(String(255), nullable=True, doc="Error message if the job failed", default=None)

    source_cursor = relationship("SourceCursor", cascade="all, delete")
    destination_cursor = relationship("DestinationCursor", cascade="all, delete")

    def __repr__(self):
        return f"<Job {self.id} {self.source_name} {self.stream_name} {self.status}>"


class SourceCursor(Base):
    __tablename__ = TABLE_SOURCE_CURSOR

    id = Column(String(100), primary_key=True, default=generate_uuid, doc="Unique identifier for the cursor")
    job_id = Column(ForeignKey(f"{TABLE_STREAM_INFO}.id"))
    source_name = Column(String(100), nullable=False, doc="Name of the source")
    stream_name = Column(String(100), nullable=False, doc="Name of the stream")
    iteration = Column(Integer, default=0)
    rows_fetched = Column(Integer, default=0)
    next_pagination = Column(String, nullable=True)
    attempt = Column(Integer, default=0, doc="Number of attempts to pull the data for this cursor")
    status = Column(String(100), default=CursorStatus.STARTED, doc="Status of the cursor")
    error_message = Column(
        String(500), nullable=True, doc="Error message if pulling failed for this cursor", default=None
    )
    created_at = Column(DateTime, default=datetime.now(tz=UTC))
    updated_at = Column(DateTime, nullable=True, default=None, doc="Timestamp when the job was last updated")

    def __repr__(self):
        return f"<SourceCursor {self.stream_name} Iteration: {self.iteration}) - fetched: {self.rows_fetched} at {self.created_at}>"


class DestinationCursor(Base):
    __tablename__ = TABLE_DESTINATION_CURSOR

    id = Column(String(100), primary_key=True, default=generate_uuid, doc="Unique identifier for the cursor")
    job_id = Column(ForeignKey(f"{TABLE_STREAM_INFO}.id"))
    source_name = Column(String(100), nullable=False, doc="Name of the source")
    stream_name = Column(String(100), nullable=False, doc="Name of the stream")
    destination_name = Column(String(100), nullable=False, doc="Name of the destination")
    from_source_iteration = Column(Integer, default=0)
    to_source_iteration = Column(Integer, default=0)
    rows_written = Column(Integer, default=0)
    success = Column(Boolean, nullable=False, doc="Whether the write operation was successful")
    attempt = Column(Integer, default=0, doc="Number of attempts to pull the data for this cursor")
    error_message = Column(
        String(500), nullable=True, doc="Error message if pulling failed for this cursor", default=None
    )
    created_at = Column(DateTime, default=datetime.now(tz=UTC))
    updated_at = Column(DateTime, nullable=True, default=None, doc="Timestamp when the job was last updated")
    pagination = Column(
        String, nullable=True, default=None, doc="Pagination source information from latest written buffer"
    )
