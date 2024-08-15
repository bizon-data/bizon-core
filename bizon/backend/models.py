from datetime import datetime
from uuid import uuid4

from pytz import UTC
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase

from bizon.common.models import CursorStatus, JobStatus

TABLE_STREAM_INFO = "stream_jobs"
TABLE_SOURCE_CURSOR = "stream_cursors"


def generate_uuid():
    return str(uuid4().hex)


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
    status = Column(String(20), default=JobStatus.NOT_STARTED, doc="Status of the job")
    error_message = Column(String(255), nullable=True, doc="Error message if the job failed", default=None)

    def __repr__(self):
        return f"<Job {self.id} {self.source_name} {self.stream_name} {self.status}>"


class StreamCursor(Base):
    __tablename__ = TABLE_SOURCE_CURSOR

    id = Column(String(100), primary_key=True, default=generate_uuid, doc="Unique identifier for the cursor")
    job_id = Column(ForeignKey(f"{TABLE_STREAM_INFO}.id"))
    source_name = Column(String(100), nullable=False, doc="Name of the source")
    stream_name = Column(String(100), nullable=False, doc="Name of the stream")
    iteration = Column(Integer, default=0)
    rows_fetched = Column(Integer, default=0)
    next_pagination = Column(String, nullable=True)
    attempt = Column(Integer, default=0, doc="Number of attempts to pull the data for this cursor")
    status = Column(String(20), default=CursorStatus.NOT_STARTED, doc="Status of the cursor")
    error_message = Column(
        String(255), nullable=True, doc="Error message if pulling failed for this cursor", default=None
    )
    created_at = Column(DateTime, default=datetime.now(tz=UTC))
    updated_at = Column(DateTime, nullable=True, default=None, doc="Timestamp when the job was last updated")

    def __repr__(self):
        return f"<StreamCursor {self.stream_name} Iteration: {self.iteration}) - fetched: {self.rows_fetched} at {self.created_at}>"
