import json
from datetime import datetime
from typing import Generator, Optional

from loguru import logger
from pytz import UTC
from sqlalchemy import Result, Select, create_engine, inspect, select, update
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from bizon.common.models import CursorStatus, JobStatus

from .config import BackendConfig, BackendTypes
from .models import (
    TABLE_SOURCE_CURSOR,
    TABLE_STREAM_INFO,
    Base,
    StreamCursor,
    StreamJob,
)


class Backend:
    def __init__(self, config: BackendConfig):
        self.config = config
        self.engine: Engine = self._get_engine()

    @classmethod
    def from_raw_config(cls, config: dict):
        """Create a Backend instance from a raw config dict"""
        backend_config = BackendConfig.model_validate(config)
        return cls(config=backend_config)

    @property
    def session(self) -> Generator[Session, None, None]:
        """yields a SQLAlchemy connection"""
        session_ = scoped_session(
            sessionmaker(
                bind=self.engine,
                expire_on_commit=False,
            )
        )

        yield session_

        session_.close()

    def _get_engine(self) -> Engine:
        if self.config.type == BackendTypes.SQLITE:
            return create_engine(
                f"{self.config.type.value}://",
                echo=self.config.echoEngine,
                connect_args={"check_same_thread": False},
            )

        if self.config.type == BackendTypes.BIGQUERY:
            return create_engine(
                f"{self.config.type.value}://{self.config.database}/{self.config.schema}", echo=self.config.echoEngine
            )

        raise Exception(f"Unsupported database type {self.config.type}")

    def _check_schema_exist(self):
        if self.config.type == BackendTypes.SQLITE:
            logger.error("SQLite does not support schemas")
            raise Exception("SQLite does not support schemas")

        with self.engine.connect() as connection:
            if not inspect(connection).has_schema(self.config.schema):
                logger.error(
                    f"Schema or dataset {self.config.schema} does not exist in the database, you need to create it first."
                )
                raise Exception(
                    f"Schema or dataset {self.config.schema} does not exist in the database, you need to create it first."
                )

    def _create_all_tables(self):
        Base.metadata.create_all(self.engine)

    def _drop_all_tables(self):
        Base.metadata.drop_all(self.engine)

    def check_prerequisites(self) -> bool:
        """Check if the database contains the necessary tables, return True if entities are present
        Return False if entities are not present, they will be created
        """

        # Check if schema exists
        if self.config.type != BackendTypes.SQLITE:
            self._check_schema_exist()

        all_entities_exist = True

        # Check if TABLE_STREAM_INFO exists, otherwise create it
        if not inspect(self.engine).has_table(TABLE_STREAM_INFO):
            all_entities_exist = False
            logger.info(f"Table {TABLE_STREAM_INFO} does not exist in the database, we will create it")

        if not inspect(self.engine).has_table(TABLE_SOURCE_CURSOR):
            all_entities_exist = False
            logger.info(f"Table {TABLE_SOURCE_CURSOR} does not exist in the database, we will create it")

        return all_entities_exist

    def _add_and_commit(self, obj, session: Optional[Session] = None):
        """Add the object to the session and commit it, return its ID"""
        session = session or next(self.session)
        session.add(obj)
        session.commit()
        return obj

    def _execute(self, select: Select, session: Optional[Session] = None) -> Result:
        session = session or next(self.session)
        return session.execute(select)

    def create_stream_job(
        self,
        source_name: str,
        stream_name: str,
        total_records_to_fetch: Optional[int] = None,
        job_status: JobStatus = JobStatus.NOT_STARTED,
        session: Optional[Session] = None,
    ) -> StreamJob:
        """Create new StreamJob record in dbt and return its ID"""

        new_stream_job = StreamJob(
            source_name=source_name,
            stream_name=stream_name,
            total_records_to_fetch=total_records_to_fetch,
            status=job_status,
        )
        new_stream_job = self._add_and_commit(new_stream_job, session=session)
        logger.debug(f"New streamJob has been created: {new_stream_job}")
        return new_stream_job

    def update_stream_job_status(
        self, job_id: str, job_status: JobStatus, error_message: Optional[str] = None, session: Optional[Session] = None
    ):
        """Update the status of the stream job with the given id"""
        stmt = (
            update(StreamJob)
            .where(StreamJob.id == job_id)
            .values(status=job_status, error_message=error_message, updated_at=datetime.now(tz=UTC))
            .execution_options(synchronize_session="fetch")
        )
        self._execute(stmt, session=session)

    def get_stream_job_by_id(self, job_id: str, session: Optional[Session] = None) -> Optional[StreamJob]:
        """Get the job by its ID"""

        smt = select(StreamJob).filter(
            StreamJob.id == job_id,
        )

        results = self._execute(smt, session=session).one_or_none()

        if results:
            return results[0]
        logger.warning(f"No job found for id={job_id}")
        return None

    def get_running_stream_job(
        self, source_name: str, stream_name: str, session: Optional[Session] = None
    ) -> Optional[StreamJob]:
        """Get the job id for the given source and stream name"""

        query = select(StreamJob).filter(
            StreamJob.source_name == source_name,
            StreamJob.stream_name == stream_name,
            StreamJob.status == JobStatus.RUNNING,
        )

        job = self._execute(query, session=session).scalar_one_or_none()

        if job:
            return job

        logger.debug(f"No running job found for source={source_name} stream={stream_name}")
        return None

    def create_stream_cursor(
        self,
        job_id: str,
        source_name: str,
        stream_name: str,
        iteration: int,
        rows_fetched: int,
        next_pagination: dict,
        cursor_status: CursorStatus = CursorStatus.NOT_STARTED,
        error_message: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> str:
        """Create a new StreamCursor record in db and return its ID"""
        new_stream_cursor = StreamCursor(
            job_id=job_id,
            source_name=source_name,
            stream_name=stream_name,
            iteration=iteration,
            rows_fetched=rows_fetched,
            next_pagination=json.dumps(next_pagination),
            status=cursor_status,
            error_message=error_message,
        )
        new_stream_cursor_id = self._add_and_commit(new_stream_cursor, session=session).id
        logger.debug(f"New streamCursor has been created with id={new_stream_cursor_id}")
        return new_stream_cursor_id

    def update_stream_cursor_status(
        self,
        cursor_id: str,
        cursor_status: CursorStatus,
        error_message: Optional[str] = None,
        session: Optional[Session] = None,
    ):
        """Update the status of the stream cursor with the given id"""
        stmt = (
            update(StreamCursor)
            .where(StreamCursor.id == cursor_id)
            .values(status=cursor_status, error_message=error_message, updated_at=datetime.now(tz=UTC))
            .execution_options(synchronize_session="fetch")
        )
        self._execute(stmt, session=session)

    def get_cursor_by_id(self, cursor_id: str, session: Optional[Session] = None) -> Optional[StreamCursor]:
        """Get the cursor by its ID"""

        smt = select(StreamCursor).filter(
            StreamCursor.id == cursor_id,
        )

        results = self._execute(smt, session=session).one_or_none()
        if results:
            return results[0]
        logger.warning(f"No cursor found for id={cursor_id}")
        return None

    def get_last_cursor_by_job_id(self, job_id: str, session: Optional[Session] = None) -> Optional[StreamCursor]:
        """Get the last cursor for the given job id"""

        smt = (
            select(StreamCursor)
            .filter(
                StreamCursor.job_id == job_id,
            )
            .order_by(StreamCursor.created_at.desc())
        )

        results = self._execute(smt, session=session).first()
        if results:
            return results[0]

        logger.warning(f"No last cursor found for job_id={job_id}")
        return None
