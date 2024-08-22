import json
from datetime import datetime
from typing import Generator, Optional, Union

from loguru import logger
from pytz import UTC
from sqlalchemy import Result, Select, create_engine, inspect, select, update
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from bizon.engine.backend.backend import AbstractBackend
from bizon.engine.backend.config import BackendTypes
from bizon.engine.backend.models import (
    TABLE_DESTINATION_CURSOR,
    TABLE_SOURCE_CURSOR,
    TABLE_STREAM_INFO,
    Base,
    CursorStatus,
    DestinationCursor,
    JobStatus,
    SourceCursor,
    StreamJob,
)

from .config import BigQueryConfigDetails, PostgresConfigDetails, SQLiteConfigDetails


class SQLAlchemyBackend(AbstractBackend):

    def __init__(self, config: Union[PostgresConfigDetails, SQLiteConfigDetails], type: BackendTypes):
        super().__init__(config, type)
        self.engine: Engine = self._get_engine()

        self.config: Union[
            PostgresConfigDetails,
            SQLiteConfigDetails,
            BigQueryConfigDetails,
        ] = config

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

    def _get_engine_bigquery(self) -> Engine:
        if hasattr(self.config, "service_account_key") and self.config.service_account_key:
            return create_engine(
                f"bigquery://{self.config.database}/{self.config.schema}",
                echo=self.config.echoEngine,
                credentials_info=self.config.service_account_key,
            )
        return create_engine(f"bigquery://{self.config.database}/{self.config.schema}", echo=self.config.echoEngine)

    def _get_engine_postgres(self) -> Engine:
        return create_engine(
            f"postgresql+psycopg2://{self.config.username}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}",
            echo=self.config.echoEngine,
        )

    def _get_engine(self) -> Engine:

        if self.type == BackendTypes.BIGQUERY:
            return self._get_engine_bigquery()

        # Postgres
        if self.type == BackendTypes.POSTGRES:
            return self._get_engine_postgres()

        # SQLite in a file
        if self.type == BackendTypes.SQLITE:
            return create_engine(
                f"sqlite:///{self.config.database}.db",
                echo=self.config.echoEngine,
            )

        # SQLite in memory
        if self.type == BackendTypes.SQLITE_IN_MEMORY:
            return create_engine(
                f"sqlite:///:memory:",
                echo=self.config.echoEngine,
                connect_args={"check_same_thread": False},
            )

        raise Exception(f"Unsupported database type {self.type}")

    def _check_schema_exist(self):
        if self.type in [BackendTypes.SQLITE, BackendTypes.SQLITE_IN_MEMORY]:
            logger.warning("SQLite does not support schemas")
            return True

        with self.engine.connect() as connection:
            if not inspect(connection).has_schema(self.config.schema):
                logger.error(
                    f"Schema or dataset {self.config.schema} does not exist in the database, you need to create it first."
                )
                raise Exception(
                    f"Schema or dataset {self.config.schema} does not exist in the database, you need to create it first."
                )

    #### INIT DATABASE ####

    def create_all_tables(self):
        Base.metadata.create_all(self.engine)

    def drop_all_tables(self):
        Base.metadata.drop_all(self.engine)

    def check_prerequisites(self) -> bool:
        """Check if the database contains the necessary tables, return True if entities are present
        Return False if entities are not present, they will be created
        """

        # Check if schema exists
        if self.type != BackendTypes.SQLITE:
            self._check_schema_exist()

        all_entities_exist = True

        # Check if TABLE_STREAM_INFO exists, otherwise create it
        if not inspect(self.engine).has_table(TABLE_STREAM_INFO):
            all_entities_exist = False
            logger.info(f"Table {TABLE_STREAM_INFO} does not exist in the database, we will create it")

        if not inspect(self.engine).has_table(TABLE_SOURCE_CURSOR):
            all_entities_exist = False
            logger.info(f"Table {TABLE_SOURCE_CURSOR} does not exist in the database, we will create it")

        if not inspect(self.engine).has_table(TABLE_DESTINATION_CURSOR):
            all_entities_exist = False
            logger.info(f"Table {TABLE_DESTINATION_CURSOR} does not exist in the database, we will create it")

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

    #### STREAM JOB ####

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

    #### SOURCE CURSOR ####

    def create_source_cursor(
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
        """Create a new SourceCursor record in db and return its ID"""
        new_source_cursor = SourceCursor(
            job_id=job_id,
            source_name=source_name,
            stream_name=stream_name,
            iteration=iteration,
            rows_fetched=rows_fetched,
            next_pagination=json.dumps(next_pagination),
            status=cursor_status,
            error_message=error_message,
        )
        new_source_cursor_id = self._add_and_commit(new_source_cursor, session=session).id
        logger.debug(f"New streamCursor has been created with id={new_source_cursor_id}")
        return new_source_cursor_id

    def update_source_cursor_status(
        self,
        cursor_id: str,
        cursor_status: CursorStatus,
        error_message: Optional[str] = None,
        session: Optional[Session] = None,
    ):
        """Update the status of the stream cursor with the given id"""
        stmt = (
            update(SourceCursor)
            .where(SourceCursor.id == cursor_id)
            .values(status=cursor_status, error_message=error_message, updated_at=datetime.now(tz=UTC))
            .execution_options(synchronize_session="fetch")
        )
        self._execute(stmt, session=session)

    def get_source_cursor_by_id(self, cursor_id: str, session: Optional[Session] = None) -> Optional[SourceCursor]:
        """Get the cursor by its ID"""

        smt = select(SourceCursor).filter(
            SourceCursor.id == cursor_id,
        )

        results = self._execute(smt, session=session).one_or_none()
        if results:
            return results[0]
        logger.warning(f"No cursor found for id={cursor_id}")
        return None

    def get_last_source_cursor_by_job_id(
        self, job_id: str, session: Optional[Session] = None
    ) -> Optional[SourceCursor]:
        """Get the last cursor we should start source from for the given job id
        The cursor should have status CursorStatus.WRITTEN_IN_DESTINATION
        """

        smt = (
            select(SourceCursor)
            .filter(
                SourceCursor.job_id == job_id,
                SourceCursor.status == CursorStatus.WRITTEN_IN_DESTINATION,
            )
            .order_by(SourceCursor.iteration.desc())
        )

        results = self._execute(smt, session=session).first()
        if results:
            return results[0]

        logger.warning(f"No last cursor found for job_id={job_id}")
        return None

    def get_last_source_cursor_by_stream_and_iteration(
        self, source_name: str, stream_name: str, iteration: int, session: Optional[Session] = None
    ) -> Optional[SourceCursor]:
        """Get the last cursor for the given stream and iteration"""

        smt = (
            select(SourceCursor)
            .filter(
                SourceCursor.source_name == source_name,
                SourceCursor.stream_name == stream_name,
                SourceCursor.iteration == iteration,
            )
            .order_by(SourceCursor.created_at.desc())
        )

        results = self._execute(smt, session=session).first()
        if results:
            return results[0]

        logger.warning(f"No last cursor found for source={source_name} stream={stream_name} iteration={iteration}")
        return None

    ### DESTINATION CURSOR ###

    def create_destination_cursor(
        self,
        job_id: str,
        source_name: str,
        stream_name: str,
        destination_name: str,
        from_source_iteration: int,
        to_source_iteration: int,
        rows_written: int,
        session: Session | None = None,
    ) -> DestinationCursor:

        destination_cursor = DestinationCursor(
            source_name=source_name,
            stream_name=stream_name,
            destination_name=destination_name,
            job_id=job_id,
            from_source_iteration=from_source_iteration,
            to_source_iteration=to_source_iteration,
            rows_written=rows_written,
        )
        new_destination_cursor = self._add_and_commit(destination_cursor, session=session)
        logger.debug(f"New Destination Cursor has been created: {new_destination_cursor}")
        return new_destination_cursor

    def update_destination_cursor(
        self,
        cursor_id: str,
        rows_written: int,
        status: CursorStatus,
        error_message: Optional[str] = None,
        session: Session | None = None,
    ):
        """Update the status of the stream job with the given id"""
        stmt = (
            update(DestinationCursor)
            .where(DestinationCursor.id == cursor_id)
            .values(
                status=status, error_message=error_message, rows_written=rows_written, updated_at=datetime.now(tz=UTC)
            )
            .execution_options(synchronize_session="fetch")
        )
        self._execute(stmt, session=session)

    def get_destination_cursor_by_id(
        self, cursor_id: str, session: Optional[Session] = None
    ) -> Optional[DestinationCursor]:
        """Get the destination cursor by its ID"""
        smt = select(DestinationCursor).filter(
            DestinationCursor.id == cursor_id,
        )
        results = self._execute(smt, session=session).one_or_none()

        if results:
            return results[0]
        logger.warning(f"No job found for id={cursor_id}")
        return None
