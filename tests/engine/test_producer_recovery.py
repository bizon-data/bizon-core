from datetime import datetime
from tempfile import NamedTemporaryFile
from uuid import uuid4

import polars as pl
import pytest
import yaml

from bizon.common.models import SyncMetadata
from bizon.connectors.destinations.file.src.config import FileDestinationDetailsConfig
from bizon.connectors.destinations.file.src.destination import FileDestination
from bizon.destination.models import destination_record_schema
from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.models import JobStatus
from bizon.engine.engine import RunnerFactory
from bizon.engine.runner.adapters.thread import ThreadRunner
from bizon.engine.runner.runner import AbstractRunner

temporary_file = NamedTemporaryFile()

BIZON_CONFIG_DUMMY_TO_FILE = f"""
name: test_job

source:
  name: dummy
  stream: creatures
  authentication:
    type: api_key
    params:
      token: dummy_key

destination:
  name: file
  config:
    filepath: {temporary_file.name}

engine:
  backend:
    type: sqlite
    config:
      syncCursorInDBEvery: 100
      database: bizon
      schema: public

  runner:
    type: thread
"""


@pytest.fixture(scope="function")
def file_destination(my_sqlite_backend: SQLAlchemyBackend, sqlite_db_session):

    my_sqlite_backend.create_all_tables()

    job = my_sqlite_backend.create_stream_job(
        name="job_test",
        source_name="dummy",
        stream_name="test",
        sync_mode="full_refresh",
        job_status=JobStatus.STARTED,
        session=sqlite_db_session,
    )

    return FileDestination(
        sync_metadata=SyncMetadata(
            job_id=job.id,
            name="job_test",
            source_name="dummy",
            stream_name="test",
            destination_name="logger",
            sync_mode="full_refresh",
        ),
        config=FileDestinationDetailsConfig(
            filepath=temporary_file.name,
            buffer_size=0,
            buffer_flush_timeout=0,
        ),
        backend=my_sqlite_backend,
    )


@pytest.fixture(scope="function")
def my_runner(my_backend) -> ThreadRunner:
    runner = RunnerFactory.create_from_config_dict(yaml.safe_load(BIZON_CONFIG_DUMMY_TO_FILE))
    runner.backend = my_backend
    return runner


# Create DataFrame with schema
df_destination_records = pl.DataFrame(
    {
        "bizon_id": ["id_1", "id_2"],
        "bizon_extracted_at": [datetime(2024, 12, 5, 12, 0), datetime(2024, 12, 5, 13, 0)],
        "bizon_loaded_at": [datetime(2024, 12, 5, 12, 30), datetime(2024, 12, 5, 13, 30)],
        "source_record_id": ["record_1", "record_2"],
        "source_timestamp": [datetime(2024, 12, 5, 11, 30), datetime(2024, 12, 5, 12, 30)],
        "source_data": ["cookies", "cream"],
    },
    schema=destination_record_schema,
)


def test_e2e_dummy_to_file_recovery(file_destination, my_sqlite_backend, sqlite_db_session):

    N_ITERATION = 2
    CURSOR = str(uuid4())

    # Simulate source records sent to logger
    file_destination.buffer.buffer_size = 0  # Deactivate buffer to write to destination
    file_destination.write_or_buffer_records(
        df_destination_records=df_destination_records,
        iteration=N_ITERATION,
        session=sqlite_db_session,
        pagination={"cursor": CURSOR},  # Nth iteration to write to destination from dummy source
    )
    runner = RunnerFactory.create_from_config_dict(yaml.safe_load(BIZON_CONFIG_DUMMY_TO_FILE))

    bizon_config = runner.bizon_config
    config = runner.config
    kwargs = runner.get_kwargs()
    source = AbstractRunner.get_source(bizon_config=bizon_config, config=config)
    queue = AbstractRunner.get_queue(bizon_config=bizon_config, **kwargs)

    producer = AbstractRunner.get_producer(
        bizon_config=bizon_config,
        source=source,
        queue=queue,
        backend=my_sqlite_backend,
    )

    cursor = producer.get_or_create_cursor(job_id=file_destination.sync_metadata.job_id, session=sqlite_db_session)

    assert cursor is not None
    assert cursor.source_name == "dummy"
    assert cursor.pagination == {"cursor": CURSOR}
    assert cursor.iteration == N_ITERATION + 1  # Should always be last iteration written to destination + 1
