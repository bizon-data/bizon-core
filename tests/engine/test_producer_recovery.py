from tempfile import NamedTemporaryFile
from uuid import uuid4

import pytest
import yaml

from bizon.common.models import SyncMetadata
from bizon.destinations.file.src.config import FileDestinationDetailsConfig
from bizon.destinations.file.src.destination import FileDestination
from bizon.destinations.models import DestinationRecord
from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.models import JobStatus
from bizon.engine.engine import RunnerFactory
from bizon.engine.runner.adapters.thread import ThreadRunner
from bizon.engine.runner.runner import AbstractRunner

temporary_file = NamedTemporaryFile()

BIZON_CONFIG_DUMMY_TO_FILE = f"""
source:
  source_name: dummy
  stream_name: creatures
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
def file_destination(my_backend: SQLAlchemyBackend, db_session):

    my_backend.create_all_tables()

    job = my_backend.create_stream_job(
        source_name="dummy",
        stream_name="test",
        job_status=JobStatus.STARTED,
        session=db_session,
    )

    return FileDestination(
        sync_metadata=SyncMetadata(
            job_id=job.id,
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
        backend=my_backend,
    )


@pytest.fixture(scope="function")
def my_runner(my_backend) -> ThreadRunner:
    runner = RunnerFactory.create_from_config_dict(yaml.safe_load(BIZON_CONFIG_DUMMY_TO_FILE))
    runner.backend = my_backend
    return runner


def test_e2e_dummy_to_file_recovery(file_destination, my_backend, db_session):

    N_ITERATION = 2
    CURSOR = str(uuid4())

    # Simulate source records sent to logger
    file_destination.buffer.buffer_size = 0  # Deactivate buffer to write to destination
    file_destination.write_or_buffer_records(
        destination_records=[
            DestinationRecord(
                bizon_id="fjdjfkdfj",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="3",
                source_data={"name": "John", "age": 14},
            ),
            DestinationRecord(
                bizon_id="fjdj9787j",
                bizon_extracted_at="2021-07-01T00:00:00Z",
                bizon_loaded_at="2021-07-01T00:00:00Z",
                source_record_id="6",
                source_data={"name": "David", "age": 10},
            ),
        ],
        iteration=N_ITERATION,
        session=db_session,
        pagination={"cursor": CURSOR},  # Nth iteration to write to destination from dummy source
    )
    runner = RunnerFactory.create_from_config_dict(yaml.safe_load(BIZON_CONFIG_DUMMY_TO_FILE))

    bizon_config = runner.bizon_config
    config = runner.config
    kwargs = runner.get_kwargs()
    source = AbstractRunner.get_source(bizon_config=bizon_config, config=config)
    queue = AbstractRunner.get_queue(bizon_config=bizon_config, **kwargs)

    producer = AbstractRunner.get_producer(
        source=source,
        queue=queue,
        backend=my_backend,
    )

    cursor = producer.get_or_create_cursor(job_id=file_destination.sync_metadata.job_id, session=db_session)

    assert cursor is not None
    assert cursor.source_name == "dummy"
    assert cursor.pagination == {"cursor": CURSOR}
    assert cursor.iteration == N_ITERATION + 1  # Should always be last iteration written to destination + 1
