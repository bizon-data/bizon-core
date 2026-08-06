"""Tests for stream reset resolution in the runner.

`init_job` is the single place where "is this run a reset?" is decided: it runs in the parent before
the producer and consumer are submitted, and mutates the config both of them are handed.
"""

import pytest
import yaml

from bizon.engine.backend.adapters.sqlalchemy.backend import SQLAlchemyBackend
from bizon.engine.backend.models import JobStatus
from bizon.engine.engine import RunnerFactory
from bizon.engine.runner.runner import AbstractRunner
from bizon.source.config import SourceSyncModes

BIZON_CONFIG_DUMMY_INCREMENTAL = """
name: test_reset_job

source:
  name: dummy
  stream: creatures
  sync_mode: incremental
  cursor_field: updated_at
  authentication:
    type: api_key
    params:
      token: dummy_key

destination:
  name: logger
  config:
    dummy: dummy

engine:
  backend:
    type: sqlite_in_memory
    config:
      database: not_used
      schema: not_used
      syncCursorInDBEvery: 400
  runner:
    log_level: INFO
"""


def build_runner(**source_overrides):
    config = yaml.safe_load(BIZON_CONFIG_DUMMY_INCREMENTAL)
    config["source"].update(source_overrides)
    return RunnerFactory.create_from_config_dict(config)


@pytest.fixture(scope="function")
def backend(my_sqlite_backend: SQLAlchemyBackend) -> SQLAlchemyBackend:
    my_sqlite_backend.create_all_tables()
    return my_sqlite_backend


def resolve(runner, backend: SQLAlchemyBackend, resuming_reset: bool = False) -> bool:
    return AbstractRunner.resolve_reset(
        bizon_config=runner.bizon_config, backend=backend, resuming_reset=resuming_reset
    )


def request_reset(runner, backend: SQLAlchemyBackend):
    return backend.create_stream_reset(
        name=runner.bizon_config.name,
        source_name=runner.bizon_config.source.name,
        stream_name=runner.bizon_config.source.stream,
    )


class TestResolveReset:
    def test_plain_incremental_run_is_not_a_reset(self, backend):
        assert resolve(build_runner(), backend) is False

    def test_reset_flag_is_honoured(self, backend):
        assert resolve(build_runner(reset=True), backend) is True

    def test_pending_request_is_picked_up_without_the_flag(self, backend):
        """This is what makes `bizon stream reset` work for a scheduled `bizon run config.yml`."""
        runner = build_runner()
        request_reset(runner, backend)

        assert resolve(runner, backend) is True

    def test_in_flight_reset_is_resumed(self, backend):
        """A crashed reset must retry as a reset, not degrade into an incremental append."""
        assert resolve(build_runner(), backend, resuming_reset=True) is True

    def test_reset_is_ignored_for_non_incremental_sync_modes(self, backend):
        """There is no incremental state to reset, and full refresh already replaces the table."""
        runner = build_runner(reset=True, sync_mode=SourceSyncModes.FULL_REFRESH.value)

        assert resolve(runner, backend) is False

    def test_pending_request_is_ignored_for_non_incremental_sync_modes(self, backend):
        runner = build_runner(sync_mode=SourceSyncModes.FULL_REFRESH.value)
        request_reset(runner, backend)

        assert resolve(runner, backend) is False


class TestBindStreamResetToJob:
    """Every reset job must end up with a consumed marker row pointing at it."""

    def _job(self, runner, backend, status=JobStatus.RUNNING):
        return backend.create_stream_job(
            name=runner.bizon_config.name,
            source_name=runner.bizon_config.source.name,
            stream_name=runner.bizon_config.source.stream,
            sync_mode=SourceSyncModes.INCREMENTAL.value,
            job_status=status,
        )

    def test_flag_path_creates_and_consumes_a_marker(self, backend):
        """`--reset` has no marker of its own, so one is created to make the run recoverable."""
        runner = build_runner(reset=True)
        job = self._job(runner, backend)

        AbstractRunner.bind_stream_reset_to_job(bizon_config=runner.bizon_config, backend=backend, job_id=job.id)

        assert backend.get_stream_reset_by_job_id(job_id=job.id) is not None

    def test_pending_marker_is_consumed_rather_than_duplicated(self, backend):
        runner = build_runner()
        stream_reset = request_reset(runner, backend)
        job = self._job(runner, backend)

        AbstractRunner.bind_stream_reset_to_job(bizon_config=runner.bizon_config, backend=backend, job_id=job.id)

        bound = backend.get_stream_reset_by_job_id(job_id=job.id)
        assert bound.id == stream_reset.id
        # Consumed, so the next run does not reset all over again.
        assert (
            backend.get_pending_stream_reset(
                name=runner.bizon_config.name,
                source_name=runner.bizon_config.source.name,
                stream_name=runner.bizon_config.source.stream,
            )
            is None
        )

    def test_binding_is_idempotent(self, backend):
        """Re-running against an already-bound job must not consume a second request."""
        runner = build_runner(reset=True)
        job = self._job(runner, backend)

        AbstractRunner.bind_stream_reset_to_job(bizon_config=runner.bizon_config, backend=backend, job_id=job.id)
        first = backend.get_stream_reset_by_job_id(job_id=job.id)

        request_reset(runner, backend)
        AbstractRunner.bind_stream_reset_to_job(bizon_config=runner.bizon_config, backend=backend, job_id=job.id)

        assert backend.get_stream_reset_by_job_id(job_id=job.id).id == first.id


class TestInitJob:
    def test_pending_request_flips_the_config_for_producer_and_consumer(self, backend, monkeypatch):
        """Producer and consumer are handed these objects, so the flag must land on both."""
        runner = build_runner()
        request_reset(runner, backend)
        monkeypatch.setattr(AbstractRunner, "get_backend", staticmethod(lambda **kwargs: backend))

        job = AbstractRunner.init_job(bizon_config=runner.bizon_config, config=runner.config)

        assert runner.bizon_config.source.reset is True
        assert runner.config["source"]["reset"] is True
        assert backend.get_stream_reset_by_job_id(job_id=job.id) is not None
        # The job itself stays incremental so it becomes the next run's watermark.
        assert job.sync_mode == SourceSyncModes.INCREMENTAL.value

    def test_plain_run_leaves_the_flag_off(self, backend, monkeypatch):
        runner = build_runner()
        monkeypatch.setattr(AbstractRunner, "get_backend", staticmethod(lambda **kwargs: backend))

        job = AbstractRunner.init_job(bizon_config=runner.bizon_config, config=runner.config)

        assert runner.bizon_config.source.reset is False
        assert backend.get_stream_reset_by_job_id(job_id=job.id) is None

    def test_reset_starts_a_fresh_job_instead_of_resuming(self, backend, monkeypatch):
        """A reset re-fetches from iteration 0, so it must not adopt a half-finished job."""
        runner = build_runner()
        running_job = backend.create_stream_job(
            name=runner.bizon_config.name,
            source_name=runner.bizon_config.source.name,
            stream_name=runner.bizon_config.source.stream,
            sync_mode=SourceSyncModes.INCREMENTAL.value,
            job_status=JobStatus.RUNNING,
        )
        request_reset(runner, backend)
        monkeypatch.setattr(AbstractRunner, "get_backend", staticmethod(lambda **kwargs: backend))

        job = AbstractRunner.init_job(bizon_config=runner.bizon_config, config=runner.config)

        assert job.id != running_job.id
        assert backend.get_stream_job_by_id(job_id=running_job.id).status == JobStatus.CANCELED

    def test_crashed_reset_resumes_the_same_job(self, backend, monkeypatch):
        """The retry has no flag and no pending marker: the bound job is what keeps it a reset."""
        runner = build_runner()
        running_job = backend.create_stream_job(
            name=runner.bizon_config.name,
            source_name=runner.bizon_config.source.name,
            stream_name=runner.bizon_config.source.stream,
            sync_mode=SourceSyncModes.INCREMENTAL.value,
            job_status=JobStatus.RUNNING,
        )
        AbstractRunner.bind_stream_reset_to_job(
            bizon_config=runner.bizon_config, backend=backend, job_id=running_job.id
        )
        monkeypatch.setattr(AbstractRunner, "get_backend", staticmethod(lambda **kwargs: backend))

        job = AbstractRunner.init_job(bizon_config=runner.bizon_config, config=runner.config)

        assert job.id == running_job.id
        assert runner.bizon_config.source.reset is True
