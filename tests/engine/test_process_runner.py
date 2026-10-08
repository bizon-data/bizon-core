import concurrent.futures
import textwrap

import yaml

from bizon.engine.engine import RunnerFactory
from bizon.engine.pipeline.models import PipelineReturnStatus
from bizon.engine.runner.config import RunnerStatus

SOURCE = """
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource


class PagesConfig(SourceConfig):
    behaviour: str = "finite"


class PagesSource(AbstractSource):
    @staticmethod
    def streams():
        return ["pages"]

    @staticmethod
    def get_config_class():
        return PagesConfig

    def get_authenticator(self):
        return None

    def check_connection(self):
        return True, None

    def get_total_records_count(self):
        return None

    def get(self, pagination=None):
        page = (pagination or {}).get("page", 0)
        if self.config.behaviour == "fail" and page == 2:
            raise RuntimeError("page 2 is gone")
        endless = self.config.behaviour in ("endless", "fail")
        next_pagination = {"page": page + 1} if endless or page < 3 else {}
        return SourceIteration(records=[SourceRecord(id=f"{page}", data={"page": page})], next_pagination=next_pagination)
"""


def run_pipeline(tmp_path, behaviour: str, transforms=None, max_workers: int = 2) -> RunnerStatus:
    (tmp_path / "source.py").write_text(SOURCE)
    config = yaml.safe_load(
        textwrap.dedent(
            f"""
            name: process runner test
            source:
              source_file_path: {tmp_path / "source.py"}
              name: pages
              stream: pages
              behaviour: {behaviour}
            destination:
              name: logger
              config: {{}}
            engine:
              backend:
                type: sqlite
                config:
                  database: {tmp_path / "state"}
                  schema: main
              runner:
                type: process
                config:
                  consumer_start_delay: 0
                  max_workers: {max_workers}
                  is_alive_check_interval: 1
            """
        )
    )
    if transforms:
        config["transforms"] = transforms
    runner = RunnerFactory.create_from_config_dict(config)

    # A runner that fails to stop one side hangs instead of returning: fail the test instead.
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(runner.run).result(timeout=120)


def test_process_runner_runs_a_pipeline(tmp_path):
    status = run_pipeline(tmp_path, "finite")

    assert isinstance(status, RunnerStatus)
    assert status.is_success
    assert status.job_id


def test_source_error_ends_the_run(tmp_path):
    status = run_pipeline(tmp_path, "fail")

    assert not status.is_success
    assert status.producer == PipelineReturnStatus.SOURCE_ERROR


def test_consumer_failure_stops_an_endless_producer(tmp_path):
    status = run_pipeline(tmp_path, "endless", transforms=[{"label": "boom", "python": "raise ValueError('boom')"}])

    assert not status.is_success
    assert status.consumer == PipelineReturnStatus.TRANSFORM_ERROR
    assert status.producer == PipelineReturnStatus.KILLED_BY_RUNNER


def test_a_single_worker_does_not_deadlock(tmp_path):
    status = run_pipeline(tmp_path, "finite", max_workers=1)

    assert status.is_success
