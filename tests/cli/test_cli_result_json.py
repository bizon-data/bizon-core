import json
import textwrap

import pytest
from click.testing import CliRunner

from bizon.cli.main import cli
from bizon.cli.result import RunResultRecorder

SOURCE = """
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource


class FlakyConfig(SourceConfig):
    fail_on: str = "nothing"


class FlakySource(AbstractSource):
    @staticmethod
    def streams():
        return ["items"]

    @staticmethod
    def get_config_class():
        return FlakyConfig

    def get_authenticator(self):
        return None

    def check_connection(self):
        if self.config.fail_on == "connection":
            return False, "vendor is down"
        return True, None

    def get_total_records_count(self):
        return None

    def get(self, pagination=None):
        if self.config.fail_on == "get":
            raise RuntimeError("page 2 came back empty")
        return SourceIteration(records=[SourceRecord(id="1", data={"a": 1})], next_pagination={})
"""


@pytest.fixture
def pipeline(tmp_path):
    (tmp_path / "source.py").write_text(SOURCE)

    def write(fail_on: str = "nothing", extra: str = "") -> tuple:
        config = tmp_path / "config.yml"
        config.write_text(
            textwrap.dedent(
                f"""
                name: result json test
                source:
                  source_file_path: {tmp_path / "source.py"}
                  name: flaky
                  stream: items
                  fail_on: {fail_on}
                destination:
                  name: logger
                  config: {{}}
                engine:
                  backend:
                    type: sqlite
                    config:
                      database: {tmp_path / "state.db"}
                      schema: main
                """
            )
            + extra
        )
        return str(config), str(tmp_path / "result.json")

    return write


def run(config: str, result: str):
    outcome = CliRunner().invoke(cli, ["run", config, "--result-json", result])
    with open(result) as f:
        return outcome, json.load(f)


def test_success(pipeline):
    outcome, result = run(*pipeline())

    assert outcome.exit_code == 0, outcome.output
    assert result["status"] == "success"
    assert result["failure_class"] is None
    assert result["job_id"]
    assert result["records_written"] == 1
    assert result["duration_s"] >= 0


def test_source_error_mid_run(pipeline):
    outcome, result = run(*pipeline(fail_on="get"))

    assert outcome.exit_code != 0
    assert result["status"] == "failure"
    assert result["failure_class"] == "source"
    assert result["producer"] == "source_error"
    assert result["error"] == "RuntimeError: page 2 came back empty"


def test_failed_connection_check(pipeline):
    outcome, result = run(*pipeline(fail_on="connection"))

    assert outcome.exit_code != 0
    assert result["failure_class"] == "source"
    assert "vendor is down" in result["error"]


def test_config_error(pipeline):
    outcome, result = run(*pipeline(extra="unknown_top_level_key: 1\n"))

    assert outcome.exit_code != 0
    assert result["failure_class"] == "config"
    assert result["job_id"] is None


def test_file_reads_running_until_the_run_ends(tmp_path):
    path = tmp_path / "result.json"
    path.write_text('{"status": "success"}')

    RunResultRecorder(str(path))

    assert json.loads(path.read_text())["status"] == "running"


def test_no_file_without_the_flag(pipeline, tmp_path):
    config, result = pipeline()

    outcome = CliRunner().invoke(cli, ["run", config])

    assert outcome.exit_code == 0, outcome.output
    assert not (tmp_path / "result.json").exists()
