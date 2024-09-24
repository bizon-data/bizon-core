import os

import pytest
from click.testing import CliRunner

from bizon.cli.main import cli


def test_run_command():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["run", os.path.abspath("tests/cli/test_pg_config.yml")])
    assert result.exit_code == 0


@pytest.mark.skipif(
    os.getenv("POETRY_ENV_TEST") == "CI",
    reason="Pytest runs test in parallel so failing",
)
def test_run_command_debug():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["run", os.path.abspath("tests/cli/test_pg_config.yml"), "--debug"])
    assert os.getenv("LOGURU_LEVEL") == "DEBUG"
    assert result.exit_code == 0


def test_source_list_command():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["source", "list"])
    assert "Available sources:" in result.output
    assert "dummy" in result.output
    assert result.exit_code == 0


def test_stream_list_command():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["stream", "list", "dummy"])
    assert "Available streams for dummy:" in result.output
    assert "[Full refresh only] - plants" in result.output
    assert result.exit_code == 0
