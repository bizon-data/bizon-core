import os
import tempfile

import pytest
from click.testing import CliRunner

from bizon.cli.main import cli

test_pg_config = f"""
name: test_pipeline

source:
  name: dummy
  stream: creatures
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
    type: postgres
    config:
        database: bizon_test
        schema: public
        syncCursorInDBEvery: 2
        host: {os.environ.get("POSTGRES_HOST", "localhost")}
        port: 5432
        username: postgres
        password: bizon
"""


def test_run_command():
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        # Write config in temp file
        with open(temp.name, "w") as f:
            f.write(test_pg_config)

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(cli, ["run", temp.name])
        assert result.exit_code == 0


def test_run_command_debug():
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        # Write config in temp file
        with open(temp.name, "w") as f:
            f.write(test_pg_config)

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(cli, ["run", temp.name, "--log-level", "DEBUG"])
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
