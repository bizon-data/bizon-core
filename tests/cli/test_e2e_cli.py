import json
import os

import pytest
from click.testing import CliRunner

from bizon.cli.main import cli

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
    filepath: ./tests/cli/dummy_e2e_test.jsonl

engine:
  backend:
    type: sqlite
    config:
      syncCursorInDBEvery: 100
      database: bizon
      schema: public

  runner:
    type: process
"""


@pytest.mark.skipif(
    os.getenv("POETRY_ENV_TEST") == "CI",
    reason="Skipping tests that require a BigQuery connexion",
)
def test_e2e_run_command_dummy_to_file():

    with open("tests/cli/test_e2e_dummy_to_file.yml", "w") as file:
        file.write(BIZON_CONFIG_DUMMY_TO_FILE)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["run", "tests/cli/test_e2e_dummy_to_file.yml"])
    assert result.exit_code == 0

    records_extracted = {}

    with open("tests/cli/dummy_e2e_test.jsonl", "r") as file:
        for line in file.readlines():
            record: dict = json.loads(line.strip())
            records_extracted[record["source_record_id"]] = record["source_data"]

    assert set(records_extracted.keys()) == set(["9898", "88787", "98", "3333", "56565"])

    os.remove(os.path.abspath("tests/cli/dummy_e2e_test.jsonl"))
    os.remove(os.path.abspath("tests/cli/test_e2e_dummy_to_file.yml"))
