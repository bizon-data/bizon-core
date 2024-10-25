import json
from tempfile import NamedTemporaryFile

import yaml

from bizon.engine.engine import RunnerFactory

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
      database: bizon_test_e2e
      schema: public
      port: 5432
      username: postgres
      password: bizon

  runner:
    type: process
"""


def test_e2e_dummy_to_file():

    runner = RunnerFactory.create_from_config_dict(yaml.safe_load(BIZON_CONFIG_DUMMY_TO_FILE))
    runner.run()
    records_extracted = {}
    with open(temporary_file.name, "r") as file:
        for line in file.readlines():
            record: dict = json.loads(line.strip())
            records_extracted[record["source_record_id"]] = record["source_data"]

    assert set(records_extracted.keys()) == set(["9898", "88787", "98", "3333", "56565"])
