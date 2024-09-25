from yaml import safe_load

from bizon.engine.engine import RunnerFactory

config_yaml = """
source:
  source_name: dummy
  stream_name: creatures
  force_ignore_checkpoint: true
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
    type: bigquery
    config:
      database: <MY_PROJECT_ID>
      schema: bizon_test
      syncCursorInDBEvery: 2
"""
config = safe_load(config_yaml)
runner = RunnerFactory.create_from_config_dict(config=config)
runner.run()
