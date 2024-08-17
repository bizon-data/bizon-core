from yaml import safe_load

from bizon.engine.runner import RunnerFactory

config_yaml = """
source:
  source_name: dummy
  stream_name: creatures
  authentication:
    type: api_key
    params:
      token: dummy_key

destination:
  name: bigquery
  config:
    dataset_id: bizon_test
    dataset_location: US
    project_id: <GCP_PROJECT_ID>
    gcs_buffer_bucket: bizon-buffer
    gcs_buffer_format: parquet

engine:
  backend:
    type: sqlite
    database: not_used
    schema: not_used
    syncCursorInDBEvery: 200

  queue:
    type: kafka
    config:
      bootstrap_servers: localhost:9092

  runner:
    type: threads
    log_level: DEBUG
"""
config = safe_load(config_yaml)
runner = RunnerFactory.create_from_config_dict(config=config)
runner.run()
