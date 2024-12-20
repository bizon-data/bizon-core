from yaml import safe_load

from bizon.engine.engine import RunnerFactory

config_yaml = """
source:
  name: dummy
  stream: creatures
  authentication:
    type: api_key
    params:
      token: dummy_key

destination:
  # Authentication: If empty it will be infered.
  # Must have the bigquery.jobUser
  # Must have the bigquery.dataEditor and storage.objectUser on the supplied dataset and bucket
  name: bigquery
  config:
    dataset_id: bizon_test
    dataset_location: US
    project_id: <project_id>
    gcs_buffer_bucket: bizon-buffer
    gcs_buffer_format: parquet
"""
config = safe_load(config_yaml)
runner = RunnerFactory.create_from_config_dict(config=config)
runner.run()
