from yaml import safe_load

from bizon.engine.engine import RunnerFactory

config_yaml = """
name: test_job

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

transforms:
  - label: failure_transform
    python: |
      data['cookies'] = data['key_that_does_not_exist'].upper()
"""

config = safe_load(config_yaml)
runner = RunnerFactory.create_from_config_dict(config=config)
runner.run()
