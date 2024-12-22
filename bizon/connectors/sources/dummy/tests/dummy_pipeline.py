from yaml import safe_load

from bizon.engine.engine import RunnerFactory

config_yaml = """
name: dummy to logger

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
"""

config = safe_load(config_yaml)
runner = RunnerFactory.create_from_config_dict(config=config)
runner.run()
