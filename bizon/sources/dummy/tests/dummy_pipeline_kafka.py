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
  name: logger
  config:
    dummy: dummy

engine:
  queue:
    type: kafka
    config:
      queue:
        bootstrap_server: localhost:9092
"""
config = safe_load(config_yaml)
runner = RunnerFactory.create_from_config_dict(config=config)
runner.run()
