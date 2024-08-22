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
    type: rabbitmq
    config:
      queue:
        host: localhost
        queue_name: bizon

  backend:
    type: postgres
    config:
      database: bizon
      schema: public
      syncCursorInDBEvery: 2
      host: localhost
      port: 5432
      username: postgres
      password: bizon
"""
config = safe_load(config_yaml)
runner = RunnerFactory.create_from_config_dict(config=config)
runner.run()
