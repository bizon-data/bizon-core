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

backend:
  type: sqlite
  database: NOT_USED_WITH_SQLITE
  schema: NOT_USED_WITH_SQLITE
  syncCursorInDBEvery: 2

consumer:
  enabled: true
  bootstrap_servers:
    - localhost:9092

pipeline:
  runner: kafka
  runner_config:
    bootstrap_servers: localhost:9092
    topic: bizon

  log_level: DEBUG
"""
config = safe_load(config_yaml)
runner = RunnerFactory.create_from_config_dict(config=config)
runner.run()
