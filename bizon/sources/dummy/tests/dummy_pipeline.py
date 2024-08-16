import os

from bizon.cli.parser import parse_from_yaml
from bizon.pipeline.runner import RunnerFactory

config = parse_from_yaml(os.path.abspath("bizon/sources/dummy/config/api_key.example.yml"))

runner = RunnerFactory.create_from_config_dict(config=config)
runner.run()
