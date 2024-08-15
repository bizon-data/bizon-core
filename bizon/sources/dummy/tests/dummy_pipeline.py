import os

from bizon.cli.parser import parse_from_yaml
from bizon.pipeline.runner import Runner

config = parse_from_yaml(os.path.abspath("bizon/sources/dummy/config/api_key.example.yml"))

runner = Runner(config=config)
runner.run()
