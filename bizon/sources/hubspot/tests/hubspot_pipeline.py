import os

from bizon.cli.parser import parse_from_yaml
from bizon.pipeline.run import Runner

config = parse_from_yaml(os.path.abspath("bizon/sources/hubspot/config/hubspot.yml"))

runner = Runner(config=config)
runner.run()
