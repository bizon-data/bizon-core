import os

from bizon.cli.parser import parse_from_yaml
from bizon.pipeline.runner import Runner

config = parse_from_yaml(os.path.abspath("bizon/sources/periscope/config/periscope_dashboards.yml"))

runner = Runner(config=config)
runner.run()
