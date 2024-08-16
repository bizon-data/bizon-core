import os

from bizon.cli.parser import parse_from_yaml
from bizon.engine.runner import RunnerFactory

config = parse_from_yaml(os.path.abspath("bizon/sources/periscope/config/periscope_dashboards.yml"))

runner = RunnerFactory.create_from_config_dict(config=config)
runner.run()
