import os

from bizon.cli.parser import parse_from_yaml
from bizon.engine.runner import RunnerFactory

config = parse_from_yaml(os.path.abspath("bizon/sources/hubspot/config/hubspot.yml"))

runner = RunnerFactory.create_from_yaml(filepath=os.path.abspath("bizon/sources/hubspot/config/hubspot.yml"))
runner.run()
