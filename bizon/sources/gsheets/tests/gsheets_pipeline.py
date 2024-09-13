import os

from bizon.engine.runner import RunnerFactory

runner = RunnerFactory.create_from_yaml(filepath=os.path.abspath("bizon/sources/gsheets/config/gsheets.yml"))
runner.run()
