import os

from bizon.cli.parser import parse_from_yaml
from bizon.common.models.config import SyncConfig
from bizon.pipeline.run import Runner

raw_config = parse_from_yaml(os.path.abspath("bizon/sources/hubspot/config/hubspot.yml"))

config = SyncConfig.parse_obj(raw_config)

runner = Runner(config=config)
runner.run()
