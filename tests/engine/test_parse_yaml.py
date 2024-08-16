import os

from bizon.cli.parser import parse_from_yaml
from bizon.engine.runner import RunnerFactory


def test_parse_from_yaml():
    config = parse_from_yaml(os.path.abspath("tests/engine/dummy_pipeline.yml"))
    runner = RunnerFactory.create_from_config_dict(config=config)

    assert runner.is_running is False
