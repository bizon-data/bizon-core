import os

from bizon.cli.parser import parse_from_yaml
from bizon.pipeline.runner import Runner


def test_load_from_config():
    config = parse_from_yaml(os.path.abspath("tests/pipeline/dummy_pipeline.yml"))
    runner = Runner.from_yaml(os.path.abspath("tests/pipeline/dummy_pipeline.yml"))
    assert runner.raw_config == config
    assert runner.is_running is False


def test_run():
    runner = Runner.from_yaml(os.path.abspath("tests/pipeline/dummy_pipeline.yml"))
    assert runner.is_running is False


def test_get_source():
    runner = Runner.from_yaml(os.path.abspath("tests/pipeline/dummy_pipeline.yml"))
    source = runner.get_source()
    assert source.config.source_name == "dummy"


def test_get_backend():
    runner = Runner.from_yaml(os.path.abspath("tests/pipeline/dummy_pipeline.yml"))
    backend = runner.get_backend()
    assert backend.config.type == "sqlite"
