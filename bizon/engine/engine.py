from loguru import logger

from bizon.cli.utils import parse_from_yaml
from bizon.common.models import BizonConfig

from .config import RunnerTypes
from .runner.runner import AbstractRunner


class RunnerFactory:
    @staticmethod
    def create_from_config_dict(config: dict) -> AbstractRunner:

        bizon_config = BizonConfig.model_validate(obj=config)

        if bizon_config.engine.runner.type == RunnerTypes.THREAD:
            from .runner.adapters.thread import ThreadRunner

            return ThreadRunner(config=config)

        if bizon_config.engine.runner.type == RunnerTypes.PROCESS:
            from .runner.adapters.process import ProcessRunner

            return ProcessRunner(config=config)

        raise ValueError(f"Runner type {bizon_config.engine.runner.type} is not supported")

    @staticmethod
    def create_from_yaml(filepath: str) -> AbstractRunner:
        config = parse_from_yaml(filepath)
        return RunnerFactory.create_from_config_dict(config)
