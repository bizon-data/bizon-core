from os import getenv

from bizon.cli.utils import parse_from_yaml
from bizon.common.models import BizonConfig

from .config import RunnerTypes
from .runner.runner import AbstractRunner


def replace_env_variables_in_config(config: dict) -> dict:
    """Replace templated secrets with actual values from environment variables"""
    for key, value in config.items():
        if isinstance(value, dict):
            config[key] = replace_env_variables_in_config(value)
        elif isinstance(value, str):
            if value.startswith("BIZON_ENV_"):
                config[key] = getenv(value)
    return config


class RunnerFactory:
    @staticmethod
    def create_from_config_dict(config: dict) -> AbstractRunner:

        # Replace env variables in config
        config = replace_env_variables_in_config(config=config)

        bizon_config = BizonConfig.model_validate(obj=config)

        if bizon_config.engine.runner.type == RunnerTypes.THREAD:
            from .runner.adapters.thread import ThreadRunner

            return ThreadRunner(config=config)

        if bizon_config.engine.runner.type == RunnerTypes.PROCESS:
            from .runner.adapters.process import ProcessRunner

            return ProcessRunner(config=config)

        if bizon_config.engine.runner.type == RunnerTypes.STREAM:
            from .runner.adapters.streaming import StreamingRunner

            return StreamingRunner(config=config)

        raise ValueError(f"Runner type {bizon_config.engine.runner.type} is not supported")

    @staticmethod
    def create_from_yaml(filepath: str) -> AbstractRunner:
        config = parse_from_yaml(filepath)
        return RunnerFactory.create_from_config_dict(config)
