import os

import yaml


def parse_from_yaml(path_to_yaml) -> dict:
    with open(path_to_yaml) as f:
        config = yaml.safe_load(f)
    return config


def set_debug_mode(debug: bool):
    # Set Log Level to DEBUG
    if debug:
        os.environ["LOGURU_LEVEL"] = "DEBUG"


def set_custom_source_path_in_config(config: dict, custom_source: str):
    if custom_source:
        config["source"]["source_file_path"] = custom_source


def set_runner_in_config(config: dict, runner: str):
    if runner:
        if "engine" not in config:
            config["engine"] = {}

        if "runner" not in config["engine"]:
            config["engine"]["runner"] = {"type": runner}

        config["engine"]["runner"]["type"] = runner
