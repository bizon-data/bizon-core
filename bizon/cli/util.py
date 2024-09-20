import yaml


def parse_from_yaml(path_to_yaml) -> dict:
    with open(path_to_yaml) as f:
        config = yaml.safe_load(f)
    return config
