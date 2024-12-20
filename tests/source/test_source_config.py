from yaml import safe_load

from bizon.source.config import SourceConfig


def test_source_config():

    config = """
    source:
        name: dummy
        stream: creatures
        authentication:
            type: api_key
            params:
                token: dummy_key
    """
    config_dict = safe_load(config)
    source_config = SourceConfig.model_validate_strings(config_dict["source"])
    assert source_config.name == "dummy"
    assert source_config.stream == "creatures"
