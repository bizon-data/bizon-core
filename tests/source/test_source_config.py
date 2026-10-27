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


def test_partial_http_block_merges_over_a_source_specific_default():
    from typing import Optional

    from pydantic import Field

    from bizon.source.config import HttpConfig, HttpRetryConfig, SourceConfig

    class TunedConfig(SourceConfig):
        http: Optional[HttpConfig] = Field(
            default_factory=lambda: HttpConfig(
                raise_for_status=False, retries=HttpRetryConfig(status_forcelist=[500, 502])
            )
        )

    config = TunedConfig(name="x", stream="y", http={"timeout": [10, 120], "retries": {"total": 3}})

    assert config.http.timeout == (10, 120)
    assert config.http.raise_for_status is False
    assert config.http.retries.total == 3
    assert config.http.retries.status_forcelist == [500, 502]
    assert TunedConfig(name="x", stream="y").http.raise_for_status is False


def test_http_block_on_a_source_without_a_default_uses_httpconfig_defaults():
    from bizon.source.config import SourceConfig

    class PlainConfig(SourceConfig):
        pass

    assert PlainConfig(name="x", stream="y").http is None
    config = PlainConfig(name="x", stream="y", http={"raise_for_status": False})
    assert config.http.raise_for_status is False
    assert config.http.timeout == (10, 60)
