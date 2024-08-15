import pytest

from bizon.api.auth.config import AuthType
from bizon.sources.dummy.src.config import (
    DummyAuthConfig,
    DummySourceConfig,
    DummyStreams,
)
from bizon.sources.dummy.src.source import DummySource


@pytest.fixture
def dummy_source_config():
    return DummySourceConfig(
        stream_name=DummyStreams.CREATURES, authentication=DummyAuthConfig(type=AuthType.API_KEY, api_key="dummy_key")
    )


@pytest.fixture
def dummy_source(dummy_source_config):
    return DummySource(config=dummy_source_config)


def test_dummy_source_url_entity(dummy_source: DummySource):
    assert dummy_source.url_entity == "https://api.dummy.com/v1/creatures"


def test_dummy_source_get_auth(dummy_source: DummySource):
    assert dummy_source.get_auth().config.api_key == "dummy_key"
    assert dummy_source.get_auth().config.type == AuthType.API_KEY


def test_get_first_page(dummy_source: DummySource):
    next_pagination, records = dummy_source.get()
    assert len(records) == 2
    assert next_pagination == {"cursor": "vfvfvuhfefpeiduzhihxb"}


def test_get_last_page(dummy_source: DummySource):
    next_pagination, records = dummy_source.get(pagination={"cursor": "final-cursor"})
    assert len(records) == 1
    assert next_pagination == {}
