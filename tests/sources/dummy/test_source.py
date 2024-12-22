import pytest

from bizon.connectors.sources.dummy.src.source import (
    DummyAuthConfig,
    DummySource,
    DummySourceConfig,
)
from bizon.source.auth.authenticators.token import TokenAuthParams
from bizon.source.auth.config import AuthType


@pytest.fixture
def dummy_source_config():
    return DummySourceConfig(
        name="dummy",
        stream="creatures",
        authentication=DummyAuthConfig(type=AuthType.API_KEY, params=TokenAuthParams(token="dummy_key")),
    )


@pytest.fixture
def dummy_source(dummy_source_config):
    return DummySource(config=dummy_source_config)


def test_dummy_source_url_entity(dummy_source: DummySource):
    assert dummy_source.url_entity == "https://api.dummy.com/v1/creatures"


def test_dummy_source_get_auth(dummy_source: DummySource):
    assert dummy_source.get_authenticator().token == "Bearer dummy_key"


def test_get_first_page(dummy_source: DummySource):
    source_iteration = dummy_source.get()
    assert len(source_iteration.records) == 2
    assert source_iteration.next_pagination == {"cursor": "vfvfvuhfefpeiduzhihxb"}


def test_get_last_page(dummy_source: DummySource):
    source_iteration = dummy_source.get(pagination={"cursor": "final-cursor"})
    assert len(source_iteration.records) == 1
    assert source_iteration.next_pagination == {}
