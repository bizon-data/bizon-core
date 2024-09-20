import pytest

from bizon.sources import (
    discover_all_sources,
    get_internal_source_class_by_source_and_stream,
    get_source_instance_by_source_and_stream,
)

DUMMY_SOURCE_CONFIG_DICT = {
    "source_name": "dummy",
    "stream_name": "creatures",
    "config": {"dummy": "dummy"},
    "authentication": {"type": "api_key", "params": {"token": "my_dummy_token"}},
}


def test_get_internal_source_class_by_source_and_stream():
    source_class = get_internal_source_class_by_source_and_stream("dummy", "creatures")
    assert source_class.__name__ == "DummySource"


def test_get_internal_source_class_by_source_and_stream_stream_not_found():
    with pytest.raises(ValueError) as error:
        source_class = get_internal_source_class_by_source_and_stream("dummy", "invalid_stream")
    assert "Stream invalid_stream not found. Available streams are" in str(error.value)


def test_get_internal_source_class_by_source_and_stream_stream_source_invalid():
    with pytest.raises(ValueError) as error:
        source_class = get_internal_source_class_by_source_and_stream("invalid_source", "inexistent_stream")
    assert "Source invalid_source not found. Available sources are" in str(error.value)


def test_get_source_instance_by_source_and_stream():
    source_instance = get_source_instance_by_source_and_stream("dummy", "creatures", DUMMY_SOURCE_CONFIG_DICT)
    assert source_instance.__class__.__name__ == "DummySource"


def test_discover_all_sources():
    all_sources = discover_all_sources()

    assert len(all_sources) > 0

    found_dummy = False

    for source_name, source_model in all_sources.items():
        if source_name == "dummy":
            found_dummy = True
            assert set(source_model.available_streams) == set(["creatures", "plants"])

    assert found_dummy
