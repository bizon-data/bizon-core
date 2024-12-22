import pytest

from bizon.source.discover import (
    discover_all_sources,
    find_all_source_paths,
    get_external_source_class_by_source_and_stream,
    get_internal_source_class_by_source_and_stream,
    get_source_instance_by_source_and_stream,
    parse_streams_from_filepath,
)

DUMMY_SOURCE_CONFIG_DICT = {
    "name": "dummy",
    "stream": "creatures",
    "config": {"dummy": "dummy"},
    "authentication": {"type": "api_key", "params": {"token": "my_dummy_token"}},
}

DUMMY_EXTERNAL_SOURCE_CONFIG_DICT = {
    "name": "dummy",
    "stream": "flowers",
    "config": {"dummy": "dummy"},
    "authentication": {"type": "api_key", "params": {"token": "my_dummy"}},
    "source_file_path": "./tests/source/custom_source.py",
}


def test_find_all_source_paths():
    source_paths = find_all_source_paths()
    assert len(source_paths) > 0
    assert "dummy" in source_paths


def test_parse_streams_from_filepath():
    streams = parse_streams_from_filepath(
        source_name="dummy", filepath="bizon/connectors/sources/dummy/src/source.py", skip_unavailable_sources=True
    )
    assert len(streams) > 0
    set(stream.name for stream in streams) == set(["creatures", "plants"])


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


def test_get_external_source_class_by_source_and_stream():
    source_class = get_external_source_class_by_source_and_stream(
        source_name="dummy",
        stream_name="flowers",
        filepath="./tests/source/custom_source.py",
    )
    assert source_class.__name__ == "MyDummyCustomSource"


def test_get_external_source_class_by_source_and_stream_invalid_stream():
    with pytest.raises(ValueError) as error:
        source_class = get_external_source_class_by_source_and_stream(
            source_name="dummy",
            stream_name="creatures",
            filepath="./tests/source/custom_source.py",
        )
    assert "Stream creatures not found. Available streams are ['flowers']" in str(error.value)


def test_get_source_instance_by_source_and_stream():
    source_instance = get_source_instance_by_source_and_stream("dummy", "creatures", DUMMY_SOURCE_CONFIG_DICT)
    assert source_instance.__class__.__name__ == "DummySource"


def test_get_source_instance_by_source_and_stream_external_file():
    source_instance = get_source_instance_by_source_and_stream("dummy", "flowers", DUMMY_EXTERNAL_SOURCE_CONFIG_DICT)
    assert source_instance.__class__.__name__ == "MyDummyCustomSource"


def test_discover_all_sources():
    all_sources = discover_all_sources()

    assert len(all_sources) > 0

    found_dummy = False

    for source_name, source_model in all_sources.items():
        if source_name == "dummy":
            found_dummy = True
            assert set(source_model.available_streams) == set(["creatures", "plants"])

    assert found_dummy
