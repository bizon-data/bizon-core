from bizon.connectors.sources.dummy.src.fake_api import (
    fake_api_call_creatures,
    fake_api_call_plants,
)


def test_fake_api_call_creatures_first_page():
    first_page = fake_api_call_creatures()
    assert len(first_page["results"]) == 2
    assert first_page["next"]["cursor"] == "vfvfvuhfefpeiduzhihxb"


def test_fake_api_call_creatures_last_page():
    last_page = fake_api_call_creatures(cursor="final-cursor")
    assert len(last_page["results"]) == 1
    assert last_page["next"]["cursor"] == ""


def test_fake_api_call_plants_first_page():
    first_page = fake_api_call_plants()
    assert len(first_page["results"]) == 2
    assert first_page["next"]["cursor"] == "vfvfvuhfefpeiduzhihxb"


def test_fake_api_call_plants_last_page():
    last_page = fake_api_call_plants(cursor="final-cursor")
    assert len(last_page["results"]) == 1
    assert last_page["next"]["cursor"] == ""
