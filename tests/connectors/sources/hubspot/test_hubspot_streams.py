import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError
from requests import HTTPError

from bizon.connectors.sources.hubspot.src.crm_search import HubSpotCrmSearchSource, HubSpotCrmSearchSourceConfig
from bizon.connectors.sources.hubspot.src.custom_objects import (
    HubSpotCustomObjectSource,
    HubSpotCustomObjectSourceConfig,
)
from bizon.connectors.sources.hubspot.src.hubspot_base import URL_BASE, URL_TOKEN_REFRESH
from bizon.connectors.sources.hubspot.src.hubspot_objects import HubSpotObjectsSource, HubSpotSourceConfig
from bizon.source.models import SourceIncrementalState

API_KEY_AUTH = {"type": "api_key", "params": {"token": "test-token"}}
OAUTH_AUTH = {
    "type": "oauth",
    "params": {
        "token_refresh_endpoint": URL_TOKEN_REFRESH,
        "client_id": "id",
        "client_secret": "secret",
        "refresh_token": "refresh",
    },
}
LAST_RUN = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
RUN_STARTED_AT = datetime(2026, 1, 1, 13, 0, tzinfo=timezone.utc)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _objects_source(stream="contacts", **extra) -> HubSpotObjectsSource:
    config = {"name": "hubspot", "stream": stream, "init_pipeline": False, "authentication": API_KEY_AUTH, **extra}
    return HubSpotObjectsSource(config=HubSpotSourceConfig.model_validate(config))


def _search_filters(request) -> list:
    return json.loads(request.body)["filterGroups"][0]["filters"]


def test_last_page_records_are_returned():
    source = _objects_source()
    iteration = source.parse_response({"results": [{"id": 1}, {"id": 2}]})
    assert iteration.next_pagination == {}
    assert [r.id for r in iteration.records] == ["1", "2"]


def test_empty_page_is_terminal():
    iteration = _objects_source().parse_response({"results": []})
    assert iteration.next_pagination == {}
    assert iteration.records == []


def test_get_sends_no_body_and_no_empty_associations(requests_mock):
    requests_mock.get(f"{URL_BASE}/crm/v3/objects/contacts", json={"results": [{"id": "1"}]})
    _objects_source().get()
    request = requests_mock.last_request
    assert request.body is None
    assert "associations" not in request.qs


def test_get_sends_configured_associations(requests_mock):
    requests_mock.get(f"{URL_BASE}/crm/v3/objects/contacts", json={"results": []})
    _objects_source(associations_types=["companies", "deals"]).get()
    assert requests_mock.last_request.qs["associations"] == ["companies,deals"]


def test_selected_properties_must_exist(requests_mock):
    requests_mock.get(f"{URL_BASE}/crm/v3/properties/contacts", json={"results": [{"name": "email"}]})
    source = _objects_source(properties={"strategy": "selected", "selected_properties": ["email", "nope"]})
    with pytest.raises(ValueError, match="nope"):
        source.get_selected_properties()


def test_objects_incremental_searches_the_window(requests_mock):
    search = requests_mock.post(
        f"{URL_BASE}/crm/v3/objects/contacts/search", json={"results": [{"id": "7"}, {"id": "42"}]}
    )
    state = SourceIncrementalState(last_run=LAST_RUN, run_started_at=RUN_STARTED_AT, cursor_field="updatedAt")

    iteration = _objects_source().get_records_after(source_state=state, pagination={"last_id": "5"})

    assert _search_filters(search.last_request) == [
        {"propertyName": "lastmodifieddate", "operator": "GTE", "value": _ms(LAST_RUN) - 300_000},
        {"propertyName": "lastmodifieddate", "operator": "LT", "value": _ms(RUN_STARTED_AT)},
        {"propertyName": "hs_object_id", "operator": "GT", "value": "5"},
    ]
    assert iteration.next_pagination == {"last_id": "42"}


def _crm_search_source(**extra) -> HubSpotCrmSearchSource:
    config = {
        "name": "hubspot",
        "stream": "deals_search",
        "authentication": API_KEY_AUTH,
        "properties": {"strategy": "selected", "selected_properties": ["dealname"]},
        **extra,
    }
    return HubSpotCrmSearchSource(config=HubSpotCrmSearchSourceConfig.model_validate(config))


@pytest.fixture
def deals_catalog(requests_mock):
    requests_mock.get(f"{URL_BASE}/crm/v3/properties/deals", json={"results": [{"name": "dealname"}]})


def test_crm_search_incremental_window_is_stable_across_pages(requests_mock, deals_catalog):
    search = requests_mock.post(
        f"{URL_BASE}/crm/v3/objects/deals/search",
        [{"json": {"results": [{"id": "3"}, {"id": "9"}]}}, {"json": {"results": []}}],
    )
    source = _crm_search_source()
    state = SourceIncrementalState(last_run=LAST_RUN)

    first = source.get_records_after(source_state=state)
    last = source.get_records_after(source_state=state, pagination=first.next_pagination)

    first_filters, second_filters = (_search_filters(r) for r in search.request_history)
    assert first_filters[0] == {
        "propertyName": "hs_lastmodifieddate",
        "operator": "GTE",
        "value": _ms(LAST_RUN) - 300_000,
    }
    assert first_filters[1] == second_filters[1]
    assert second_filters[2] == {"propertyName": "hs_object_id", "operator": "GT", "value": "9"}
    assert json.loads(search.last_request.body)["properties"] == ["dealname"]
    assert first.next_pagination == {"last_id": "9"}
    assert last.next_pagination == {}


def test_crm_search_full_scan_ignores_the_window(requests_mock, deals_catalog):
    search = requests_mock.post(f"{URL_BASE}/crm/v3/objects/deals/search", json={"results": []})
    _crm_search_source(full_scan=True, start_date="2026-01-01T00:00:00Z").get()
    assert "filterGroups" not in json.loads(search.last_request.body)


def test_crm_search_without_start_date_fetches_everything(requests_mock, deals_catalog):
    search = requests_mock.post(f"{URL_BASE}/crm/v3/objects/deals/search", json={"results": []})
    _crm_search_source().get()
    assert "filterGroups" not in json.loads(search.last_request.body)


def _oauth_objects_source() -> HubSpotObjectsSource:
    config = {"name": "hubspot", "stream": "contacts", "init_pipeline": False, "authentication": OAUTH_AUTH}
    return HubSpotObjectsSource(config=HubSpotSourceConfig.model_validate(config))


EXPIRED = {"status_code": 401, "json": {"category": "EXPIRED_AUTHENTICATION", "message": "expired"}}


def test_expired_token_is_refreshed_and_replayed_once(requests_mock):
    token = requests_mock.post(URL_TOKEN_REFRESH, json={"access_token": "tok", "expires_in": 1800})
    listing = requests_mock.get(f"{URL_BASE}/crm/v3/objects/contacts", [EXPIRED, {"json": {"results": [{"id": "1"}]}}])

    iteration = _oauth_objects_source().get()

    assert [r.id for r in iteration.records] == ["1"]
    assert listing.call_count == 2
    assert token.call_count == 2


def test_expired_token_twice_in_a_row_is_fatal(requests_mock):
    requests_mock.post(URL_TOKEN_REFRESH, json={"access_token": "tok", "expires_in": 1800})
    listing = requests_mock.get(f"{URL_BASE}/crm/v3/objects/contacts", [EXPIRED, EXPIRED, {"json": {"results": []}}])

    with pytest.raises(HTTPError):
        _oauth_objects_source().get()
    assert listing.call_count == 2


def test_other_401_is_not_replayed(requests_mock):
    requests_mock.post(URL_TOKEN_REFRESH, json={"access_token": "tok", "expires_in": 1800})
    listing = requests_mock.get(
        f"{URL_BASE}/crm/v3/objects/contacts", status_code=401, json={"category": "INVALID_AUTHENTICATION"}
    )

    with pytest.raises(HTTPError):
        _oauth_objects_source().get()
    assert listing.call_count == 1


def test_oauth_refreshes_ahead_of_expiry():
    assert _oauth_objects_source().session.auth.get_expiry_skew_seconds() == 120


def _custom_object_config(**extra) -> dict:
    return {
        "name": "hubspot",
        "stream": "custom_object",
        "init_pipeline": False,
        "authentication": API_KEY_AUTH,
        **extra,
    }


def test_custom_object_requires_object_type():
    with pytest.raises(ValidationError):
        HubSpotCustomObjectSourceConfig.model_validate(_custom_object_config())


def test_custom_object_lists_the_configured_object(requests_mock):
    listing = requests_mock.get(f"{URL_BASE}/crm/v3/objects/2-1234567", json={"results": [{"id": "1"}]})
    config = HubSpotCustomObjectSourceConfig.model_validate(_custom_object_config(object_type="2-1234567"))
    source = HubSpotCustomObjectSource(config=config)

    iteration = source.get()

    assert source.object == "2-1234567"
    assert listing.called
    assert [r.id for r in iteration.records] == ["1"]


def test_custom_object_incremental_uses_hs_lastmodifieddate(requests_mock):
    search = requests_mock.post(f"{URL_BASE}/crm/v3/objects/2-1234567/search", json={"results": []})
    config = HubSpotCustomObjectSourceConfig.model_validate(_custom_object_config(object_type="2-1234567"))

    HubSpotCustomObjectSource(config=config).get_records_after(source_state=SourceIncrementalState(last_run=LAST_RUN))

    assert _search_filters(search.last_request)[0]["propertyName"] == "hs_lastmodifieddate"
