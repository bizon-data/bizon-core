from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from requests.exceptions import HTTPError

from bizon.connectors.sources.buildbetter.src import source as buildbetter
from bizon.source import graphql as gql
from bizon.source.models import SourceIncrementalState

# Shape returned by the BuildBetter API for a malformed query: extensions.code is
# lowercase-hyphenated and never collides with the transport's upper-cased vocabulary.
FATAL_BODY = {
    "errors": [
        {
            "message": "field 'nope' not found in type: 'query_root'",
            "extensions": {"path": "$.selectionSet.nope", "code": "validation-failed"},
        }
    ]
}
TRANSIENT_BODY = {"errors": [{"message": "Internal server error"}]}
HASURA_UNEXPECTED_BODY = {
    "errors": [{"message": "Internal server error", "extensions": {"path": "$", "code": "unexpected"}}]
}
WEBHOOK_BODY = {"errors": [{"message": "webhook authentication request failed"}]}
RATE_LIMITED_BODY = {"errors": [{"message": "slow down", "extensions": {"code": "RATE_LIMITED"}}]}
CONNECTION_ERROR_BODY = {"errors": [{"message": "connection error"}]}
OK_BODY = {"data": {"interview": [{"id": 1}]}}


def _response(status_code=200, body=None):
    response = Mock(status_code=status_code, ok=200 <= status_code < 400, headers={}, text="")
    response.json.return_value = {} if body is None else body
    response.raise_for_status.side_effect = HTTPError(f"HTTP {status_code}", response=response)
    return response


def _bare_source(*responses):
    source = object.__new__(buildbetter.BuildBetterSource)
    source.session = Mock()
    source.session.post.side_effect = list(responses)
    return source


@pytest.fixture
def slept(monkeypatch):
    calls = []
    monkeypatch.setattr(gql.time, "sleep", calls.append)
    return calls


def test_graphql_post_retries_transient_then_succeeds(slept):
    source = _bare_source(_response(body=TRANSIENT_BODY), _response(body=TRANSIENT_BODY), _response(body=OK_BODY))
    assert source._graphql_post("query", {"limit": 500, "cursor": 0}) == OK_BODY
    assert slept == [15, 30]


def test_graphql_post_retries_hasura_unexpected_internal_error_then_succeeds(slept):
    source = _bare_source(_response(body=HASURA_UNEXPECTED_BODY), _response(body=OK_BODY))
    assert source._graphql_post("query", {"limit": 500, "cursor": 0}) == OK_BODY
    assert slept == [15]


def test_graphql_post_retries_hasura_webhook_failure_then_succeeds(slept):
    source = _bare_source(_response(body=WEBHOOK_BODY), _response(body=OK_BODY))
    assert source._graphql_post("query", {"limit": 500, "cursor": 0}) == OK_BODY
    assert slept == [15]


def test_graphql_post_retries_hasura_connection_error_then_succeeds(slept):
    source = _bare_source(_response(body=CONNECTION_ERROR_BODY), _response(body=OK_BODY))
    assert source._graphql_post("query", {"limit": 500, "cursor": 0}) == OK_BODY
    assert source.session.post.call_count == 2
    assert slept == [15]


def test_graphql_post_raises_fatal_immediately_on_a_genuine_query_bug(slept):
    source = _bare_source(_response(body=FATAL_BODY))
    with pytest.raises(gql.GraphQLError) as exc:
        source._graphql_post("query", {"limit": 500, "cursor": 0})
    assert not isinstance(exc.value, gql.TransientGraphQLError)
    assert source.session.post.call_count == 1
    assert slept == []


def test_graphql_post_does_not_claim_a_non_ok_status(slept):
    source = _bare_source(_response(status_code=502, body=TRANSIENT_BODY))
    with pytest.raises(HTTPError):
        source._graphql_post("query", {"limit": 500, "cursor": 0})
    assert source.session.post.call_count == 1
    assert slept == []


def test_classifier_maps_a_rate_limited_error_body_to_rate_limited():
    assert (
        buildbetter._classify_buildbetter_graphql(_response(body=RATE_LIMITED_BODY), RATE_LIMITED_BODY)
        is gql.ErrorClass.RATE_LIMITED
    )


def test_classifier_keeps_a_validation_error_naming_a_timeout_field_fatal():
    body = {
        "errors": [
            {
                "message": "field 'timeout' not found in type: 'interview'",
                "extensions": {"path": "$.selectionSet.interview.selectionSet.timeout", "code": "validation-failed"},
            }
        ]
    }
    assert buildbetter._classify_buildbetter_graphql(_response(body=body), body) is gql.ErrorClass.FATAL


def test_graphql_post_passes_the_shared_timeout(slept):
    source = _bare_source(_response(body=OK_BODY))
    source._graphql_post("query", {"limit": 500, "cursor": 0})
    _, kwargs = source.session.post.call_args
    assert kwargs["timeout"] == gql.GRAPHQL_TIMEOUT


def test_keyset_pagination_full_page_returns_last_row_id_as_cursor():
    rows = [{"id": i} for i in range(500)]
    assert buildbetter.BuildBetterSource._keyset_pagination(rows, 500) == {"cursor": 499}


def test_keyset_pagination_short_page_returns_empty_dict():
    rows = [{"id": i} for i in range(3)]
    assert buildbetter.BuildBetterSource._keyset_pagination(rows, 500) == {}


def test_keyset_pagination_empty_page_returns_empty_dict():
    assert buildbetter.BuildBetterSource._keyset_pagination([], 500) == {}


def _people_source(*pages, stream=buildbetter.BuildBetterStreams.PEOPLE):
    source = object.__new__(buildbetter.BuildBetterSource)
    source.session = Mock()
    source.config = Mock(stream=stream)
    source._graphql_post = Mock(side_effect=[{"data": {"person": page}} for page in pages])
    return source


def _person_rows(n, start_id=1, updated_at="2026-10-06T10:00:00.5"):
    return [{"id": start_id + i, "updated_at": updated_at} for i in range(n)]


def _state(last_run):
    return SourceIncrementalState(last_run=last_run, state={}, cursor_field="updated_at")


def test_people_incremental_first_page_starts_an_hour_before_last_run_and_id_zero():
    source = _people_source([])
    source.get_records_after(_state(datetime(2026, 10, 5, 5, 0, 1, 250000)))
    _, variables = source._graphql_post.call_args.args
    assert variables == {"limit": 500, "updated_at": "2026-10-05T04:00:01.250000", "id": 0}


def test_people_incremental_full_page_returns_compound_next_pagination():
    rows = _person_rows(500)
    source = _people_source(rows)
    iteration = source.get_records_after(_state(datetime(2026, 10, 5, 5, 0)))
    assert iteration.next_pagination == {"updated_at": "2026-10-06T10:00:00.5", "id": 500}
    assert [r.id for r in iteration.records][:2] == ["1", "2"]


def test_people_incremental_next_page_uses_the_compound_cursor():
    source = _people_source([])
    source.get_records_after(_state(datetime(2026, 10, 5, 5, 0)), {"updated_at": "2026-10-06T10:00:00.5", "id": 500})
    _, variables = source._graphql_post.call_args.args
    assert variables == {"limit": 500, "updated_at": "2026-10-06T10:00:00.5", "id": 500}


def test_people_incremental_short_page_returns_empty_dict():
    source = _people_source(_person_rows(3))
    iteration = source.get_records_after(_state(datetime(2026, 10, 5, 5, 0)))
    assert iteration.next_pagination == {}
    assert len(iteration.records) == 3


def test_people_incremental_converts_tz_aware_last_run_to_naive_utc():
    source = _people_source([])
    last_run = datetime(2026, 10, 5, 7, 0, tzinfo=timezone(timedelta(hours=2)))
    source.get_records_after(_state(last_run))
    _, variables = source._graphql_post.call_args.args
    assert variables["updated_at"] == "2026-10-05T04:00:00"


def test_incremental_rejects_streams_other_than_people():
    source = _people_source(stream=buildbetter.BuildBetterStreams.CALLS)
    with pytest.raises(NotImplementedError):
        source.get_records_after(_state(datetime(2026, 10, 5, 5, 0)))


def test_config_keeps_non_2xx_bodies_for_the_graphql_transport():
    config = buildbetter.BuildBetterSourceConfig(name="buildbetter", stream="calls", api_key="x")
    assert config.http.raise_for_status is False


def test_transform_company_lifts_configured_metadata_fields():
    row = {
        "id": 7,
        "name": "Acme",
        "metadata_records": [
            {
                "record": {
                    "values": [{"field": {"name": "arr"}, "value": 1200}, {"field": {"name": "Tier"}, "value": "A"}]
                }
            }
        ],
    }

    company = buildbetter.BuildBetterSource._transform_company(row, {"annual_revenue": "arr", "missing": "nope"})

    assert company["metadata"] == {"arr": 1200, "Tier": "A"}
    assert company["annual_revenue"] == 1200
    assert company["missing"] is None
    assert "Tier" not in company
