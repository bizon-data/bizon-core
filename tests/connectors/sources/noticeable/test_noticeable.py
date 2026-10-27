from unittest.mock import Mock

import pytest
from requests.exceptions import HTTPError

from bizon.connectors.sources.noticeable.src import source as noticeable
from bizon.source import graphql as gql

TRANSIENT_BODY = {"errors": [{"message": "Internal server error"}]}
FATAL_BODY = {"errors": [{"message": "Field 'nope' doesn't exist on type 'Query'"}]}
PUBLICATIONS_OK_BODY = {
    "data": {
        "project": {
            "publications": {
                "pageInfo": {"endCursor": "cursor-2", "hasNextPage": False},
                "edges": [{"cursor": "cursor-2", "node": {"id": "pub-1", "title": "hello"}}],
            }
        }
    }
}


def _response(status_code=200, body=None):
    response = Mock(status_code=status_code, ok=200 <= status_code < 400, headers={}, text="")
    response.json.return_value = {} if body is None else body
    response.raise_for_status.side_effect = HTTPError(f"HTTP {status_code}", response=response)
    return response


def _bare_source(*responses):
    source = object.__new__(noticeable.NoticeableSource)
    source.url_graphql = "https://api.noticeable.io/graphql"
    source.config = Mock(project_id="proj-123")
    source.session = Mock()
    source.session.post.side_effect = list(responses)
    source._page_sizes = dict(noticeable.DEFAULT_PAGE_SIZES)
    return source


@pytest.fixture
def slept(monkeypatch):
    calls = []
    monkeypatch.setattr(gql.time, "sleep", calls.append)
    return calls


# --------------------------------------------------------------------------
# classifier behaviour (default_classifier, routed via graphql_post_adaptive)
# --------------------------------------------------------------------------


def test_transient_errors_retry_then_succeed(slept):
    source = _bare_source(
        _response(body=TRANSIENT_BODY), _response(body=TRANSIENT_BODY), _response(body=PUBLICATIONS_OK_BODY)
    )
    result = source.get_publications(pagination=None)
    assert len(result.records) == 1
    assert result.records[0].data["id"] == "pub-1"
    assert slept == [15, 30]


def test_fatal_error_raises_without_retry(slept):
    source = _bare_source(_response(body=FATAL_BODY))
    with pytest.raises(gql.GraphQLError) as exc:
        source.get_publications(pagination=None)
    assert not isinstance(exc.value, gql.TransientGraphQLError)
    assert source.session.post.call_count == 1
    assert slept == []


# --------------------------------------------------------------------------
# GraphQL variables replace string interpolation
# --------------------------------------------------------------------------


def test_publications_query_uses_variables_not_interpolation():
    source = _bare_source(_response(body=PUBLICATIONS_OK_BODY))
    source.get_publications(pagination={"endCursor": "cursor-live-value"})

    _, kwargs = source.session.post.call_args
    payload = kwargs["json"]
    assert payload["variables"]["first"] == 10
    assert payload["variables"]["after"] == "cursor-live-value"
    assert payload["variables"]["projectId"] == "proj-123"
    # cursor must travel only through `variables`, never string-interpolated
    # into the query text itself.
    assert "cursor-live-value" not in payload["query"]
    assert "$PAGINATION_STRING" not in payload["query"]
    assert "$project_id" not in payload["query"]


def test_first_call_has_no_pagination_sends_after_null():
    source = _bare_source(_response(body=PUBLICATIONS_OK_BODY))
    source.get_publications(pagination=None)

    _, kwargs = source.session.post.call_args
    assert kwargs["json"]["variables"]["after"] is None


# --------------------------------------------------------------------------
# default session retries 429/5xx on POST and hands non-2xx bodies back
# --------------------------------------------------------------------------


def test_session_retries_429_and_5xx_on_post():
    source = _bare_source()
    source.config = noticeable.NoticeableSourceConfig(name="noticeable", stream="publications", project_id="proj-123")
    session = source.get_session()
    adapter = session.adapters["https://"]
    assert set(adapter.max_retries.status_forcelist) >= {429, 500, 502, 503, 504}
    assert "POST" in adapter.max_retries.allowed_methods
    # The GraphQL classifier reads errors[] out of 4xx bodies, so the session must not raise first.
    assert not session.hooks["response"]


# --------------------------------------------------------------------------
# a transient blip shrinks `first` on the same cursor instead of aborting
# --------------------------------------------------------------------------


def test_transient_blip_shrinks_first_on_the_same_cursor(slept):
    # 3 transient responses exhaust the default transient budget at first=10,
    # then the adaptive ladder retries the SAME cursor at a smaller page size.
    # `variables` is mutated and reused in place across calls (graphql_post
    # doesn't copy it), so `call_args_list` would just show the final state
    # four times over -- snapshot the size/cursor at call time instead.
    responses = [
        _response(body=TRANSIENT_BODY),
        _response(body=TRANSIENT_BODY),
        _response(body=TRANSIENT_BODY),
        _response(body=PUBLICATIONS_OK_BODY),
    ]
    seen = []

    def post(url, json, **kwargs):
        seen.append((json["variables"]["first"], json["variables"]["after"]))
        return responses[len(seen) - 1]

    source = _bare_source()
    source.session.post.side_effect = post

    result = source.get_publications(pagination={"endCursor": "cursor-abc"})
    assert len(result.records) == 1
    assert seen == [(10, "cursor-abc")] * 3 + [(5, "cursor-abc")]


def test_shrunk_page_size_carries_forward_to_the_next_page(slept):
    """A page that shrank once starts the NEXT page already shrunk, rather than
    re-walking the ladder from the stream's full size every call -- otherwise a
    sustained degradation on a thousands-of-pages stream re-pays the same ladder
    on every single page instead of paying it once."""
    responses = [
        _response(body=TRANSIENT_BODY),
        _response(body=TRANSIENT_BODY),
        _response(body=TRANSIENT_BODY),
        _response(body=PUBLICATIONS_OK_BODY),  # first page recovers at first=5
        _response(body=PUBLICATIONS_OK_BODY),  # second page: should start at 5, not 10
    ]
    seen = []

    def post(url, json, **kwargs):
        seen.append(json["variables"]["first"])
        return responses[len(seen) - 1]

    source = _bare_source()
    source.session.post.side_effect = post

    source.get_publications(pagination={"endCursor": "cursor-abc"})
    assert seen == [10, 10, 10, 5]

    source.get_publications(pagination={"endCursor": "cursor-def"})
    assert seen[-1] == 5, "second page must start from the shrunk size, not reset to 10"


# --------------------------------------------------------------------------
# query constants: variables and page sizes (all four streams)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "query, label",
    [
        (noticeable.EMAIL_OPENED_EVENTS_QUERY, "email_opened_events"),
        (noticeable.PUBLICATIONS_QUERY, "publications"),
        (noticeable.PUBLICATION_COMMENTS_QUERY, "publication_comments"),
        (noticeable.EMAIL_SUBSCRIPTIONS_QUERY, "email_subscriptions"),
    ],
    ids=["email_opened_events", "publications", "publication_comments", "email_subscriptions"],
)
def test_query_declares_variables_with_live_verified_types(query, label):
    # $projectId is ID! on every root field that takes it (project and publicationComments alike).
    # A string literal coerces to either type; a declared variable does not.
    assert "$first: Int!" in query
    assert "$after: String" in query
    assert "$projectId: ID!" in query
    assert "$PAGINATION_STRING" not in query
    assert "$project_id" not in query


@pytest.mark.parametrize(
    "label, expected_size",
    [
        ("email_opened_events", 100),
        ("publications", 10),
        ("publication_comments", 10),
        ("email_subscriptions", 100),
    ],
)
def test_default_page_size_matches_the_stream(label, expected_size):
    assert noticeable.DEFAULT_PAGE_SIZES[label] == expected_size
