import time
from unittest.mock import Mock

import pytest
from requests.exceptions import HTTPError

from bizon.connectors.sources.linear.src import source as linear
from bizon.source import graphql as gql

# Verbatim from a throttled Linear response: HTTP 400, not 429, which is why
# urllib3's status_forcelist never sees it.
RATELIMITED_BODY = {
    "errors": [
        {
            "message": "Rate limit exceeded",
            "extensions": {"code": "RATELIMITED", "type": "ratelimited"},
        }
    ]
}
INTERNAL_ERROR_BODY = {"errors": [{"message": "Internal server error"}]}
OK_BODY = {"data": {"viewer": {"id": "u1"}}}


def _response(status_code=200, body=None, headers=None):
    response = Mock(
        status_code=status_code,
        ok=200 <= status_code < 400,
        headers=headers or {},
        text="",
    )
    response.json.return_value = {} if body is None else body
    response.raise_for_status.side_effect = HTTPError(f"HTTP {status_code}", response=response)
    return response


def _bare_source(*responses):
    """A LinearSource with just enough state for the transport: bypasses __init__
    (which needs a real LinearSourceConfig) and stubs the session."""
    source = object.__new__(linear.LinearSource)
    source._ratelimit_headers_logged = False
    source.session = Mock()
    source.session.post.side_effect = list(responses)
    return source


@pytest.fixture
def slept(monkeypatch):
    calls = []
    monkeypatch.setattr(gql.time, "sleep", calls.append)
    return calls


# --------------------------------------------------------------------------
# _classify_linear_graphql
# --------------------------------------------------------------------------


def test_classifier_http_400_ratelimited():
    assert (
        linear._classify_linear_graphql(_response(400, RATELIMITED_BODY), RATELIMITED_BODY)
        is gql.ErrorClass.RATE_LIMITED
    )


@pytest.mark.parametrize(
    "body",
    [
        INTERNAL_ERROR_BODY,
        {"errors": [{"extensions": {"type": "INTERNAL"}}]},
        {"errors": [{"extensions": {"code": "SERVICE_UNAVAILABLE"}}]},
        {"errors": [{"message": "Request timed out"}]},
    ],
)
def test_classifier_transient_signatures(body):
    assert linear._classify_linear_graphql(_response(200, body), body) is gql.ErrorClass.TRANSIENT


def test_classifier_genuine_query_bug_is_fatal():
    body = {"errors": [{"message": "Field 'nope' doesn't exist on type 'Issue'"}]}
    assert linear._classify_linear_graphql(_response(200, body), body) is gql.ErrorClass.FATAL


def test_classifier_success_is_unclaimed():
    assert linear._classify_linear_graphql(_response(200, OK_BODY), OK_BODY) is None


@pytest.mark.parametrize("status", [401, 500, 503])
def test_classifier_leaves_other_non_ok_statuses_unclaimed(status):
    """The session adapter already retries 5xx; claiming them TRANSIENT
    here (as the transport's default_classifier does) would add a second ladder."""
    assert linear._classify_linear_graphql(_response(status), {}) is None


def test_classifier_does_not_read_the_top_level_type_field():
    """Linear puts the discriminator under extensions; widening the match set is a
    silent behaviour change, not a cleanup."""
    body = {"errors": [{"type": "INTERNAL"}]}
    assert linear._classify_linear_graphql(_response(200, body), body) is gql.ErrorClass.FATAL


# --------------------------------------------------------------------------
# _rate_limit_reset_wait_seconds
# --------------------------------------------------------------------------


def _wait(headers):
    return linear.LinearSource._rate_limit_reset_wait_seconds(_response(400, headers=headers))


def test_retry_after_wins_over_both_reset_headers():
    far_future_ms = str(int((time.time() + 3600) * 1000))
    assert (
        _wait(
            {
                "Retry-After": "42",
                "X-RateLimit-Requests-Reset": far_future_ms,
                "X-RateLimit-Complexity-Reset": far_future_ms,
            }
        )
        == 42.0
    )


def test_requests_reset_is_parsed_as_epoch_milliseconds():
    wait = _wait({"X-RateLimit-Requests-Reset": str(int((time.time() + 120) * 1000))})
    assert 118 <= wait <= 121, "epoch-ms, not epoch-seconds"


def test_complexity_reset_is_used_only_when_requests_reset_is_absent():
    requests_ms = str(int((time.time() + 30) * 1000))
    complexity_ms = str(int((time.time() + 600) * 1000))
    both = _wait({"X-RateLimit-Requests-Reset": requests_ms, "X-RateLimit-Complexity-Reset": complexity_ms})
    assert 28 <= both <= 31
    only_complexity = _wait({"X-RateLimit-Complexity-Reset": complexity_ms})
    assert 598 <= only_complexity <= 601


def test_a_reset_already_in_the_past_never_goes_negative():
    assert _wait({"X-RateLimit-Requests-Reset": str(int((time.time() - 600) * 1000))}) == 0.0


@pytest.mark.parametrize(
    "headers",
    [{}, {"Retry-After": "soon"}, {"X-RateLimit-Requests-Reset": "not-a-number"}],
)
def test_missing_or_malformed_headers_fall_back_to_the_blind_ladder(headers):
    assert _wait(headers) is None


def test_rate_limit_hint_is_capped_at_900s_and_dumps_headers_once(slept, monkeypatch):
    """A reset an hour out must not stall the stream on a single throttle event,
    and the raw-header dump fires once per source lifetime, not per event."""
    far_future_ms = str(int((time.time() + 3600) * 1000))
    throttled = _response(400, RATELIMITED_BODY, {"X-RateLimit-Requests-Reset": far_future_ms})
    source = _bare_source(throttled, throttled, _response(200, OK_BODY))

    dumps = []
    monkeypatch.setattr(linear.logger, "warning", lambda msg: dumps.append(msg))
    assert source._graphql_post("{ viewer { id } }") == OK_BODY

    assert slept == [900, 900]
    assert sum("first occurrence this run" in d for d in dumps) == 1


def test_rate_limit_budget_is_seven_attempts_on_the_blind_ladder(slept):
    source = _bare_source(*[_response(400, RATELIMITED_BODY) for _ in range(7)])
    with pytest.raises(gql.GraphQLRateLimited):
        source._graphql_post("{ viewer { id } }")
    assert source.session.post.call_count == 7
    assert slept == [60, 120, 240, 480, 900, 900]


def test_transient_budget_is_four_attempts(slept):
    source = _bare_source(*[_response(200, INTERNAL_ERROR_BODY) for _ in range(4)])
    with pytest.raises(gql.TransientGraphQLError):
        source._graphql_post("{ viewer { id } }")
    assert source.session.post.call_count == 4
    assert slept == [15, 30, 60]


def test_every_post_carries_an_explicit_timeout(slept):
    source = _bare_source(_response(200, OK_BODY))
    source._graphql_post("{ viewer { id } }")
    assert source.session.post.call_args.kwargs["timeout"] == gql.GRAPHQL_TIMEOUT


# --------------------------------------------------------------------------
# Shrink ladder
# --------------------------------------------------------------------------


def test_history_shrink_ladder_is_100_50_10_1():
    assert gql.build_page_shrink_sequence(100, 1, steps=linear.HISTORY_SHRINK_STEPS) == [100, 50, 10, 1]


def test_history_shrink_ladder_at_history_first_10_has_no_intermediate_rung():
    """The transport's default steps would insert a 5 here; Linear's never has."""
    assert gql.build_page_shrink_sequence(10, 1, steps=linear.HISTORY_SHRINK_STEPS) == [10, 1]
    assert gql.build_page_shrink_sequence(50, 1, steps=linear.HISTORY_SHRINK_STEPS) == [50, 10, 1]


def test_adaptive_walks_the_linear_ladder_and_returns_the_size_that_worked():
    source = object.__new__(linear.LinearSource)
    source.session = Mock()
    sizes = []

    def post(query, variables):
        sizes.append(variables["first"])
        if variables["first"] > 10:
            raise gql.TransientGraphQLError("Internal server error")
        return {"data": {}}

    source._graphql_post = post
    variables = {"issueId": "i1", "after": "cursor-1"}
    _, size = source._graphql_post_adaptive(
        linear.ISSUE_HISTORY_PAGE_QUERY, variables, start_size=100, label="issue_history:i1"
    )
    assert sizes == [100, 50, 10]
    assert size == 10
    assert variables["after"] == "cursor-1", "same cursor retried, never advanced"


# --------------------------------------------------------------------------
# _fetch_history_batch -- ladder down to the id-only floor
# --------------------------------------------------------------------------


def _history_batch_source(post):
    source = object.__new__(linear.LinearSource)
    source.session = Mock()
    source._graphql_post = post
    return source


def _batch_variables():
    return {"first": 25, "historyFirst": 10, "after": "cursor-1", "filter": {"updatedAt": {}}}


def test_fetch_history_batch_happy_path_reports_no_skip():
    body = {"data": {"issues": {"nodes": [], "pageInfo": {"hasNextPage": False}}}}
    result, skipped_head = _history_batch_source(lambda q, v: body)._fetch_history_batch(
        batch_query=linear.ISSUES_WITH_HISTORY_QUERY,
        idonly_query=linear.ISSUES_IDONLY_QUERY,
        connection="issues",
        variables=_batch_variables(),
        history_first=10,
        label="issue_history",
    )
    assert (result, skipped_head) == (body, 0)


def test_fetch_history_batch_reaches_the_id_only_floor_and_increments_skipped_head():
    """Every rung of the ladder fails on the history subfield; only the id-only
    projection (a scalar `id`, no history) can advance the cursor."""
    attempts = []
    idonly_page_info = {"hasNextPage": True, "endCursor": "cursor-2"}

    def post(query, variables):
        attempts.append((query, variables["first"], variables.get("historyFirst")))
        if query == linear.ISSUES_IDONLY_QUERY:
            return {"data": {"issues": {"pageInfo": idonly_page_info, "nodes": [{"id": "poison-issue"}]}}}
        raise gql.TransientGraphQLError("Internal server error")

    result, skipped_head = _history_batch_source(post)._fetch_history_batch(
        batch_query=linear.ISSUES_WITH_HISTORY_QUERY,
        idonly_query=linear.ISSUES_IDONLY_QUERY,
        connection="issues",
        variables=_batch_variables(),
        history_first=10,
        label="issue_history",
    )

    assert skipped_head == 1
    page = result["data"]["issues"]
    assert page["pageInfo"] == idonly_page_info, "cursor advances past the poison parent"
    assert page["nodes"] == [
        {
            "id": "poison-issue",
            "history": {"nodes": [], "pageInfo": {"hasNextPage": False, "endCursor": None}},
        }
    ]
    # full batch, historyFirst=1, then first=1/historyFirst=1, then the floor.
    assert [(a[1], a[2]) for a in attempts[:-1]] == [(25, 10), (25, 1), (1, 1)]
    assert attempts[-1][0] == linear.ISSUES_IDONLY_QUERY


def test_fetch_history_batch_fails_closed_when_even_the_id_only_floor_fails():
    def post(query, variables):
        raise gql.TransientGraphQLError("Internal server error")

    with pytest.raises(gql.TransientGraphQLError):
        _history_batch_source(post)._fetch_history_batch(
            batch_query=linear.ISSUES_WITH_HISTORY_QUERY,
            idonly_query=linear.ISSUES_IDONLY_QUERY,
            connection="issues",
            variables=_batch_variables(),
            history_first=10,
            label="issue_history",
        )


# --------------------------------------------------------------------------
# Overflow paginators -- skip-and-advance
# --------------------------------------------------------------------------


def _history_page(nodes, has_next_page, end_cursor=None):
    return {
        "data": {
            "issue": {
                "history": {
                    "nodes": nodes,
                    "pageInfo": {"hasNextPage": has_next_page, "endCursor": end_cursor},
                }
            }
        }
    }


def test_paginate_issue_history_skips_an_unfetchable_tail_and_keeps_what_it_has():
    cursors = []

    def adaptive(query, variables, *, start_size, label=""):
        cursors.append(variables["after"])
        if variables["after"] == "cursor-1":
            return _history_page([{"id": "h1"}], True, "cursor-2"), start_size
        raise gql.TransientGraphQLError("Internal server error")

    source = object.__new__(linear.LinearSource)
    source.session = Mock()
    source._graphql_post_adaptive = adaptive

    nodes, skipped = source._paginate_issue_history("i1", "cursor-1")
    assert skipped == 1
    assert nodes == [{"id": "h1"}], "page 1 is kept; only the unfetchable tail is dropped"
    assert cursors == ["cursor-1", "cursor-2"], "the cursor advanced before the tail failed"


def test_paginate_issue_history_walks_every_page_when_healthy():
    def adaptive(query, variables, *, start_size, label=""):
        if variables["after"] == "cursor-1":
            return _history_page([{"id": "h1"}], True, "cursor-2"), start_size
        return _history_page([{"id": "h2"}], False), start_size

    source = object.__new__(linear.LinearSource)
    source.session = Mock()
    source._graphql_post_adaptive = adaptive

    assert source._paginate_issue_history("i1", "cursor-1") == ([{"id": "h1"}, {"id": "h2"}], 0)


def test_get_issue_history_surfaces_both_skip_counters(monkeypatch):
    """The `skipped_overflow=` / `skipped_head=` summary is the only observability
    surface for a run that reports success with a gap in it."""
    source = object.__new__(linear.LinearSource)
    source.session = Mock()
    source._start_date = None
    source.config = Mock(stream=linear.LinearStreams.ISSUE_HISTORY)

    page = {
        "pageInfo": {"hasNextPage": True, "endCursor": "cursor-2"},
        "nodes": [
            {
                "id": "i1",
                "history": {
                    "nodes": [{"id": "h1", "issue": {"id": "i1"}}],
                    "pageInfo": {"hasNextPage": True, "endCursor": "hc-1"},
                },
            }
        ],
    }
    source._fetch_history_batch = lambda **kwargs: ({"data": {"issues": page}}, 1)
    source._paginate_issue_history = lambda issue_id, after: ([], 1)

    logged = []
    monkeypatch.setattr(linear.logger, "info", lambda msg: logged.append(msg))
    result = source.get_issue_history()

    summary = next(m for m in logged if "Batch fetched" in m)
    assert "skipped_overflow=1" in summary
    assert "skipped_head=1" in summary
    assert [r.id for r in result.records] == ["h1"]
    assert result.next_pagination == page["pageInfo"]


def test_get_issue_history_ends_pagination_with_an_empty_dict_not_none():
    source = object.__new__(linear.LinearSource)
    source.session = Mock()
    source._start_date = None
    page = {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": []}
    source._fetch_history_batch = lambda **kwargs: ({"data": {"issues": page}}, 0)

    assert source.get_issue_history().next_pagination == {}


# --------------------------------------------------------------------------
# Default HTTP policy
# --------------------------------------------------------------------------


def test_default_session_hands_non_2xx_back_to_the_classifier():
    """Rate limits arrive as HTTP 400 with a RATELIMITED body, so a raise_for_status hook would
    turn every throttle into a hard HTTPError before the classifier could read it."""
    source = object.__new__(linear.LinearSource)
    source.config = linear.LinearSourceConfig(name="linear", stream="issues", api_key="dummy")
    session = source.session = source.get_session()
    assert not session.hooks["response"]
    retries = session.adapters["https://"].max_retries
    assert "POST" in retries.allowed_methods
    assert set(retries.status_forcelist) >= {429, 500, 502, 503, 504}
