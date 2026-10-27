from unittest.mock import Mock

import pytest
from requests.exceptions import HTTPError

from bizon.source import graphql as gql

REAL_QUERY = "REAL_QUERY"
PROBE_QUERY = "PROBE_QUERY"
URL = "https://example.test/graphql"


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


def _non_json_response(status_code=200, text="<html>not json</html>"):
    response = Mock(status_code=status_code, ok=200 <= status_code < 400, headers={}, text=text)
    response.json.side_effect = ValueError("Expecting value: line 1 column 1 (char 0)")
    response.raise_for_status.side_effect = HTTPError(f"HTTP {status_code}", response=response)
    return response


def _session(*responses):
    session = Mock()
    session.post.side_effect = list(responses)
    return session


@pytest.fixture
def slept(monkeypatch):
    """Capture every sleep the transport asks for instead of serving it."""
    calls = []
    monkeypatch.setattr(gql.time, "sleep", calls.append)
    return calls


@pytest.fixture
def warned(monkeypatch):
    """Capture every logger.warning message the transport emits."""
    calls = []
    monkeypatch.setattr(gql.logger, "warning", calls.append)
    return calls


# --------------------------------------------------------------------------
# default_classifier
# --------------------------------------------------------------------------


def test_default_classifier_success_returns_none():
    assert gql.default_classifier(_response(body={"data": {"x": 1}}), {"data": {"x": 1}}) is None


@pytest.mark.parametrize(
    "status, body",
    [
        (200, {"errors": [{"message": "Internal server error"}]}),
        (200, {"errors": [{"extensions": {"type": "SERVICE_UNAVAILABLE"}}]}),
        (503, {}),
    ],
)
def test_default_classifier_transient(status, body):
    assert gql.default_classifier(_response(status, body), body) is gql.ErrorClass.TRANSIENT


@pytest.mark.parametrize(
    "status, body",
    [
        (429, {}),
        (400, {"errors": [{"extensions": {"code": "RATELIMITED"}}]}),
        (200, {"errors": [{"extensions": {"type": "RATE_LIMITED"}}]}),
    ],
)
def test_default_classifier_rate_limited(status, body):
    assert gql.default_classifier(_response(status, body), body) is gql.ErrorClass.RATE_LIMITED


def test_default_classifier_fatal_for_a_genuine_query_bug():
    body = {"errors": [{"message": "Field 'nope' doesn't exist on type 'Query'"}]}
    assert gql.default_classifier(_response(200, body), body) is gql.ErrorClass.FATAL


def test_errors_match_only_inspects_requested_type_fields():
    errors = [{"extensions": {"type": "INTERNAL"}}]
    assert not gql.errors_match(errors, types=("INTERNAL",), type_fields=("type",))
    assert gql.errors_match(errors, types=("INTERNAL",), type_fields=("extensions.type",))


# --------------------------------------------------------------------------
# graphql_post
# --------------------------------------------------------------------------


TRANSIENT_BODY = {"errors": [{"message": "Internal server error"}]}
OK_BODY = {"data": {"ok": True}}


def test_graphql_post_returns_body_on_success(slept):
    session = _session(_response(body=OK_BODY))
    assert gql.graphql_post(session, URL, REAL_QUERY, {"first": 5}) == OK_BODY
    assert slept == []
    _, kwargs = session.post.call_args
    assert kwargs["timeout"] == gql.GRAPHQL_TIMEOUT
    assert kwargs["json"] == {"query": REAL_QUERY, "variables": {"first": 5}}


def test_graphql_post_retries_transient_then_succeeds(slept):
    session = _session(
        _response(body=TRANSIENT_BODY),
        _response(body=TRANSIENT_BODY),
        _response(body=OK_BODY),
    )
    assert gql.graphql_post(session, URL, REAL_QUERY) == OK_BODY
    assert slept == [15, 30]


def test_graphql_post_raises_transient_after_budget(slept):
    session = _session(*[_response(body=TRANSIENT_BODY) for _ in range(3)])
    with pytest.raises(gql.TransientGraphQLError) as exc:
        gql.graphql_post(session, URL, REAL_QUERY)
    assert "Internal server error" in str(exc.value)
    assert session.post.call_count == 3
    assert slept == [15, 30]


def test_graphql_post_raises_fatal_immediately(slept):
    body = {"errors": [{"message": "Field 'nope' doesn't exist"}]}
    session = _session(_response(body=body))
    with pytest.raises(gql.GraphQLError) as exc:
        gql.graphql_post(session, URL, REAL_QUERY)
    assert not isinstance(exc.value, gql.TransientGraphQLError)
    assert session.post.call_count == 1
    assert slept == []


def test_graphql_post_unclaimed_non_ok_response_raises_for_status(slept):
    """A 401 is not a class the classifier owns -- it must surface as HTTPError so
    the caller can refresh its token."""
    session = _session(_response(401))
    with pytest.raises(HTTPError):
        gql.graphql_post(session, URL, REAL_QUERY)


def test_graphql_post_rate_limit_uses_fallback_ladder_then_raises(slept, warned):
    session = _session(*[_response(429) for _ in range(6)])
    with pytest.raises(gql.GraphQLRateLimited):
        gql.graphql_post(session, URL, REAL_QUERY)
    assert session.post.call_count == 6
    assert slept == [60, 120, 240, 480, 900]
    assert "attempt 1/6" in warned[0]


# --------------------------------------------------------------------------
# graphql_post: unparseable HTTP-200 body (silent empty page)
# --------------------------------------------------------------------------


def test_graphql_post_ok_non_json_body_raises_transient_after_budget(slept):
    session = _session(*[_non_json_response() for _ in range(3)])
    with pytest.raises(gql.TransientGraphQLError) as exc:
        gql.graphql_post(session, URL, REAL_QUERY)
    assert "Non-JSON response body (HTTP 200)" in str(exc.value)
    assert "not json" in str(exc.value)
    assert session.post.call_count == 3
    assert slept == [15, 30]


def test_graphql_post_ok_non_json_body_then_valid_json_succeeds(slept):
    session = _session(_non_json_response(), _response(body=OK_BODY))
    assert gql.graphql_post(session, URL, REAL_QUERY) == OK_BODY
    assert slept == [15]


def test_graphql_post_non_ok_non_json_body_still_raises_http_error(slept):
    """A 401 is unclaimed by the classifier regardless of body shape -- it must
    still surface as HTTPError, not get swept into the transient path."""
    session = _session(_non_json_response(401))
    with pytest.raises(HTTPError):
        gql.graphql_post(session, URL, REAL_QUERY)
    assert slept == []


def test_graphql_post_rate_limit_honours_wait_hint(slept):
    session = _session(_response(429), _response(body=OK_BODY))
    assert gql.graphql_post(session, URL, REAL_QUERY, rate_limit_wait_hint=lambda r: 10.0) == OK_BODY
    # hint + a 1-2s pad so we don't race the exact reset instant.
    assert len(slept) == 1
    assert 11.0 <= slept[0] < 12.0


def test_graphql_post_rate_limit_hint_is_capped(slept):
    """A reset far in the future must not stall the stream on a single event."""
    session = _session(_response(429), _response(body=OK_BODY))
    gql.graphql_post(
        session,
        URL,
        REAL_QUERY,
        rate_limit_wait_hint=lambda r: 100_000.0,
        rate_limit_wait=(60, 900),
    )
    assert slept == [900]


def test_graphql_post_retry_exceptions_share_the_rate_limit_budget(slept):
    boom = ConnectionError("connection reset")
    session = Mock()
    session.post.side_effect = [boom, boom, _response(body=OK_BODY)]
    assert gql.graphql_post(session, URL, REAL_QUERY, retry_exceptions=(ConnectionError,)) == OK_BODY
    assert slept == [60, 120]


def test_graphql_post_retry_exceptions_reraise_after_budget(slept):
    session = Mock()
    session.post.side_effect = ConnectionError("connection reset")
    with pytest.raises(ConnectionError):
        gql.graphql_post(session, URL, REAL_QUERY, retry_exceptions=(ConnectionError,))
    assert session.post.call_count == 6


def test_auth_factory_is_called_inside_the_retry_ladder(slept):
    """A credential minted over the network must be minted per attempt, inside the
    retried region -- otherwise a blip fetching it escapes the ladder entirely."""
    minted = []

    def factory():
        minted.append(len(minted))
        if len(minted) == 1:
            raise ConnectionError("connection reset while minting")
        return f"auth-{len(minted)}"

    session = _session(_response(body=OK_BODY))
    assert (
        gql.graphql_post(
            session,
            URL,
            REAL_QUERY,
            auth_factory=factory,
            retry_exceptions=(ConnectionError,),
        )
        == OK_BODY
    )
    assert len(minted) == 2, "factory re-ran on the retry"
    assert session.post.call_count == 1, "the failed attempt never reached POST"
    assert session.post.call_args.kwargs["auth"] == "auth-2"
    assert slept == [60]


def test_auth_factory_takes_precedence_over_auth(slept):
    session = _session(_response(body=OK_BODY))
    gql.graphql_post(session, URL, REAL_QUERY, auth="static", auth_factory=lambda: "minted")
    assert session.post.call_args.kwargs["auth"] == "minted"


def test_auth_is_used_when_no_factory_is_given(slept):
    session = _session(_response(body=OK_BODY))
    gql.graphql_post(session, URL, REAL_QUERY, auth="static")
    assert session.post.call_args.kwargs["auth"] == "static"


def test_graphql_post_transient_retry_resets_the_rate_limit_budget(slept):
    session = _session(
        _response(429),
        _response(body=TRANSIENT_BODY),
        _response(429),
        _response(body=OK_BODY),
    )
    assert gql.graphql_post(session, URL, REAL_QUERY) == OK_BODY
    assert slept == [60, 15, 60]


# --------------------------------------------------------------------------
# build_page_shrink_sequence
# --------------------------------------------------------------------------


def test_shrink_sequence_default_steps():
    assert gql.build_page_shrink_sequence(25) == [25, 10, 5, 1]


def test_shrink_sequence_custom_steps():
    assert gql.build_page_shrink_sequence(100, steps=(100, 50, 10, 1)) == [100, 50, 10, 1]


def test_shrink_sequence_start_below_first_step_does_not_invent_one():
    assert gql.build_page_shrink_sequence(3) == [3, 1]


def test_shrink_sequence_start_below_floor_still_keeps_both_endpoints():
    # Degenerate input; pinned so the ladder stays non-empty rather than
    # collapsing to [] and skipping the fetch entirely.
    assert gql.build_page_shrink_sequence(1, floor=5) == [5, 1]


def test_shrink_sequence_respects_a_raised_floor():
    assert gql.build_page_shrink_sequence(25, floor=5) == [25, 10, 5]


# --------------------------------------------------------------------------
# graphql_post_adaptive
# --------------------------------------------------------------------------


def _probe_body(end_cursor, has_next_page=True):
    return {"pageInfo": {"endCursor": end_cursor, "hasNextPage": has_next_page}}


def _page_info(body):
    return body.get("pageInfo", {}) or {}


def _make_probe(counter, *, per_page=50, per_run=200):
    return gql.PhantomProbe(
        query=PROBE_QUERY,
        cursor_var="after",
        max_skips_per_page=per_page,
        max_skips_per_run=per_run,
        run_counter=counter,
        page_info=_page_info,
    )


def _adaptive(post, variables, *, start_size=25, floor=1, max_size=None, probe=None):
    return gql.graphql_post_adaptive(
        Mock(),
        URL,
        REAL_QUERY,
        variables,
        page_size_var="first",
        start_size=start_size,
        floor=floor,
        max_size=max_size,
        probe=probe,
        label="stream",
        post=post,
    )


def test_adaptive_returns_the_size_that_worked():
    sizes = []

    def post(query, variables):
        sizes.append(variables["first"])
        if variables["first"] > 5:
            raise gql.TransientGraphQLError("resource limits exceeded")
        return OK_BODY

    body, size = _adaptive(post, {"first": 25})
    assert body == OK_BODY
    assert size == 5
    assert sizes == [25, 10, 5]


def test_adaptive_without_probe_raises_once_the_ladder_is_exhausted():
    post = Mock(side_effect=gql.TransientGraphQLError("boom"))
    with pytest.raises(gql.TransientGraphQLError):
        _adaptive(post, {"first": 25})
    assert post.call_count == 4


def test_adaptive_probe_recovery_returns_max_size_not_start_size():
    """By the time pagination reaches an unresolvable run the ladder has already
    walked the page size to the floor, so resuming at start_size would leave the
    rest of the connection fetched one row at a time."""
    counter = gql.MutableCounter()
    probes = {"n": 0}
    reals = {"n": 0}

    def post(query, variables):
        if query == PROBE_QUERY:
            probes["n"] += 1
            return _probe_body(f"cursor-{probes['n']}")
        reals["n"] += 1
        if reals["n"] < 3:
            raise gql.TransientGraphQLError("something went wrong")
        return OK_BODY

    variables = {"first": 1}
    body, size = _adaptive(post, variables, start_size=1, max_size=25, probe=_make_probe(counter))
    assert body == OK_BODY
    assert size == 25
    assert counter.value == 2
    assert variables["after"] == "cursor-2"


def test_adaptive_probe_failure_propagates_the_original_error():
    counter = gql.MutableCounter()
    post = Mock(side_effect=gql.TransientGraphQLError("service unavailable"))
    with pytest.raises(gql.TransientGraphQLError):
        _adaptive(post, {"first": 25}, probe=_make_probe(counter))
    assert counter.value == 0


def test_adaptive_per_page_cap_raises():
    counter = gql.MutableCounter()
    probes = {"n": 0}

    def post(query, variables):
        if query == PROBE_QUERY:
            probes["n"] += 1
            return _probe_body(f"cursor-{probes['n']}")
        raise gql.TransientGraphQLError("something went wrong")

    with pytest.raises(gql.TransientGraphQLError):
        _adaptive(post, {"first": 25}, probe=_make_probe(counter, per_page=2))
    assert probes["n"] == 2
    assert counter.value == 2


def test_adaptive_per_run_cap_raises_a_plain_runtime_error():
    """The run-wide ceiling must not be a TransientGraphQLError -- a systemically
    broken run has to fail loudly, not look like one more retryable page."""
    counter = gql.MutableCounter(2)

    def post(query, variables):
        if query == PROBE_QUERY:
            return _probe_body("cursor-1")
        raise gql.TransientGraphQLError("something went wrong")

    with pytest.raises(RuntimeError) as exc:
        _adaptive(post, {"first": 25}, probe=_make_probe(counter, per_run=2))
    assert not isinstance(exc.value, gql.GraphQLError)
    assert "stream reset" in str(exc.value)
    assert counter.value == 3


def test_adaptive_probe_zero_per_page_skips_probing_entirely():
    counter = gql.MutableCounter()
    post = Mock(side_effect=gql.TransientGraphQLError("boom"))
    with pytest.raises(gql.TransientGraphQLError):
        _adaptive(post, {"first": 25}, probe=_make_probe(counter, per_page=0))
    assert post.call_count == 4
    assert counter.value == 0


@pytest.mark.parametrize(
    "probe_body",
    [
        _probe_body("cursor-1", has_next_page=False),
        _probe_body(None),
        {"pageInfo": {}},
    ],
)
def test_adaptive_no_progress_guard_raises(probe_body):
    """No usable next cursor means this can't be an entry we step over."""
    counter = gql.MutableCounter()

    def post(query, variables):
        if query == PROBE_QUERY:
            return probe_body
        raise gql.TransientGraphQLError("something went wrong")

    with pytest.raises(gql.TransientGraphQLError):
        _adaptive(post, {"first": 25}, probe=_make_probe(counter))
    assert counter.value == 0


def test_adaptive_echoed_cursor_raises_instead_of_spinning():
    counter = gql.MutableCounter()
    probes = {"n": 0}

    def post(query, variables):
        if query == PROBE_QUERY:
            probes["n"] += 1
            return _probe_body("cursor-1")
        raise gql.TransientGraphQLError("something went wrong")

    with pytest.raises(gql.TransientGraphQLError):
        _adaptive(post, {"first": 25}, probe=_make_probe(counter))
    assert probes["n"] == 2
    assert counter.value == 1


def test_adaptive_defaults_to_graphql_post_when_no_post_is_injected(slept):
    session = _session(_response(body=OK_BODY))
    body, size = gql.graphql_post_adaptive(
        session, URL, REAL_QUERY, {"first": 25}, page_size_var="first", start_size=25
    )
    assert (body, size) == (OK_BODY, 25)


def test_adaptive_rejects_post_and_graphql_post_kwargs_together():
    with pytest.raises(TypeError):
        gql.graphql_post_adaptive(
            Mock(),
            URL,
            REAL_QUERY,
            {"first": 25},
            page_size_var="first",
            start_size=25,
            post=lambda q, v: OK_BODY,
            log_prefix="should not be accepted",
        )
